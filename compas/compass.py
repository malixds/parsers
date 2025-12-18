import asyncio
import json
import logging
import os
import re
import uuid
from typing import Any
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO

import httpx
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from schema import DbDTO, AgentData

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CompassParser:
    """
    Парсер для compass.com через API (асинхронный)
    1. Получение списка объявлений через API (POST запросы)
    2. Получение HTML для каждого листинга через httpx
    3. Парсинг данных из API и HTML для извлечения обязательных полей
    """
    
    def __init__(
        self,
        save_html_every: int = 20,
        html_save_dir: str = "htmls",
        concurrency: int = 10,
    ) -> None:
        self.source_name = "compass"
        self.base_url = "https://www.compass.com"
        
        # Настройки сохранения HTML
        self.save_html_every = save_html_every
        self.html_save_dir = html_save_dir
        self.html_counter = 0
        
        # Настройки concurrency
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        
        # Создаем папку для сохранения HTML, если её нет
        if not os.path.exists(self.html_save_dir):
            os.makedirs(self.html_save_dir)
            logger.info(f"Создана папка для сохранения HTML: {self.html_save_dir}")

    # ---------------------- ЭТАП 1: ИНДЕКСАЦИЯ (ЧЕРЕЗ САЙТМАПЫ) ----------------------

    async def get_sitemap_urls(self, client: httpx.AsyncClient) -> list[str]:
        """
        Получает все URL сайтмапов из robots.txt
        """
        robots_url = f"{self.base_url}/robots.txt"
        logger.info(f"[1-SITEMAP] Получаю robots.txt: {robots_url}")
        
        try:
            headers = {'User-Agent': UserAgent().random}
            response = await client.get(robots_url, headers=headers, timeout=30.0)
            response.raise_for_status()
            
            robots_content = response.text
            sitemap_urls = []
            
            # Парсим robots.txt и ищем все строки с Sitemap:
            for line in robots_content.split('\n'):
                line = line.strip()
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    sitemap_urls.append(sitemap_url)
            
            logger.info(f"[1-SITEMAP] Найдено {len(sitemap_urls)} сайтмапов в robots.txt")
            return sitemap_urls
            
        except Exception as e:
            logger.error(f"[1-SITEMAP] Ошибка при получении robots.txt: {e}")
            return []

    async def parse_sitemap(self, client: httpx.AsyncClient, sitemap_url: str) -> list[str]:
        """
        Парсит один sitemap и возвращает список URL с /homedetails
        Поддерживает как обычные XML сайтмапы, так и gzip сайтмапы
        """
        logger.info(f"[1-SITEMAP] Обрабатываю sitemap: {sitemap_url}")
        
        try:
            headers = {'User-Agent': UserAgent().random}
            async with self.semaphore:
                response = await client.get(sitemap_url, headers=headers, timeout=60.0)
                response.raise_for_status()
            
            content = response.content
            
            # Проверяем, gzip ли это
            if sitemap_url.endswith('.gz'):
                try:
                    content = gzip.decompress(content)
                except Exception as e:
                    logger.warning(f"[1-SITEMAP] Не удалось разархивировать gzip: {e}")
                    return []
            
            # Парсим XML
            try:
                root = ET.fromstring(content)
            except ET.ParseError as e:
                logger.error(f"[1-SITEMAP] Ошибка парсинга XML: {e}")
                return []
            
            # Извлекаем URL
            # Namespace для sitemap
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            urls = []
            
            # Проверяем, это sitemap index или обычный sitemap
            sitemap_elements = root.findall('.//sm:sitemap', ns)
            if sitemap_elements:
                # Это sitemap index - рекурсивно обрабатываем вложенные сайтмапы
                logger.info(f"[1-SITEMAP] Найден sitemap index с {len(sitemap_elements)} вложенными сайтмапами")
                for sitemap_elem in sitemap_elements:
                    loc = sitemap_elem.find('sm:loc', ns)
                    if loc is not None and loc.text:
                        nested_urls = await self.parse_sitemap(client, loc.text)
                        urls.extend(nested_urls)
            else:
                # Это обычный sitemap с URL
                url_elements = root.findall('.//sm:url', ns)
                
                for url_elem in url_elements:
                    loc = url_elem.find('sm:loc', ns)
                    if loc is not None and loc.text:
                        url = loc.text
                        # Фильтруем только URL с /homedetails
                        if '/homedetails' in url:
                            urls.append(url)
                
                logger.info(f"[1-SITEMAP] Найдено {len(urls)} URL с /homedetails в sitemap")
            
            return urls
            
        except Exception as e:
            logger.error(f"[1-SITEMAP] Ошибка при обработке sitemap {sitemap_url}: {e}")
            return []

    async def get_all_listing_urls_from_sitemaps(self, client: httpx.AsyncClient, max_urls: int = None) -> list[str]:
        """
        Получает URL листингов (/homedetails) из сайтмапов
        Останавливается, как только набрано нужное количество
        """
        logger.info("[1-SITEMAP] Начинаю сбор URL из сайтмапов...")
        
        # Получаем список сайтмапов
        sitemap_urls = await self.get_sitemap_urls(client)
        
        if not sitemap_urls:
            logger.error("[1-SITEMAP] Не найдено сайтмапов в robots.txt")
            return []
        
        logger.info(f"[1-SITEMAP] Найдено {len(sitemap_urls)} сайтмапов в robots.txt")
        
        if max_urls:
            logger.info(f"[1-SITEMAP] Собираю первые {max_urls} URL...")
        
        all_urls = []
        seen_urls = set()
        
        # Обрабатываем сайтмапы последовательно, останавливаемся при достижении лимита
        for idx, sitemap_url in enumerate(sitemap_urls):
            if max_urls and len(all_urls) >= max_urls:
                logger.info(f"[1-SITEMAP] Достигнут лимит {max_urls} URL, останавливаю сбор")
                break
            
            try:
                urls = await self.parse_sitemap(client, sitemap_url)
                
                # Добавляем только уникальные URL
                new_urls = []
                for url in urls:
                    if url not in seen_urls:
                        seen_urls.add(url)
                        new_urls.append(url)
                        all_urls.append(url)
                        
                        # Останавливаемся, если достигли лимита
                        if max_urls and len(all_urls) >= max_urls:
                            break
                
                if new_urls:
                    logger.info(f"[1-SITEMAP] Sitemap {idx + 1}/{len(sitemap_urls)}: добавлено {len(new_urls)} новых URL (всего: {len(all_urls)})")
                    
            except Exception as e:
                logger.error(f"[1-SITEMAP] Ошибка при обработке sitemap {idx + 1}: {e}")
        
        logger.info(f"[1-SITEMAP] Всего собрано {len(all_urls)} уникальных URL с /homedetails")
        
        # Ограничиваем количество, если нужно
        if max_urls and len(all_urls) > max_urls:
            all_urls = all_urls[:max_urls]
            logger.info(f"[1-SITEMAP] Ограничено до {max_urls} URL")
        
        return all_urls

    # ---------------------- ЭТАП 1: ИНДЕКСАЦИЯ (СТАРЫЙ МЕТОД ЧЕРЕЗ API) ----------------------

    def _split_area_into_grid(self, ne_point: dict, sw_point: dict, grid_size: int = 3) -> list[tuple]:
        """
        Разбивает область на более мелкие части (grid) для получения всех объявлений
        Возвращает список кортежей (ne_point, sw_point) для каждой ячейки grid
        """
        ne_lat, ne_lng = ne_point['latitude'], ne_point['longitude']
        sw_lat, sw_lng = sw_point['latitude'], sw_point['longitude']
        
        lat_step = (ne_lat - sw_lat) / grid_size
        lng_step = (ne_lng - sw_lng) / grid_size
        
        grid_cells = []
        for i in range(grid_size):
            for j in range(grid_size):
                cell_ne_lat = ne_lat - (i * lat_step)
                cell_ne_lng = ne_lng - (j * lng_step)
                cell_sw_lat = ne_lat - ((i + 1) * lat_step)
                cell_sw_lng = ne_lng - ((j + 1) * lng_step)
                
                cell_ne = {"latitude": cell_ne_lat, "longitude": cell_ne_lng}
                cell_sw = {"latitude": cell_sw_lat, "longitude": cell_sw_lng}
                
                grid_cells.append((cell_ne, cell_sw))
        
        return grid_cells

    async def get_listings_from_api(self, client: httpx.AsyncClient, location: str = "new-york", max_results: int = 1000, use_grid: bool = True) -> list[dict]:
        """
        ЭТАП 1 (API): Получает данные объявлений через API compass.com (асинхронно)
        Возвращает список словарей с данными объявлений (включая listing объекты)
        
        Args:
            use_grid: Если True, разбивает область на части для получения всех объявлений
        """
        logger.info(f"[1-API] Получаю данные объявлений через API для локации: {location}")
        
        all_listings_data = []
        search_result_id = str(uuid.uuid4())
        
        # Базовые координаты для New York (можно расширить для других локаций)
        # Эти координаты покрывают весь штат NY
        ne_point = {"latitude": 45.3525295, "longitude": -72.3285732}
        sw_point = {"latitude": 39.9017281, "longitude": -79.2115078}
        viewport_ne = {"lat": 45.2954092, "lng": -72.3285732}
        viewport_sw = {"lat": 39.839376, "lng": -79.2115078}
        
        if use_grid:
            # Разбиваем область на части для получения всех объявлений
            logger.info(f"[1-API] Используется grid-подход: разбиваем область на части")
            # Увеличиваем grid_size для получения большего количества объявлений
            grid_size = 6  # 6x6 = 36 частей (больше покрытие)
            grid_cells = self._split_area_into_grid(ne_point, sw_point, grid_size=grid_size)
            logger.info(f"[1-API] Область разбита на {len(grid_cells)} частей ({grid_size}x{grid_size})")
            
            # Обрабатываем каждую часть параллельно
            tasks = []
            for idx, (cell_ne, cell_sw) in enumerate(grid_cells):
                cell_viewport_ne = {"lat": cell_ne["latitude"], "lng": cell_ne["longitude"]}
                cell_viewport_sw = {"lat": cell_sw["latitude"], "lng": cell_sw["longitude"]}
                task = self._get_listings_for_area(
                    client, location, str(uuid.uuid4()),  # Уникальный search_result_id для каждой части
                    cell_ne, cell_sw, cell_viewport_ne, cell_viewport_sw,
                    max_results // len(grid_cells) + 50  # Немного больше на часть
                )
                tasks.append(task)
            
            # Выполняем все задачи параллельно
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"[1-API] Ошибка при обработке части {idx + 1}: {result}")
                else:
                    all_listings_data.extend(result)
                    logger.info(f"[1-API] Часть {idx + 1}/{len(grid_cells)}: получено {len(result)} объявлений")
            
            # Удаляем дубликаты по listingIdSHA
            seen_ids = set()
            unique_listings = []
            for listing in all_listings_data:
                listing_id = listing.get('listingIdSHA')
                if listing_id and listing_id not in seen_ids:
                    seen_ids.add(listing_id)
                    unique_listings.append(listing)
            
            logger.info(f"[1-API] После удаления дубликатов: {len(unique_listings)} уникальных объявлений из {len(all_listings_data)}")
            return unique_listings[:max_results]
        else:
            # Старый подход - одна область
            return await self._get_listings_for_area(
                client, location, search_result_id,
                ne_point, sw_point, viewport_ne, viewport_sw,
                max_results
            )

    async def _get_listings_for_area(
        self, 
        client: httpx.AsyncClient,
        location: str,
        search_result_id: str,
        ne_point: dict,
        sw_point: dict,
        viewport_ne: dict,
        viewport_sw: dict,
        max_results: int
    ) -> list[dict]:
        """Получает объявления для конкретной области"""
        listings_data = []
        location_ids = None
        page = 0
        num_per_page = 50
        
        try:
            while len(listings_data) < max_results:
                headers = {
                    'User-Agent': UserAgent().random,
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Referer': f'https://www.compass.com/homes-for-sale/{location}/',
                    'Content-Type': 'application/json',
                    'Origin': 'https://www.compass.com',
                    'Sec-GPC': '1',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'Priority': 'u=6',
                }
                
                # Пагинация работает через параметр start в searchQuery URL!
                search_query = {
                    "sort": {"column": "dom", "direction": "asc"},
                    "start": page * num_per_page  # Добавляем start для пагинации
                }
                params = {
                    'searchQuery': json.dumps(search_query),
                }
                
                # Формируем rawLolSearchQuery
                raw_query = {
                    'listingTypes': [2],  # 2 = For Sale
                    'saleStatuses': [12, 9],  # Active listings
                    'num': num_per_page,
                    'start': page * num_per_page,
                    'sortOrder': 46,  # DOM ascending
                    'facetFieldNames': [
                        'contributingDatasetList',
                        'compassListingTypes',
                        'comingSoon',
                    ],
                }
                
                # Добавляем locationIds если есть (из первого ответа)
                if location_ids:
                    raw_query['locationIds'] = location_ids
                else:
                    # Используем координаты для первого запроса
                    raw_query['nePoint'] = ne_point
                    raw_query['swPoint'] = sw_point
                
                json_data = {
                    'searchResultId': search_result_id,
                    'rawLolSearchQuery': raw_query,
                    'viewport': {
                        'northeast': viewport_ne,
                        'southwest': viewport_sw,
                    },
                    'viewportFrom': 'response',
                    'height': 1350,
                    'width': 1253,
                    'isMapFullyInitialized': True,
                    'purpose': 'search',
                }
                
                api_url = f"{self.base_url}/homes-for-sale/{location}/mapview={viewport_ne['lat']},{viewport_ne['lng']},{viewport_sw['lat']},{viewport_sw['lng']}/"
                
                logger.info(f"[1-API] Запрос страницы {page + 1}, start={page * num_per_page}")
                
                try:
                    async with self.semaphore:
                        response = await client.post(
                            api_url,
                            params=params,
                            json=json_data,
                            headers=headers,
                            timeout=30.0
                        )
                        response.raise_for_status()
                    
                    data = response.json()
                    
                    if 'lolResults' not in data or 'data' not in data['lolResults']:
                        logger.warning(f"[1-API] Неожиданная структура ответа API")
                        break
                    
                    listings = data['lolResults']['data']
                    total_items = data['lolResults'].get('totalItems', 0)
                    
                    # Извлекаем locationIds из ответа для последующих запросов
                    if not location_ids and 'rawLolSearchQuery' in data:
                        response_query = data.get('rawLolSearchQuery', {})
                        if 'locationIds' in response_query:
                            location_ids = response_query['locationIds']
                            logger.info(f"[1-API] Найдены locationIds: {location_ids}, используем для последующих запросов")
                    
                    logger.info(f"[1-API] Получено {len(listings)} объявлений (всего доступно: {total_items})")
                    
                    if not listings:
                        logger.info(f"[1-API] Больше нет объявлений. Завершаем.")
                        break
                    
                    # Сохраняем данные объявлений (включая listing объекты)
                    for item in listings:
                        listing = item.get('listing', {})
                        if listing:  # Только если есть данные listing
                            listings_data.append(listing)
                    
                    logger.info(f"[1-API] На странице {page + 1} добавлено {len(listings)} объявлений. Всего: {len(listings_data)}")
                    
                    # Проверяем, достигли ли мы лимита
                    if len(listings_data) >= max_results:
                        logger.info(f"[1-API] Достигнут лимит max_results ({max_results}). Завершаем.")
                        break
                    
                    # Проверяем, получили ли мы все доступные объявления
                    if total_items > 0 and len(listings_data) >= total_items:
                        logger.info(f"[1-API] Получены все доступные объявления ({total_items}). Завершаем.")
                        break
                    
                    # Пагинация теперь работает правильно через start в searchQuery URL!
                    # Не нужно проверять дубликаты между страницами, так как результаты разные
                    
                    # Если получили меньше, чем запрашивали, проверяем есть ли еще данные
                    if len(listings) < num_per_page:
                        # Если получили меньше результатов и еще есть объявления - продолжаем
                        if total_items > 0 and len(listings_data) < total_items:
                            remaining = total_items - len(listings_data)
                            logger.info(f"[1-API] Получено меньше результатов ({len(listings)}), но еще есть {remaining} объявлений. Продолжаем.")
                        else:
                            # Если total_items неизвестен или мы получили все - завершаем
                            logger.info(f"[1-API] Получено меньше результатов и больше нет объявлений. Завершаем.")
                            break
                    # Если получили пустой список - завершаем
                    elif len(listings) == 0:
                        logger.info(f"[1-API] Получен пустой список объявлений. Завершаем.")
                        break
                    
                    page += 1
                    
                    # Небольшая задержка между запросами
                    await asyncio.sleep(0.5)
                    
                except httpx.HTTPStatusError as e:
                    logger.error(f"[1-API] HTTP ошибка при запросе к API: {e}")
                    break
                except httpx.RequestError as e:
                    logger.error(f"[1-API] Ошибка при запросе к API: {e}")
                    break
                except json.JSONDecodeError as e:
                    logger.error(f"[1-API] Ошибка при парсинге JSON ответа: {e}")
                    break
            
            logger.info(f"[1-API] Итого получено {len(listings_data)} объявлений")
            return listings_data[:max_results]
            
        except Exception as e:
            logger.error(f"[1-API] Ошибка при получении данных объявлений через API: {e}")
            import traceback
            traceback.print_exc()
            return listings_data


    # ---------------------- ЭТАП 3: ПАРСИНГ ДАННЫХ ----------------------

    async def get_listing_html(self, client: httpx.AsyncClient, url: str) -> str | None:
        """Получает HTML страницы объявления через httpx (асинхронно)"""
        try:
            headers = {
                'User-Agent': UserAgent().random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.compass.com/',
            }
            async with self.semaphore:
                response = await client.get(url, headers=headers, timeout=15.0, follow_redirects=True)
                response.raise_for_status()
                return response.text
        except Exception as e:
            logger.warning(f"Не удалось загрузить HTML для {url}: {e}")
            return None

    async def parse_listing_from_api_data(self, client: httpx.AsyncClient, listing_data: dict) -> DbDTO | None:
        """
        ЭТАП 3 (API): Парсит данные объявления напрямую из API ответа (асинхронно)
        Для недостающих полей (описание, MLS, brochure, агенты) загружает HTML страницы
        """
        try:
            # Извлекаем URL и ID
            page_link = listing_data.get('pageLink') or listing_data.get('navigationPageLink', '')
            if not page_link:
                logger.warning("Не найден pageLink в данных объявления")
                return None
            
            if not page_link.startswith('http'):
                url = urljoin(self.base_url, page_link)
            else:
                url = page_link
            
            # Извлекаем ID из URL
            listing_id = "unknown"
            parsed = urlparse(url)
            path_parts = [p for p in parsed.path.split('/') if p]
            if path_parts:
                listing_id = path_parts[-1]
            
            # Сначала маппим данные из API
            dto = self._map_to_dto_from_api(listing_data, url, listing_id)
            
            # Затем загружаем HTML для недостающих полей
            html = await self.get_listing_html(client, url)
            if html:
                # Дополняем данными из HTML
                self._enrich_from_html(dto, html, listing_id)
            
            return dto
        except Exception as e:
            logger.error(f"Ошибка при парсинге данных объявления: {e}")
            import traceback
            traceback.print_exc()
            return None


    def _map_to_dto_from_api(self, listing_data: dict, url: str, listing_id: str) -> DbDTO:
        """Преобразует данные из API ответа в DbDTO"""
        
        # Helper для безопасного получения вложенных полей
        def get_val(obj, path, default=None):
            for key in path.split('.'):
                if isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    return default
            return obj
        
        # Address - из structuredData.singleFamilyResidence (JSON строка)
        address = "Address not found"
        structured_data = listing_data.get('structuredData', {})
        if 'singleFamilyResidence' in structured_data:
            try:
                sfr_str = structured_data['singleFamilyResidence']
                if isinstance(sfr_str, str):
                    sfr = json.loads(sfr_str)
                    if 'address' in sfr:
                        addr = sfr['address']
                        street = addr.get('streetAddress', '')
                        city = addr.get('addressLocality', '')
                        state = addr.get('addressRegion', '')
                        zip_code = addr.get('postalCode', '')
                        parts = [p for p in [street, city, state, zip_code] if p]
                        address = ', '.join(parts) if parts else "Address not found"
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Если адрес не найден, пробуем из других мест
        if address == "Address not found":
            location = listing_data.get('location', {})
            if isinstance(location, dict) and 'prettyAddress' in location:
                address = location['prettyAddress']
        
        # Price - из title (например "$2,300,000")
        title = listing_data.get('title', '')
        sale_price = None
        lease_price = None
        listing_type = 'sale'  # По умолчанию
        
        if title and title.startswith('$'):
            # Извлекаем число из строки типа "$2,300,000"
            price_match = re.search(r'\$([\d,]+)', title.replace(',', ''))
            if price_match:
                price_str = title  # Сохраняем как есть
                # Определяем тип по listingType или другим признакам
                listing_type_code = listing_data.get('status')  # status может указывать на тип
                # По умолчанию для homes-for-sale это продажа
                sale_price = price_str
                listing_type = 'sale'
        
        # Size - из subStats (площадь)
        size = None
        sub_stats = listing_data.get('subStats', [])
        for stat in sub_stats:
            title_key = stat.get('title', '')
            subtitle = stat.get('subtitle', '').replace(',', '').replace('-', '').strip()
            
            if title_key == 'sqft' and subtitle and subtitle != 'Unavailable':
                try:
                    sqft_num = int(subtitle)
                    size = f"{sqft_num:,} SF"
                except ValueError:
                    size = subtitle + " SF"
                break
            elif title_key == 'acres' and subtitle and subtitle != 'Unavailable' and not size:
                # Если нет sqft, используем acres
                size = f"{subtitle} acres"
        
        # Description - из structuredData.product (короткое описание)
        # Полное описание будет загружено из HTML
        description = None
        structured_data = listing_data.get('structuredData', {})
        if 'product' in structured_data:
            try:
                product_str = structured_data['product']
                if isinstance(product_str, str):
                    product = json.loads(product_str)
                    description = product.get('description', '')
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Photos - из media
        photos = []
        media = listing_data.get('media', [])
        for item in media:
            # В API media содержит originalUrl и thumbnailUrl
            if 'originalUrl' in item:
                photos.append(item['originalUrl'])
            elif 'thumbnailUrl' in item:
                photos.append(item['thumbnailUrl'])
            elif 'url' in item:
                photos.append(item['url'])
        
        # Status - из badges или status
        status = "Available"
        badges = listing_data.get('badges', {})
        corner_badges = badges.get('cornerBadges', [])
        if corner_badges:
            # Берем первый badge (обычно это статус типа "Coming Soon")
            status = corner_badges[0].get('displayText', 'Available')
        else:
            # Если нет badges, используем status код
            status_code = listing_data.get('status')
            if status_code:
                status_map = {
                    12: "Active",
                    9: "Active",
                    14: "Active",
                    # Добавить другие коды по мере необходимости
                }
                status = status_map.get(status_code, f"Status {status_code}")
        
        # Details - из subStats и других полей (extract_details)
        details = {}
        
        # Bedrooms, Bathrooms из subStats
        for stat in sub_stats:
            title_key = stat.get('title', '')
            subtitle = stat.get('subtitle', '').replace('-', '').strip()
            
            if title_key == 'beds' and subtitle and subtitle != 'Unavailable':
                try:
                    details['bedrooms'] = int(subtitle)
                except ValueError:
                    details['bedrooms'] = subtitle
            elif title_key == 'baths' and subtitle and subtitle != 'Unavailable':
                try:
                    details['bathrooms'] = float(subtitle) if '.' in subtitle else int(subtitle)
                except ValueError:
                    details['bathrooms'] = subtitle
            elif title_key == 'acres' and subtitle and subtitle != 'Unavailable':
                details['acres'] = subtitle
            elif title_key == 'sqft' and subtitle and subtitle != 'Unavailable':
                try:
                    details['square_feet'] = int(subtitle.replace(',', ''))
                except ValueError:
                    details['square_feet'] = subtitle
        
        # Property type из clusterSummary
        cluster_summary = listing_data.get('clusterSummary', {})
        if 'propertyType' in cluster_summary:
            prop_type = cluster_summary['propertyType']
            if isinstance(prop_type, dict) and 'masterType' in prop_type:
                master_type = prop_type['masterType']
                if isinstance(master_type, dict) and 'GLOBAL' in master_type:
                    types_list = master_type['GLOBAL']
                    if types_list:
                        details['property_type'] = types_list[0]
        
        # Price range из clusterSummary
        if 'priceRange' in cluster_summary:
            price_range = cluster_summary['priceRange']
            if isinstance(price_range, list) and price_range:
                details['price_range'] = price_range
        
        # Agents - будет заполнено из HTML
        agents = []
        
        # MLS number - будет заполнено из HTML (extract_mls)
        mls_number = None
        
        return DbDTO(
            source_name=self.source_name,
            listing_id=listing_id,
            listing_link=url,
            listing_type=listing_type,
            listing_status=status,
            address=address,
            sale_price=sale_price,
            lease_price=lease_price,
            size=size,
            property_description=description,
            listing_details=details if details else None,
            photos=photos if photos else None,
            brochure_pdf=None,
            mls_number=mls_number,
            agents=agents if agents else None,
            agency_phone=None,
        )

    def _enrich_from_html(self, dto: DbDTO, html: str, listing_id: str) -> None:
        """Дополняет DTO данными из HTML страницы (описание, MLS, brochure, агенты)"""
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # 1. Extract MLS number
            mls = self.extract_mls(soup)
            if mls:
                dto.mls_number = mls
            
            # 2. Extract description
            description = self.extract_description(soup)
            if description and not dto.property_description:
                dto.property_description = description
            
            # 3. Extract brochure PDF
            brochure = self.extract_brochure_pdf(soup)
            if brochure:
                dto.brochure_pdf = brochure
            
            # 4. Extract agents
            agents = self.extract_agents(soup, self.base_url)
            if agents:
                dto.agents = agents
            
            # 5. Extract additional details
            additional_details = self.extract_details(soup)
            if additional_details:
                if dto.listing_details:
                    dto.listing_details.update(additional_details)
                else:
                    dto.listing_details = additional_details
            
            # 6. Extract size если не было найдено
            if not dto.size:
                size = self.extract_size(soup)
                if size:
                    dto.size = size
            
        except Exception as e:
            logger.warning(f"Ошибка при обогащении данных из HTML: {e}")

    @staticmethod
    def extract_mls(soup: BeautifulSoup) -> str | None:
        """Извлекает MLS номер из HTML"""
        mls_patterns = [
            re.compile(r'MLS[#:\s]*([A-Z0-9\-]+)', re.I),
            re.compile(r'MLS\s*Number[#:\s]*([A-Z0-9\-]+)', re.I),
            re.compile(r'Multiple\s*Listing\s*Service[#:\s]*([A-Z0-9\-]+)', re.I),
        ]
        
        page_text = soup.get_text()
        for pattern in mls_patterns:
            match = pattern.search(page_text)
            if match:
                return match.group(1).strip()
        
        # Ищем в структурированных данных
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    # Ищем MLS в разных местах
                    if 'identifier' in data:
                        identifier = data['identifier']
                        if isinstance(identifier, dict) and identifier.get('@type') == 'PropertyValue':
                            value = identifier.get('value', '')
                            if 'MLS' in value.upper():
                                return value
            except (json.JSONDecodeError, AttributeError):
                continue
        
        return None

    @staticmethod
    def extract_description(soup: BeautifulSoup) -> str | None:
        """Извлекает описание объявления из HTML"""
        # Ищем в разных местах
        selectors = [
            'div[class*="description"]',
            'div[class*="property-description"]',
            'div[class*="listing-description"]',
            'section[class*="description"]',
            '[data-testid*="description"]',
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text(strip=True)
                if text and len(text) > 50:  # Минимум 50 символов
                    return text
        
        # Ищем в структурированных данных
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if 'description' in data:
                        desc = data['description']
                        if isinstance(desc, str) and len(desc) > 50:
                            return desc
            except (json.JSONDecodeError, AttributeError):
                continue
        
        return None

    @staticmethod
    def extract_brochure_pdf(soup: BeautifulSoup) -> str | None:
        """Извлекает ссылку на brochure PDF"""
        # Ищем ссылки на PDF
        pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
        for link in pdf_links:
            href = link.get('href', '')
            text = link.get_text(strip=True).lower()
            if 'brochure' in text or 'flyer' in text or 'marketing' in text:
                if not href.startswith('http'):
                    href = urljoin('https://www.compass.com', href)
                return href
        
        # Ищем в data-атрибутах
        elements = soup.find_all(attrs={'data-brochure': True})
        for elem in elements:
            brochure_url = elem.get('data-brochure')
            if brochure_url:
                if not brochure_url.startswith('http'):
                    brochure_url = urljoin('https://www.compass.com', brochure_url)
                return brochure_url
        
        return None

    @staticmethod
    def extract_agents(soup: BeautifulSoup, base_url: str) -> list[AgentData]:
        """Извлекает агентов из HTML"""
        agents = []
        
        # Ищем блоки с агентами
        agent_selectors = [
            '[class*="agent"]',
            '[class*="listing-agent"]',
            '[data-testid*="agent"]',
            '[class*="broker"]',
        ]
        
        for selector in agent_selectors:
            elements = soup.select(selector)
            for elem in elements:
                # Получаем весь текст блока для парсинга
                full_text = elem.get_text(separator=' ', strip=True)
                
                # Ищем имя - сначала пытаемся найти отдельный элемент
                name = None
                name_elem = elem.find(['h3', 'h4', 'h5', 'div', 'span'], class_=re.compile(r'name|agent', re.I))
                if not name_elem:
                    name_elem = elem.find('a', href=re.compile(r'/agent/|/team/'))
                
                if name_elem:
                    name = name_elem.get_text(strip=True)
                else:
                    # Если не нашли отдельный элемент, парсим из текста
                    # Формат: "Listed byLynn Wadleigh • Coldwell Banker..."
                    name_match = re.search(r'(?:Listed\s+by|Agent:?)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', full_text)
                    if name_match:
                        name = name_match.group(1).strip()
                
                # Если имя не найдено, пропускаем
                if not name or len(name) < 2:
                    continue
                
                # Ищем ссылку на профиль агента
                link = None
                a_tag = elem.find('a', href=re.compile(r'/agent/|/team/'))
                if a_tag:
                    link = a_tag.get('href', '')
                    if link and not link.startswith('http'):
                        link = urljoin(base_url, link)
                
                # Парсим телефон из текста
                # Форматы: P:(518)-260-2222, C:(518)-260-2222, Phone: (518) 260-2222
                phone_primary = None
                phone_alt = None
                
                # Ищем через tel: ссылки
                phone_elems = elem.find_all('a', href=re.compile(r'tel:'))
                for phone_elem in phone_elems:
                    phone_match = re.search(r'tel:([\d\s\-\(\)]+)', phone_elem.get('href', ''))
                    if phone_match:
                        if not phone_primary:
                            phone_primary = phone_match.group(1).strip()
                        else:
                            phone_alt = phone_match.group(1).strip()
                
                # Парсим из текста: P:(518)-260-2222 или C:(518)-260-2222
                if not phone_primary:
                    phone_patterns = [
                        r'P:\s*\(?(\d{3})\)?\s*-?\s*(\d{3})\s*-?\s*(\d{4})',  # P:(518)-260-2222
                        r'Phone:\s*\(?(\d{3})\)?\s*-?\s*(\d{3})\s*-?\s*(\d{4})',  # Phone: (518) 260-2222
                        r'\((\d{3})\)\s*(\d{3})\s*-?\s*(\d{4})',  # (518) 260-2222
                    ]
                    for pattern in phone_patterns:
                        match = re.search(pattern, full_text)
                        if match:
                            phone_primary = f"({match.group(1)}) {match.group(2)}-{match.group(3)}"
                            break
                
                # Ищем альтернативный телефон (C:)
                if not phone_alt:
                    cell_match = re.search(r'C:\s*\(?(\d{3})\)?\s*-?\s*(\d{3})\s*-?\s*(\d{4})', full_text)
                    if cell_match:
                        phone_alt = f"({cell_match.group(1)}) {cell_match.group(2)}-{cell_match.group(3)}"
                
                # Парсим email
                email = None
                email_elem = elem.find('a', href=re.compile(r'mailto:'))
                if email_elem:
                    email_match = re.search(r'mailto:([^\s"\'<>]+)', email_elem.get('href', ''))
                    if email_match:
                        email = email_match.group(1).strip()
                else:
                    # Парсим email из текста
                    email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', full_text)
                    if email_match:
                        email = email_match.group(1).strip()
                
                # Парсим название офиса
                # Формат: "• Coldwell Banker Prime Properties"
                office_name = None
                office_match = re.search(r'•\s*([A-Z][^•P:C:@]+?)(?:\s+P:|C:|@|$)', full_text)
                if office_match:
                    office_name = office_match.group(1).strip()
                else:
                    # Ищем в отдельном элементе
                    office_elem = elem.find(['div', 'span'], class_=re.compile(r'office|brokerage|company', re.I))
                    if office_elem:
                        office_name = office_elem.get_text(strip=True)
                
                # Ищем телефон офиса
                office_phone = None
                office_phone_elem = elem.find(['div', 'span'], class_=re.compile(r'office.*phone|brokerage.*phone', re.I))
                if office_phone_elem:
                    office_phone_match = re.search(r'\(?(\d{3})\)?\s*-?\s*(\d{3})\s*-?\s*(\d{4})', office_phone_elem.get_text())
                    if office_phone_match:
                        office_phone = f"({office_phone_match.group(1)}) {office_phone_match.group(2)}-{office_phone_match.group(3)}"
                
                # Ищем фото
                photo_url = None
                img = elem.find('img')
                if img:
                    photo_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                    if photo_url and not photo_url.startswith('http'):
                        photo_url = urljoin(base_url, photo_url)
                
                # Ищем должность
                title = None
                title_elem = elem.find(['div', 'span'], class_=re.compile(r'title|position|role', re.I))
                if title_elem:
                    title = title_elem.get_text(strip=True)
                
                # Очищаем имя от лишних префиксов
                if name:
                    name = re.sub(r'^Listed\s+by\s*', '', name, flags=re.I).strip()
                    name = re.sub(r'\s*•.*$', '', name).strip()  # Убираем все после •
                
                agent = AgentData(
                    name=name,
                    title=title,
                    phone_primary=phone_primary,
                    phone_alt=phone_alt,
                    email=email,
                    photo_url=photo_url,
                    social_media=link,
                    office_name=office_name,
                    office_phone=office_phone,
                )
                agents.append(agent)
        
        # Удаляем дубликаты по имени и email
        seen = set()
        unique_agents = []
        for agent in agents:
            key = (agent.name, agent.email)
            if key not in seen and agent.name:
                seen.add(key)
                unique_agents.append(agent)
        
        return unique_agents

    @staticmethod
    def extract_details(soup: BeautifulSoup) -> dict[str, Any]:
        """Извлекает детали объявления (таблица) из HTML"""
        details = {}
        
        # Ищем таблицы с деталями
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    if key and value:
                        details[key.lower().replace(' ', '_')] = value
        
        # Ищем списки определений (dl)
        dl_elements = soup.find_all('dl')
        for dl in dl_elements:
            dts = dl.find_all('dt')
            dds = dl.find_all('dd')
            for dt, dd in zip(dts, dds):
                key = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                if key and value:
                    details[key.lower().replace(' ', '_')] = value
        
        # Ищем div с парами ключ-значение
        detail_divs = soup.find_all(['div', 'section'], class_=re.compile(r'detail|feature|spec', re.I))
        for div in detail_divs:
            # Ищем паттерн "Key: Value"
            text = div.get_text()
            matches = re.findall(r'([^:]+):\s*([^\n]+)', text)
            for key, value in matches:
                key = key.strip().lower().replace(' ', '_')
                value = value.strip()
                if key and value and len(key) < 50:
                    details[key] = value
        
        return details

    @staticmethod
    def extract_size(soup: BeautifulSoup) -> str | None:
        """Извлекает площадь из HTML (если не было найдено в API)"""
        # Ищем площадь в разных форматах
        size_patterns = [
            re.compile(r'(\d{1,3}(?:,\d{3})*)\s*(?:sq\.?\s*ft\.?|SF|square\s*feet)', re.I),
            re.compile(r'(\d+\.?\d*)\s*acres?', re.I),
        ]
        
        page_text = soup.get_text()
        for pattern in size_patterns:
            match = pattern.search(page_text)
            if match:
                size_val = match.group(1)
                if 'acre' in pattern.pattern.lower():
                    return f"{size_val} acres"
                else:
                    return f"{size_val.replace(',', '')} SF"
        
        return None

    def _save_html_if_needed(self, html: str, listing_id: str) -> None:
        """Сохраняет HTML в файл"""
        self.html_counter += 1
        if self.html_counter % self.save_html_every == 0:
            safe_filename = re.sub(r'[^\w\-_\.]', '_', listing_id)
            if not safe_filename or safe_filename == '_':
                safe_filename = f"listing_{self.html_counter}"
            filepath = os.path.join(self.html_save_dir, f"{safe_filename}.html")
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(html)
                logger.info(f"💾 Сохранен HTML [{self.html_counter}]: {filepath}")
            except Exception as e:
                logger.error(f"Ошибка при сохранении HTML: {e}")

    async def parse_listing_from_html_only(self, client: httpx.AsyncClient, url: str) -> DbDTO | None:
        """
        Парсит данные объявления напрямую из HTML страницы (для режима sitemap)
        """
        try:
            # Извлекаем ID из URL
            listing_id = "unknown"
            parsed = urlparse(url)
            path_parts = [p for p in parsed.path.split('/') if p]
            if path_parts:
                listing_id = path_parts[-1]
            
            # Загружаем HTML
            html = await self.get_listing_html(client, url)
            if not html:
                logger.warning(f"Не удалось загрузить HTML для {url}")
                return None
            
            soup = BeautifulSoup(html, 'lxml')
            
            # Создаем базовый DTO
            dto = DbDTO(
                source_name=self.source_name,
                listing_id=listing_id,
                listing_link=url,
                listing_type='sale',
                listing_status="Available",
                address="Address not found",
                sale_price=None,
                lease_price=None,
                size=None,
                property_description=None,
                listing_details={},
                photos=[],
                brochure_pdf=None,
                mls_number=None,
                agents=[],
                agency_phone=None,
            )
            
            # Извлекаем данные из JSON-LD структурированных данных
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    if not script.string:
                        continue
                    data = json.loads(script.string)
                    
                    # Может быть SingleFamilyResidence, Product, или другой тип
                    if isinstance(data, dict):
                        # Address
                        if 'address' in data:
                            addr = data['address']
                            if isinstance(addr, dict):
                                street = addr.get('streetAddress', '')
                                city = addr.get('addressLocality', '')
                                state = addr.get('addressRegion', '')
                                zip_code = addr.get('postalCode', '')
                                parts = [p for p in [street, city, state, zip_code] if p]
                                if parts:
                                    dto.address = ', '.join(parts)
                        
                        # Description
                        if 'description' in data and not dto.property_description:
                            desc = data['description']
                            if isinstance(desc, str) and len(desc) > 50:
                                dto.property_description = desc
                        
                        # Price
                        if 'offers' in data:
                            offers = data['offers']
                            if isinstance(offers, dict):
                                price = offers.get('price')
                                if price:
                                    dto.sale_price = f"${price:,.0f}" if isinstance(price, (int, float)) else str(price)
                        
                        # Photos
                        if 'image' in data:
                            images = data['image']
                            if isinstance(images, list):
                                dto.photos = images
                            elif isinstance(images, str):
                                dto.photos = [images]
                        
                        # Additional properties
                        if 'numberOfRooms' in data:
                            dto.listing_details['rooms'] = data['numberOfRooms']
                        if 'numberOfBedrooms' in data:
                            dto.listing_details['bedrooms'] = data['numberOfBedrooms']
                        if 'numberOfBathroomsTotal' in data:
                            dto.listing_details['bathrooms'] = data['numberOfBathroomsTotal']
                        if 'floorSize' in data:
                            floor_size = data['floorSize']
                            if isinstance(floor_size, dict) and 'value' in floor_size:
                                dto.size = f"{floor_size['value']:,} SF"
                            elif isinstance(floor_size, str):
                                dto.size = floor_size
                
                except (json.JSONDecodeError, KeyError, AttributeError) as e:
                    logger.debug(f"Ошибка при парсинге JSON-LD: {e}")
                    continue
            
            # Дополняем данными из HTML парсинга
            self._enrich_from_html(dto, html, listing_id)
            
            return dto
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге {url}: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def run(self, location: str = "new-york", max_results: int = 1000, mode: str = "sitemap") -> list[DbDTO]:
        """
        Основной процесс парсинга (асинхронно)
        
        Args:
            location: Локация для поиска (используется только в режиме api)
            max_results: Максимальное количество результатов
            mode: Режим работы - "sitemap" (через robots.txt) или "api" (через API)
        """
        async with httpx.AsyncClient() as client:
            if mode == "sitemap":
                # Новый режим: через сайтмапы из robots.txt
                logger.info("[SITEMAP MODE] Используются сайтмапы из robots.txt")
                
                # Получаем все URL с /homedetails из сайтмапов
                listing_urls = await self.get_all_listing_urls_from_sitemaps(client, max_urls=max_results)
                
                if not listing_urls:
                    logger.warning("Не найдено URL в сайтмапах")
                    return []
                
                # Парсим каждый URL параллельно
                logger.info(f"[2-3] Начинаю обработку {len(listing_urls)} листингов из сайтмапов...")
                
                tasks = [
                    self.parse_listing_from_html_only(client, url)
                    for url in listing_urls
                ]
                
                # Выполняем все задачи параллельно
                results = []
                completed = 0
                for coro in asyncio.as_completed(tasks):
                    try:
                        dto = await coro
                        completed += 1
                        if dto:
                            results.append(dto)
                            logger.info(f"✓ [{completed}/{len(listing_urls)}] Успешно: {dto.address}")
                        else:
                            logger.warning(f"⚠ [{completed}/{len(listing_urls)}] Не удалось распарсить объявление")
                    except Exception as e:
                        completed += 1
                        logger.error(f"❌ [{completed}/{len(listing_urls)}] Ошибка при обработке: {e}")
                
                logger.info(f"\nОбработано объявлений: {len(results)}/{len(listing_urls)}")
                return results
                
            else:
                # Старый режим: через API
                logger.info("[API MODE] Используется API для получения данных")
                listings_data = await self.get_listings_from_api(client, location, max_results)
                
                if not listings_data:
                    logger.warning("Не найдено данных объявлений через API")
                    return []
                
                # Парсим данные из API ответа параллельно
                logger.info(f"[2-3] Начинаю обработку {len(listings_data)} объявлений из API...")
                
                # Создаем задачи для параллельной обработки
                tasks = [
                    self.parse_listing_from_api_data(client, listing_data)
                    for listing_data in listings_data
                ]
                
                # Выполняем все задачи параллельно
                results = []
                completed = 0
                for coro in asyncio.as_completed(tasks):
                    try:
                        dto = await coro
                        completed += 1
                        if dto:
                            results.append(dto)
                            logger.info(f"✓ [{completed}/{len(listings_data)}] Успешно: {dto.address}")
                        else:
                            logger.warning(f"⚠ [{completed}/{len(listings_data)}] Не удалось распарсить объявление")
                    except Exception as e:
                        completed += 1
                        logger.error(f"❌ [{completed}/{len(listings_data)}] Ошибка при обработке: {e}")
                
                logger.info(f"\nОбработано объявлений: {len(results)}/{len(listings_data)}")
                return results

if __name__ == '__main__':
    async def main():
        parser = CompassParser()
        # Используем режим sitemap для получения данных через сайтмапы
        results = await parser.run(max_results=10, mode="sitemap")
        print(f"Получено {len(results)} объявлений")
    
    asyncio.run(main())
