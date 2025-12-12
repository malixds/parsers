import asyncio
import json
import logging
import os
import re
import time
import uuid
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
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
    Парсер для compass.com через API
    1. Получение списка объявлений через API (POST запросы)
    2. Получение HTML для каждого листинга через requests
    3. Парсинг данных из API и HTML для извлечения обязательных полей
    """
    
    def __init__(
        self,
        save_html_every: int = 20,
        html_save_dir: str = "htmls",
    ) -> None:
        self.source_name = "compass"
        self.base_url = "https://www.compass.com"
        
        # Настройки сохранения HTML
        self.save_html_every = save_html_every
        self.html_save_dir = html_save_dir
        self.html_counter = 0
        
        # Создаем папку для сохранения HTML, если её нет
        if not os.path.exists(self.html_save_dir):
            os.makedirs(self.html_save_dir)
            logger.info(f"Создана папка для сохранения HTML: {self.html_save_dir}")

    # ---------------------- ЭТАП 1: ИНДЕКСАЦИЯ ----------------------

    def get_listings_from_api(self, location: str = "new-york", max_results: int = 1000) -> list[dict]:
        """
        ЭТАП 1 (API): Получает данные объявлений через API compass.com
        Возвращает список словарей с данными объявлений (включая listing объекты)
        """
        logger.info(f"[1-API] Получаю данные объявлений через API для локации: {location}")
        
        listings_data = []
        search_result_id = str(uuid.uuid4())
        
        # Базовые координаты для New York (можно расширить для других локаций)
        # Эти координаты покрывают весь штат NY
        ne_point = {"latitude": 45.3525295, "longitude": -72.3285732}
        sw_point = {"latitude": 39.9017281, "longitude": -79.2115078}
        viewport_ne = {"lat": 45.2954092, "lng": -72.3285732}
        viewport_sw = {"lat": 39.839376, "lng": -79.2115078}
        
        # Пробуем получить locationId из первой страницы (если нужно)
        # Пока используем общие координаты
        
        page = 0
        num_per_page = 50  # Максимум результатов за запрос
        
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
                    'Connection': 'keep-alive',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'Priority': 'u=6',
                }
                
                params = {
                    'searchQuery': '{"sort":{"column":"dom","direction":"asc"}}',
                }
                
                json_data = {
                    'searchResultId': search_result_id,
                    'rawLolSearchQuery': {
                        'listingTypes': [2],  # 2 = For Sale
                        'nePoint': ne_point,
                        'swPoint': sw_point,
                        'saleStatuses': [12, 9],  # Active listings
                        'num': num_per_page,
                        'start': page * num_per_page,
                        'sortOrder': 46,  # DOM ascending
                        'facetFieldNames': [
                            'contributingDatasetList',
                            'compassListingTypes',
                            'comingSoon',
                        ],
                    },
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
                    response = requests.post(
                        api_url,
                        params=params,
                        json=json_data,
                        headers=headers,
                        timeout=30
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    if 'lolResults' not in data or 'data' not in data['lolResults']:
                        logger.warning(f"[1-API] Неожиданная структура ответа API")
                        break
                    
                    listings = data['lolResults']['data']
                    total_items = data['lolResults'].get('totalItems', 0)
                    
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
                    
                    # Если получили меньше, чем запрашивали, или достигли лимита, завершаем
                    if len(listings) < num_per_page or len(listings_data) >= max_results:
                        logger.info(f"[1-API] Получено меньше результатов или достигнут лимит. Завершаем.")
                        break
                    
                    # Если total_items меньше или равно текущему количеству, завершаем
                    if total_items > 0 and len(listings_data) >= total_items:
                        logger.info(f"[1-API] Получены все доступные объявления ({total_items}). Завершаем.")
                        break
                    
                    page += 1
                    
                    # Задержка между запросами
                    time.sleep(1)
                    
                except requests.exceptions.RequestException as e:
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

    def get_listing_html(self, url: str) -> str | None:
        """Получает HTML страницы объявления через requests"""
        try:
            headers = {
                'User-Agent': UserAgent().random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.compass.com/',
                'Connection': 'keep-alive',
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.warning(f"Не удалось загрузить HTML для {url}: {e}")
            return None

    def parse_listing_from_api_data(self, listing_data: dict) -> DbDTO | None:
        """
        ЭТАП 3 (API): Парсит данные объявления напрямую из API ответа
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
            html = self.get_listing_html(url)
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
                # Ищем имя
                name_elem = elem.find(['h3', 'h4', 'h5', 'div'], class_=re.compile(r'name|agent', re.I))
                if not name_elem:
                    name_elem = elem.find('a', href=re.compile(r'/agent/|/team/'))
                
                if name_elem:
                    name = name_elem.get_text(strip=True)
                    if name and len(name) > 2:
                        # Ищем ссылку
                        link = None
                        a_tag = name_elem.find('a') if name_elem.name != 'a' else name_elem
                        if a_tag:
                            link = a_tag.get('href', '')
                            if link and not link.startswith('http'):
                                link = urljoin(base_url, link)
                        
                        # Ищем телефон
                        phone = None
                        phone_elem = elem.find('a', href=re.compile(r'tel:'))
                        if phone_elem:
                            phone_match = re.search(r'tel:([\d\s\-\(\)]+)', phone_elem.get('href', ''))
                            if phone_match:
                                phone = phone_match.group(1).strip()
                        
                        # Ищем email
                        email = None
                        email_elem = elem.find('a', href=re.compile(r'mailto:'))
                        if email_elem:
                            email_match = re.search(r'mailto:([^\s]+)', email_elem.get('href', ''))
                            if email_match:
                                email = email_match.group(1).strip()
                        
                        # Ищем фото
                        photo_url = None
                        img = elem.find('img')
                        if img:
                            photo_url = img.get('src') or img.get('data-src')
                            if photo_url and not photo_url.startswith('http'):
                                photo_url = urljoin(base_url, photo_url)
                        
                        # Ищем должность
                        title = None
                        title_elem = elem.find(['div', 'span'], class_=re.compile(r'title|position|role', re.I))
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                        
                        agent = AgentData(
                            name=name,
                            title=title,
                            phone_primary=phone,
                            email=email,
                            photo_url=photo_url,
                            social_media=link,
                        )
                        agents.append(agent)
        
        return agents

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

    def run(self, location: str = "new-york", max_results: int = 1000) -> list[DbDTO]:
        """Основной процесс - использует только API"""
        # Получаем данные объявлений через API
        logger.info("[API MODE] Используется API для получения данных")
        listings_data = self.get_listings_from_api(location, max_results)
        
        if not listings_data:
            logger.warning("Не найдено данных объявлений через API")
            return []
        
        # Парсим данные из API ответа
        results = []
        logger.info(f"[2-3] Начинаю обработку {len(listings_data)} объявлений из API...")
        
        for i, listing_data in enumerate(listings_data):
            logger.info(f"Обработка [{i+1}/{len(listings_data)}]...")
            dto = self.parse_listing_from_api_data(listing_data)
            if dto:
                results.append(dto)
                logger.info(f"✓ Успешно: {dto.address}")
            else:
                logger.warning(f"⚠ Не удалось распарсить объявление {i+1}")
        
        logger.info(f"\nОбработано объявлений: {len(results)}/{len(listings_data)}")
        return results

if __name__ == '__main__':
    parser = CompassParser()
    parser.run("new-york", 5)
