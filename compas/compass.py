import asyncio
import json
import logging
import os
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from schema import DbDTO, AgentData

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CompassParser:
    """
    Парсер для compass.com с использованием Selenium
    1. Получение списка объявлений через браузер (обход защиты)
    2. Получение HTML для каждого листинга
    3. Парсинг window.__INITIAL_DATA__ и извлечение обязательных полей
    """
    
    def __init__(
        self,
        headless: bool = True,
        save_html_every: int = 20,
        html_save_dir: str = "htmls",
        page_load_timeout: int = 30,
    ) -> None:
        self.source_name = "compass"
        self.base_url = "https://www.compass.com"
        
        # Настройки Selenium
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument("--headless=new")
        
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        self.chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)
        
        self.page_load_timeout = page_load_timeout
        self.driver = None
        
        # Настройки сохранения HTML
        self.save_html_every = save_html_every
        self.html_save_dir = html_save_dir
        self.html_counter = 0
        
        # Создаем папку для сохранения HTML, если её нет
        if not os.path.exists(self.html_save_dir):
            os.makedirs(self.html_save_dir)
            logger.info(f"Создана папка для сохранения HTML: {self.html_save_dir}")

    def start_driver(self):
        """Запуск драйвера"""
        if not self.driver:
            logger.info("Запуск Chrome driver...")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            
            # Маскировка webdriver
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })

    def stop_driver(self):
        """Остановка драйвера"""
        if self.driver:
            logger.info("Остановка Chrome driver...")
            self.driver.quit()
            self.driver = None

    def get_page_source(self, url: str) -> str | None:
        """Получает HTML страницы через Selenium"""
        if not self.driver:
            self.start_driver()
        
        try:
            logger.info(f"Загрузка страницы: {url}")
            self.driver.get(url)
            
            # Ждем загрузки контента (например, заголовка или списка)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Даем время на выполнение JS скриптов
                time.sleep(2)
            except:
                pass
            
            return self.driver.page_source
        except Exception as e:
            logger.error(f"Ошибка при загрузке страницы {url}: {e}")
            # Если драйвер упал, перезапустим его
            try:
                self.stop_driver()
            except:
                pass
            return None

    # ---------------------- ЭТАП 1: ИНДЕКСАЦИЯ ----------------------

    def get_listing_urls_from_search(self, location: str = "new-york", max_results: int = 1000) -> list[str]:
        """
        ЭТАП 1: Получает список URL объявлений через парсинг страниц поиска (Selenium)
        """
        logger.info(f"[1] Получаю список объявлений для локации: {location}")
        
        if not self.driver:
            self.start_driver()
            
        urls = []
        page = 1
        
        try:
            while len(urls) < max_results:
                # URL страницы поиска
                search_url = f"{self.base_url}/homes-for-sale/{location}/"
                if page > 1:
                    search_url += f"?page={page}"
                
                logger.info(f"[1] Парсинг страницы {page}: {search_url}")
                
                html = self.get_page_source(search_url)
                if not html:
                    logger.warning(f"[1] Не удалось получить HTML для страницы {page}")
                    break
                
                # Сохраняем HTML поиска для отладки
                if page == 1:
                    debug_file = os.path.join(self.html_save_dir, f"search_page_{page}_selenium.html")
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(html)
                
                # Извлекаем ссылки
                new_urls = self._extract_urls_from_html(html)
                
                # Фильтруем уже найденные
                page_urls = [url for url in new_urls if url not in urls]
                
                if not page_urls:
                    logger.info(f"[1] На странице {page} не найдено новых объявлений. Завершаем парсинг.")
                    break
                
                urls.extend(page_urls)
                logger.info(f"[1] На странице {page} найдено {len(page_urls)} новых объявлений. Всего: {len(urls)}")
                
                # Переходим на следующую страницу
                page += 1
                
                # Ограничение на количество страниц для безопасности
                if page > 50:
                    logger.warning(f"[1] Достигнут лимит страниц (50). Остановка.")
                    break
                
                # Задержка между страницами
                time.sleep(2)
            
            logger.info(f"[1] Итого найдено {len(urls)} ссылок на объявления")
            return urls[:max_results]
            
        except Exception as e:
            logger.error(f"[1] Ошибка при получении списка объявлений: {e}")
            import traceback
            traceback.print_exc()
            return urls

    def _extract_urls_from_html(self, html: str) -> list[str]:
        """Извлекает ссылки на объявления из HTML"""
        urls = []
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. Пробуем найти через __INITIAL_DATA__
        initial_data = self.extract_initial_data(html)
        if initial_data:
            listings = self.extract_listings_from_initial_data(initial_data)
            for listing in listings:
                url = None
                if isinstance(listing, dict):
                    # Пробуем сформировать URL из разных полей
                    listing_id = (
                        listing.get('id') or 
                        listing.get('listingId') or 
                        listing.get('mlsNumber') or
                        listing.get('listingIdSHA')
                    )
                    
                    # Пробуем получить URL из location (как в примере пользователя)
                    if not url and listing.get('location') and listing.get('location', {}).get('seoId'):
                        # Формат: /homes-for-sale/{seoId}/{listingIdSHA}/
                        seo_id = listing['location']['seoId']
                        sha_id = listing.get('listingIdSHA') or listing_id
                        if sha_id:
                            url = f"{self.base_url}/homes-for-sale/{seo_id}/{sha_id}/"
                    
                    # Пробуем стандартный URL
                    if not url and listing_id:
                        url = f"{self.base_url}/homes-for-sale/{listing_id}/"
                        
                    # Пробуем поле pageLink (из примера пользователя)
                    if not url and listing.get('pageLink'):
                        url = listing['pageLink']
                
                if url:
                    if not url.startswith('http'):
                        url = urljoin(self.base_url, url)
                    if url not in urls:
                        urls.append(url)
        
        # 2. Если мало ссылок, ищем в HTML
        if len(urls) < 5:
            # Ищем ссылки вида /homes-for-sale/{location}/{id}/ или /homes-for-sale/{id}/
            all_links = soup.find_all('a', href=True)
            known_locations = {
                'new-york', 'los-angeles', 'san-francisco', 'chicago', 
                'boston', 'miami', 'seattle', 'washington-dc', 'brooklyn',
                'manhattan', 'queens', 'bronx', 'staten-island'
            }
            
            for link in all_links:
                href = link.get('href', '')
                if not href or '=' in href or '?' in href or href.startswith('#'):
                    continue
                
                # Паттерн URL
                match = re.search(r'/homes-for-sale/([^/]+)/([^/]+)/?$', href)
                if match:
                    part1, part2 = match.groups()
                    listing_id = part2 if part1 in known_locations else part1
                    
                    if len(listing_id) > 5 and not listing_id.startswith(('start', 'page', 'sort')):
                        full_url = urljoin(self.base_url, href)
                        if full_url not in urls:
                            urls.append(full_url)
                            
        return urls

    @staticmethod
    def extract_initial_data(html: str) -> dict[str, Any] | None:
        """Извлекает window.__INITIAL_DATA__ из HTML страницы"""
        try:
            soup = BeautifulSoup(html, 'lxml')
            scripts = soup.find_all('script')
            
            for script in scripts:
                script_text = script.string or script.get_text()
                if not script_text:
                    continue
                
                # Ищем window.__INITIAL_DATA__ = {...}
                # Обновленный паттерн, учитывающий пробелы и переносы
                patterns = [
                    r'window\.__INITIAL_DATA__\s*=\s*({.+?});',
                    r'__INITIAL_DATA__\s*=\s*({.+?});',
                ]
                
                for pattern in patterns:
                    matches = list(re.finditer(pattern, script_text, re.DOTALL))
                    for match in matches:
                        json_str = match.group(1).strip()
                        try:
                            # Пробуем распарсить JSON
                            data = json.loads(json_str)
                            if isinstance(data, dict) and len(data) > 0:
                                return data
                        except json.JSONDecodeError:
                            pass
            return None
        except Exception:
            return None

    @staticmethod
    def extract_listings_from_initial_data(data: dict[str, Any]) -> list[dict[str, Any]]:
        """Извлекает список объявлений из __INITIAL_DATA__"""
        listings = []
        
        # Рекурсивный поиск ключа 'listings' или 'listing'
        def find_key(obj, key):
            if isinstance(obj, dict):
                if key in obj:
                    return obj[key]
                for k, v in obj.items():
                    res = find_key(v, key)
                    if res: return res
            elif isinstance(obj, list):
                for item in obj:
                    res = find_key(item, key)
                    if res: return res
            return None

        # Ищем список объявлений
        found = find_key(data, 'listings') or find_key(data, 'cards')
        if found and isinstance(found, list):
            listings = found
            
        return listings

    # ---------------------- ЭТАП 3: ПАРСИНГ ДАННЫХ ----------------------

    def parse_listing(self, url: str) -> DbDTO | None:
        """
        ЭТАП 3: Парсит HTML и извлекает обязательные поля из window.__INITIAL_DATA__
        """
        html = self.get_page_source(url)
        if not html:
            return None
        
        # Извлекаем ID из URL для сохранения файла
        listing_id = "unknown"
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        if path_parts:
            listing_id = path_parts[-1]
        
        self._save_html_if_needed(html, listing_id)
        
        # Извлекаем данные
        initial_data = self.extract_initial_data(html)
        if not initial_data:
            logger.warning(f"Не удалось извлечь __INITIAL_DATA__ для {url}")
            return None
        
        # Ищем объект листинга внутри данных
        listing_data = self._find_listing_data(initial_data)
        if not listing_data:
            logger.warning(f"Не удалось найти данные объявления в JSON для {url}")
            return None
            
        # Маппинг данных в DbDTO
        try:
            return self._map_to_dto(listing_data, url, listing_id)
        except Exception as e:
            logger.error(f"Ошибка при маппинге данных для {url}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _find_listing_data(self, data: dict) -> dict | None:
        """Находит данные конкретного объявления в структуре"""
        # Обычно это listingRelation -> listing или просто listing
        if 'listingRelation' in data and 'listing' in data['listingRelation']:
            return data['listingRelation']['listing']
        
        if 'listing' in data:
            return data['listing']
            
        # Рекурсивный поиск
        def find_listing(obj):
            if isinstance(obj, dict):
                if 'listingIdSHA' in obj or 'compassPropertyId' in obj:
                    return obj
                for k, v in obj.items():
                    res = find_listing(v)
                    if res: return res
            return None
            
        return find_listing(data)

    def _map_to_dto(self, data: dict, url: str, listing_id: str) -> DbDTO:
        """Преобразует JSON данные в DbDTO"""
        
        # Helper для безопасного получения вложенных полей
        def get_val(obj, path, default=None):
            for key in path.split('.'):
                if isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    return default
            return obj

        # Address
        location = data.get('location', {})
        address = location.get('prettyAddress') or get_val(data, 'address.prettyAddress') or "Address not found"
        
        # Price
        price_val = get_val(data, 'price.listed') or get_val(data, 'price.lastKnown')
        price_str = f"${price_val:,.0f}" if price_val else None
        
        # Type
        listing_type_code = data.get('listingType')
        listing_type = 'sale'  # Default
        if listing_type_code == 1: listing_type = 'lease' # Пример, нужно уточнять коды
        if 'rent' in str(data.get('detailedPropertyType', '')).lower():
            listing_type = 'lease'
            
        sale_price = price_str if listing_type == 'sale' else None
        lease_price = price_str if listing_type == 'lease' else None
        
        # Size
        size_sqft = get_val(data, 'size.squareFeet')
        size = f"{size_sqft:,.0f} SF" if size_sqft else None
        
        # Description
        description = data.get('description')
        
        # Photos
        photos = []
        media = data.get('media', [])
        for item in media:
            if item.get('type') == 0 and item.get('url'): # 0 = image
                photos.append(item['url'])
        
        # Если фото не в media, ищем в других местах
        if not photos:
            # Compass часто использует gallery
            gallery = get_val(data, 'gallery', [])
            for item in gallery:
                if item.get('url'): photos.append(item['url'])

        # Agents
        agents = []
        # TODO: Реализовать извлечение агентов из listing.agents
        
        # Status
        status = data.get('localizedStatus') or "Available"
        
        # Details
        details = {
            'bedrooms': get_val(data, 'size.bedrooms'),
            'bathrooms': get_val(data, 'size.bathrooms'),
            'year_built': get_val(data, 'yearBuilt'),
            'property_type': get_val(data, 'propertyType.name'),
        }

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
            listing_details=details,
            photos=photos,
            brochure_pdf=None,
            mls_number=None,
            agents=agents,
            agency_phone=None,
        )

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
        """Основной процесс"""
        try:
            # 1. Получаем ссылки
            urls = self.get_listing_urls_from_search(location, max_results)
            if not urls:
                logger.warning("Не найдено ссылок на объявления")
                return []
            
            # 2. Парсим каждое объявление
            results = []
            logger.info(f"[2-3] Начинаю обработку {len(urls)} объявлений...")
            
            for i, url in enumerate(urls):
                logger.info(f"Обработка [{i+1}/{len(urls)}]: {url}")
                dto = self.parse_listing(url)
                if dto:
                    results.append(dto)
                    logger.info(f"✓ Успешно: {dto.address}")
                
                # Задержка между запросами
                time.sleep(1)
                
            logger.info(f"\nОбработано объявлений: {len(results)}/{len(urls)}")
            return results
            
        finally:
            self.stop_driver()

if __name__ == '__main__':
    parser = CompassParser(headless=False) # Headless=False для отладки
    parser.run("new-york", 5)
