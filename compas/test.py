import requests
from fake_useragent import UserAgent
import xml.etree.ElementTree as ET
import json
import time
import uuid
import asyncio
import httpx
import re

headers = {
    'User-Agent': UserAgent().random,
}

response = requests.get(
    'https://www.compass.com/robots.txt',
    headers=headers,
)

sitemaps = [
    'https://www.compass.com/sitemaps/for-sale/index.xml',
    'https://www.compass.com/sitemaps/for-rent/index.xml',
    'https://www.compass.com/sitemaps/agent-pages/index.xml',
    'https://www.compass.com/sitemaps/agent-office-pages/index.xml',
    'https://www.compass.com/sitemaps/agent-location-pages/index.xml',
    'https://www.compass.com/sitemaps/static/index.xml',
    'https://www.compass.com/sitemaps/recently-sold/index.xml',
    'https://www.compass.com/sitemaps/building/index.xml',
    'https://www.compass.com/sitemaps/neighborhood-guides/index.xml',
    'https://www.compass.com/sitemaps/newsroom/index.xml',
    'https://www.compass.com/xmlsitemaps/pdp/for-sale-by-compass_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/pdp/for-rent-by-compass_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/pdp/for-sale-by-agent_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/pdp/for-sale-by-owner_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/pdp/for-rent_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/pdp/pending_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/pdp/recently-sold_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/pdp/off-market_index_pdp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/for-sale-by-compass_index_ldp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/for-rent-by-compass_index_ldp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/for-sale-by-agent_index_ldp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/for-sale-by-owner_index_ldp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/for-rent_index_ldp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/pending_index_ldp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/recently-sold_index_ldp.xml',
    'https://www.compass.com/xmlsitemaps/ldp/off-market_index_ldp.xml',
]

# Пример: парсинг sitemap index
sitemap_url = 'https://www.compass.com/sitemaps/for-sale/index.xml'
response = requests.get(sitemap_url, headers=headers)

if response.status_code == 200:
    # Парсим XML
    root = ET.fromstring(response.content)
    
    # Определяем namespace для sitemap
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    # Извлекаем все ссылки на sitemap файлы
    sitemap_links = []
    for sitemap_elem in root.findall('ns:sitemap', namespace):
        loc = sitemap_elem.find('ns:loc', namespace)
        lastmod = sitemap_elem.find('ns:lastmod', namespace)
        if loc is not None:
            sitemap_info = {
                'url': loc.text,
                'lastmod': lastmod.text if lastmod is not None else None
            }
            sitemap_links.append(sitemap_info)
            print(f"Sitemap: {sitemap_info['url']} (Last modified: {sitemap_info['lastmod']})")
    
    print(f"\nВсего найдено sitemap файлов: {len(sitemap_links)}")
    
    # Пример: парсим первый sitemap файл для получения URL страниц
    if sitemap_links:
        print(f"\nПарсим первый sitemap: {sitemap_links[0]['url']}")
        sitemap_response = requests.get(sitemap_links[0]['url'], headers=headers)
        if sitemap_response.status_code == 200:
            sitemap_root = ET.fromstring(sitemap_response.content)
            urls = []
            for url_elem in sitemap_root.findall('ns:url', namespace):
                loc = url_elem.find('ns:loc', namespace)
                if loc is not None:
                    urls.append(loc.text)
            
            print(f"Найдено URL страниц: {len(urls)}")
            print("Первые 5 URL:")
            for url in urls[:5]:
                print(f"  - {url}")
else:
    print(f"Ошибка при получении sitemap: {response.status_code}")

# ========== Получение всех ссылок на объявления с пагинацией (асинхронная версия) ==========

async def fetch_page_links(
    client: httpx.AsyncClient,
    api_url: str,
    page: int,
    start: int,
    num_per_page: int,
    search_result_id: str,
    location_ids: list,
    viewport_ne: dict,
    viewport_sw: dict,
    post_headers: dict
) -> tuple[int, list]:
    """
    Асинхронно получает ссылки с одной страницы
    
    Returns:
        tuple: (page_number, list_of_links)
    """
    search_query = {
        "sort": {"column": "dom", "direction": "asc"},
        "start": start
    }
    params = {
        'searchQuery': json.dumps(search_query),
    }
    
    raw_query = {
        'listingTypes': [2],
        'saleStatuses': [12, 9],
        'num': num_per_page,
        'start': start,
        'sortOrder': 46,
        'facetFieldNames': [
            'contributingDatasetList',
            'compassListingTypes',
            'comingSoon',
        ],
    }
    
    if location_ids:
        raw_query['locationIds'] = location_ids
    elif viewport_ne and viewport_sw:
        # Используем координаты только если они есть
        raw_query['nePoint'] = {
            'latitude': viewport_ne['lat'],
            'longitude': viewport_ne['lng'],
        }
        raw_query['swPoint'] = {
            'latitude': viewport_sw['lat'],
            'longitude': viewport_sw['lng'],
        }
    
    json_data = {
        'searchResultId': search_result_id,
        'rawLolSearchQuery': raw_query,
        'viewport': {
            'northeast': viewport_ne if viewport_ne else {},
            'southwest': viewport_sw if viewport_sw else {},
        },
        'viewportFrom': 'response',
        'height': 1350,
        'width': 1253,
        'isMapFullyInitialized': True,
        'purpose': 'search',
    }
    
    try:
        response = await client.post(
            api_url,
            params=params,
            json=json_data,
            headers=post_headers,
            timeout=30.0
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Извлекаем данные из ответа
        listings = []
        
        if 'data' in data:
            if isinstance(data['data'], list):
                listings = data['data']
            elif isinstance(data['data'], dict):
                if 'listing' in data['data']:
                    listings = [data['data']]
                elif isinstance(data['data'].get('listings'), list):
                    listings = data['data']['listings']
        elif 'lolResults' in data:
            if 'data' in data['lolResults']:
                listings = data['lolResults']['data']
        
        # Извлекаем navigationPageLink
        page_links = []
        for item in listings:
            listing = None
            if 'listing' in item:
                listing = item['listing']
            elif isinstance(item, dict) and 'navigationPageLink' in item:
                listing = item
            else:
                listing = item
            
            if listing and isinstance(listing, dict):
                navigation_link = listing.get('navigationPageLink')
                if navigation_link:
                    full_url = f'https://www.compass.com{navigation_link}'
                    page_links.append(full_url)
        
        return (page, page_links)
        
    except Exception as e:
        print(f"Ошибка при запросе страницы {page + 1}: {e}")
        return (page, [])


async def get_all_listing_links_async(location_url: str, concurrency: int = 10):
    """
    Асинхронно получает все ссылки на объявления (navigationPageLink) со всех страниц
    
    Args:
        location_url: URL страницы, например 'https://www.compass.com/homes-for-sale/arizona/'
        concurrency: Количество одновременных запросов (по умолчанию 10)
    
    Returns:
        list: Массив всех ссылок на объявления
    """
    get_headers = {
        'User-Agent': UserAgent().random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    post_headers = {
    'User-Agent': UserAgent().random,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
        'Referer': location_url,
    'Content-Type': 'application/json',
    'Origin': 'https://www.compass.com',
    'Sec-GPC': '1',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=6',
}

    num_per_page = 40
    search_result_id = str(uuid.uuid4())
    location_ids = None
    viewport_ne = None
    viewport_sw = None
    api_url = location_url.rstrip('/')
    
    # Пытаемся извлечь координаты из URL, если там есть mapview
    # Например: /homes-for-sale/arizona/mapview=37.0,-109.0,31.0,-114.0/
    mapview_match = re.search(r'mapview=([\d.-]+),([\d.-]+),([\d.-]+),([\d.-]+)', location_url)
    if mapview_match:
        viewport_ne = {'lat': float(mapview_match.group(1)), 'lng': float(mapview_match.group(2))}
        viewport_sw = {'lat': float(mapview_match.group(3)), 'lng': float(mapview_match.group(4))}
        print(f"Координаты извлечены из URL: NE={viewport_ne}, SW={viewport_sw}")
    
    print(f"\nНачинаем асинхронный сбор ссылок на объявления из: {location_url}")
    print(f"Concurrency: {concurrency}")
    
    # Создаем семафор для ограничения количества одновременных запросов
    semaphore = asyncio.Semaphore(concurrency)
    
    async with httpx.AsyncClient() as client:
        # Если координаты не были извлечены из URL, используем дефолтные широкие координаты для первого запроса
        # Они будут обновлены из ответа API
        if not viewport_ne or not viewport_sw:
            # Дефолтные координаты для США (широкий охват)
            viewport_ne = {'lat': 49.0, 'lng': -66.0}
            viewport_sw = {'lat': 24.0, 'lng': -125.0}
            print("Используем дефолтные координаты для первого запроса, будут обновлены из ответа API")
        
        # Делаем первый запрос для получения locationIds, координат и определения общего количества страниц
        print("Получаем начальные параметры...")
        async with semaphore:
            try:
                search_query = {"sort": {"column": "dom", "direction": "asc"}, "start": 0}
                params = {'searchQuery': json.dumps(search_query)}
                raw_query = {
                    'listingTypes': [2],
                    'saleStatuses': [12, 9],
                    'num': num_per_page,
                    'start': 0,
                    'sortOrder': 46,
                    'facetFieldNames': ['contributingDatasetList', 'compassListingTypes', 'comingSoon'],
                }
                
                # Добавляем координаты только если они есть
                if viewport_ne and viewport_sw:
                    raw_query['nePoint'] = {'latitude': viewport_ne['lat'], 'longitude': viewport_ne['lng']}
                    raw_query['swPoint'] = {'latitude': viewport_sw['lat'], 'longitude': viewport_sw['lng']}
                
                json_data = {
                    'searchResultId': search_result_id,
                    'rawLolSearchQuery': raw_query,
                    'viewport': {'northeast': viewport_ne, 'southwest': viewport_sw} if viewport_ne and viewport_sw else {},
                    'viewportFrom': 'response',
                    'height': 1350,
                    'width': 1253,
                    'isMapFullyInitialized': True,
                    'purpose': 'search',
                }

                response = await client.post(api_url, params=params, json=json_data, headers=post_headers, timeout=30.0)
                response.raise_for_status()
                data = response.json()
                
                # Получаем viewport из ответа, если его там нет
                if 'viewport' in data:
                    resp_viewport = data['viewport']
                    if 'northeast' in resp_viewport and 'southwest' in resp_viewport:
                        viewport_ne = resp_viewport['northeast']
                        viewport_sw = resp_viewport['southwest']
                        print(f"Координаты получены из ответа API: NE={viewport_ne}, SW={viewport_sw}")
                
                # Получаем locationIds из ответа
                if 'rawLolSearchQuery' in data and 'locationIds' in data.get('rawLolSearchQuery', {}):
                    location_ids = data['rawLolSearchQuery']['locationIds']
                    print(f"Найдены locationIds: {location_ids}")
                
                # Извлекаем ссылки из первого ответа
                listings = []
                if 'data' in data:
                    if isinstance(data['data'], list):
                        listings = data['data']
                    elif isinstance(data['data'], dict):
                        if 'listing' in data['data']:
                            listings = [data['data']]
                        elif isinstance(data['data'].get('listings'), list):
                            listings = data['data']['listings']
                elif 'lolResults' in data:
                    if 'data' in data['lolResults']:
                        listings = data['lolResults']['data']
                
                # Извлекаем navigationPageLink из первого ответа
                first_links = []
                for item in listings:
                    listing = None
                    if 'listing' in item:
                        listing = item['listing']
                    elif isinstance(item, dict) and 'navigationPageLink' in item:
                        listing = item
                    else:
                        listing = item
                    
                    if listing and isinstance(listing, dict):
                        navigation_link = listing.get('navigationPageLink')
                        if navigation_link:
                            full_url = f'https://www.compass.com/{navigation_link}'
                            first_links.append(full_url)
                
                all_links = first_links.copy()
                print(f"Первая страница: найдено {len(first_links)} ссылок")
                
                # Пытаемся определить общее количество страниц
                total_items = 0
                if 'lolResults' in data:
                    total_items = data['lolResults'].get('totalItems', 0)
                    print(f"DEBUG: lolResults.totalItems = {total_items}")
                elif 'data' in data and isinstance(data['data'], dict):
                    total_items = data['data'].get('totalItems', 0)
                    print(f"DEBUG: data.totalItems = {total_items}")
                
                # Также проверяем другие возможные поля
                if total_items == 0:
                    if 'total' in data:
                        total_items = data.get('total', 0)
                        print(f"DEBUG: total = {total_items}")
                    if 'totalCount' in data:
                        total_items = data.get('totalCount', 0)
                        print(f"DEBUG: totalCount = {total_items}")
                
                if total_items > 0:
                    total_pages = (total_items + num_per_page - 1) // num_per_page
                    print(f"Всего объявлений: {total_items}, страниц: {total_pages} (по {num_per_page} на страницу)")
                else:
                    # Если не знаем общее количество, будем запрашивать до пустого ответа
                    total_pages = None
                    print("Не удалось определить общее количество страниц, будем запрашивать до пустого ответа")
                    print(f"DEBUG: Структура ответа: {list(data.keys())}")
            except Exception as e:
                print(f"Ошибка при получении начальных данных: {e}")
                import traceback
                traceback.print_exc()
                total_pages = None
                all_links = []
        
        # Если получили меньше результатов, чем запрашивали, значит это последняя страница
        if len(first_links) < num_per_page:
            print("Получено меньше результатов на первой странице, завершаем.")
            return all_links
        
        # Если знаем общее количество страниц, создаем задачи для всех с семафором
        if total_pages:
            print(f"\nЗапускаем параллельные запросы для {total_pages - 1} страниц...")
            
            # Создаем задачи с семафором
            async def fetch_with_semaphore(page_num):
                async with semaphore:
                    return await fetch_page_links(
                        client, api_url, page_num, page_num * num_per_page, num_per_page,
                        search_result_id, location_ids, viewport_ne, viewport_sw, post_headers
                    )
            
            semaphore_tasks = [
                fetch_with_semaphore(page_num) 
                for page_num in range(1, total_pages)
            ]
            
            # Выполняем все задачи
            results = await asyncio.gather(*semaphore_tasks, return_exceptions=True)
            
            # Обрабатываем результаты
            successful_pages = 0
            failed_pages = 0
            for result in results:
                if isinstance(result, Exception):
                    print(f"Ошибка на странице: {result}")
                    failed_pages += 1
                    continue
                
                page_num, links = result
                if links:
                    all_links.extend(links)
                    successful_pages += 1
                    if (page_num + 1) % 50 == 0 or page_num == 0:
                        print(f"Обработано страниц: {page_num + 1}/{total_pages - 1}, собрано ссылок: {len(all_links)}")
                else:
                    print(f"Предупреждение: страница {page_num + 1} вернула пустой результат")
            
            print(f"\nОбработано успешно: {successful_pages} страниц, ошибок: {failed_pages}")
            print(f"Ожидалось страниц: {total_pages - 1}, обработано: {successful_pages + failed_pages}")
            print(f"Итого собрано ссылок: {len(all_links)}")
            
            # Проверяем, что мы получили все страницы
            if successful_pages + failed_pages < total_pages - 1:
                print(f"ВНИМАНИЕ: Обработано меньше страниц, чем ожидалось!")
                print(f"Ожидалось: {total_pages - 1}, обработано: {successful_pages + failed_pages}")
            
            return all_links
        else:
            # Если не знаем, делаем запросы пакетами
            # Сначала делаем несколько параллельных запросов
            batch_size = concurrency * 2
            max_pages = 1000  # Максимальное количество страниц на случай бесконечного цикла
            
            while page < max_pages:
                batch_tasks = []
                for _ in range(batch_size):
                    task = fetch_page_links(
                        client, api_url, page, start, num_per_page,
                        search_result_id, location_ids, viewport_ne, viewport_sw, post_headers
                    )
                    batch_tasks.append(task)
                    page += 1
                    start += num_per_page
                
                # Выполняем пакет запросов
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Обрабатываем результаты
                batch_links = []
                empty_pages = 0
                for result in results:
                    if isinstance(result, Exception):
                        print(f"Ошибка в пакете: {result}")
                        empty_pages += 1
                        continue
                    
                    page_num, links = result
                    if links:
                        batch_links.extend(links)
                        all_links.extend(links)
                        print(f"Страница {page_num + 1}: найдено {len(links)} ссылок. Всего: {len(all_links)}")
                    else:
                        empty_pages += 1
                
                # Если все страницы в пакете пустые, завершаем
                if empty_pages == len(results):
                    print("Получены пустые ответы, завершаем.")
                    break
                
                # Если получили меньше результатов, чем ожидали, возможно это конец
                if len(batch_links) < batch_size * num_per_page * 0.5:  # Если получили меньше 50% ожидаемого
                    print("Получено значительно меньше результатов, завершаем.")
                    break
            
            print(f"\nИтого собрано ссылок: {len(all_links)}")
            return all_links


# Синхронная обертка для обратной совместимости
def get_all_listing_links(location_url: str, concurrency: int = 10):
    """
    Получает все ссылки на объявления (navigationPageLink) со всех страниц (асинхронная версия)
    
    Args:
        location_url: URL страницы, например 'https://www.compass.com/homes-for-sale/arizona/'
        concurrency: Количество одновременных запросов (по умолчанию 10)
    
    Returns:
        list: Массив всех ссылок на объявления
    """
    return asyncio.run(get_all_listing_links_async(location_url, concurrency))

# ========== Парсинг объявлений из HTML ==========

def extract_json_from_text(text: str, start_marker: str) -> str | None:
    """
    Извлекает JSON объект из текста, начиная с маркера.
    Использует подсчет скобок для правильного определения конца JSON.
    """
    # Находим позицию маркера
    pos = text.find(start_marker)
    if pos == -1:
        return None
    
    # Находим начало JSON объекта (первая '{' после '=')
    pos = text.find('=', pos) + 1
    # Пропускаем пробелы
    while pos < len(text) and text[pos] in ' \t\n\r':
        pos += 1
    
    if pos >= len(text) or text[pos] != '{':
        return None
    
    # Начинаем с первой '{'
    start_pos = pos
    bracket_count = 0
    in_string = False
    escape_next = False
    
    # Проходим по тексту и считаем скобки
    for i in range(start_pos, len(text)):
        char = text[i]
        
        if escape_next:
            escape_next = False
            continue
        
        if char == '\\':
            escape_next = True
            continue
        
        if char == '"' and not escape_next:
            in_string = not in_string
            continue
        
        if not in_string:
            if char == '{':
                bracket_count += 1
            elif char == '}':
                bracket_count -= 1
                if bracket_count == 0:
                    # Нашли закрывающую скобку
                    json_str = text[start_pos:i+1]
                    return json_str
    
    return None


def extract_initial_data(html: str) -> dict | None:
    """
    Извлекает данные из window.__INITIAL_DATA__ в HTML
    Использует алгоритм подсчета скобок для правильного извлечения больших JSON объектов
    """
    try:
        # Пробуем разные маркеры
        markers = [
            '__INITIAL_DATA__',
            'window.__INITIAL_DATA__',
        ]
        
        for marker in markers:
            json_str = extract_json_from_text(html, marker)
            if json_str:
                try:
                    data = json.loads(json_str)
                    return data
                except json.JSONDecodeError as e:
                    # Пробуем следующий маркер
                    continue
        
        return None
        
    except Exception as e:
        print(f"Ошибка при извлечении __INITIAL_DATA__: {e}")
        return None


def extract_listing_data(initial_data: dict, url: str = '') -> dict | None:
    """
    Извлекает нужные поля из window.__INITIAL_DATA__
    """
    try:
        # Находим данные листинга
        listing = None
        if 'props' in initial_data and 'listingRelation' in initial_data['props']:
            listing = initial_data['props']['listingRelation'].get('listing')
        
        if not listing:
            return None
        
        # Исправляем URL (убираем двойной слэш)
        fixed_url = url.replace('https://www.compass.com//', 'https://www.compass.com/')
        
        # Извлекаем нужные поля
        result = {
            'url': fixed_url,
            'price': None,
            'square_feet': None,
            'listing_type': None,  # аренда/продажа
            'description': None,
            'listing_status': None,
            'listing_details': None,
            'photos': [],
            'brochure_pdf': None,
            'mls': None,
            'agents': []
        }
        
        # Цена
        price_value = 0
        if 'price' in listing:
            price_data = listing['price']
            price_value = price_data.get('lastKnown', 0)
            per_square_foot = price_data.get('perSquareFoot', 0)
            
            result['price'] = {
                'formatted': price_data.get('formatted', ''),
                'value': price_value,
                'per_square_foot': per_square_foot
            }
        else:
            result['price'] = {
                'formatted': '',
                'value': 0,
                'per_square_foot': 0
            }
        
        # Площадь - проверяем несколько мест
        square_feet = 0
        if 'size' in listing:
            size_data = listing['size']
            square_feet = size_data.get('squareFeet', 0)
        
        # Если не нашли в size, проверяем в detailedInfo
        if not square_feet and 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            # Ищем в listingDetails
            if 'listingDetails' in detailed_info:
                for detail_group in detailed_info['listingDetails']:
                    if 'subCategories' in detail_group:
                        for subcat in detail_group['subCategories']:
                            if 'fields' in subcat:
                                for field in subcat['fields']:
                                    key = field.get('key', '').lower()
                                    if 'sqft' in key or 'square' in key or 'sq ft' in key:
                                        values = field.get('values', [])
                                        if values:
                                            try:
                                                # Пытаемся извлечь число из строки
                                                value_str = str(values[0]).replace(',', '').replace(' ', '')
                                                square_feet = float(value_str)
                                                break
                                            except (ValueError, TypeError):
                                                pass
        
        result['square_feet'] = int(square_feet) if square_feet else None
        
        # Вычисляем per_square_foot, если он не был указан, но есть цена и площадь
        if (result['price'] and result['price'].get('value') and 
            result['square_feet'] and result['square_feet'] > 0):
            if not result['price'].get('per_square_foot') or result['price']['per_square_foot'] == 0:
                result['price']['per_square_foot'] = round(result['price']['value'] / result['square_feet'], 2)
        
        # Тип (аренда/продажа)
        listing_type = listing.get('listingType', 0)
        result['listing_type'] = 'rent' if listing_type == 1 else 'sale'
        
        # Описание (может быть в разных местах)
        # НЕ используем contactFormMessage, так как это не описание
        description = None
        if 'description' in listing and listing['description']:
            description = listing['description']
            # Проверяем, что это не шаблонное сообщение
            if description.startswith('I would like more information about'):
                description = None
        
        if not description and 'dealInfo' in listing:
            deal_info = listing['dealInfo']
            if 'description' in deal_info and deal_info['description']:
                description = deal_info['description']
                if description.startswith('I would like more information about'):
                    description = None
        
        # Также проверяем в detailedInfo
        if not description and 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'description' in detailed_info and detailed_info['description']:
                description = detailed_info['description']
        
        result['description'] = description
        
        # Listing status
        result['listing_status'] = listing.get('localizedStatus', '')
        if not result['listing_status'] and 'status' in listing:
            # Если нет localizedStatus, используем числовой статус
            status_map = {
                0: 'Listed (Active)',
                9: 'Active',
                12: 'Active',
                14: 'Coming Soon',
                10: 'Sold',
                8: 'Contract Signed',
            }
            result['listing_status'] = status_map.get(listing['status'], f"Status {listing['status']}")
        
        # Listing details (таблица) - это detailedInfo.listingDetails
        if 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'listingDetails' in detailed_info:
                result['listing_details'] = detailed_info['listingDetails']
            # Также сохраняем keyDetails если есть
            if 'keyDetails' in detailed_info and not result['listing_details']:
                result['listing_details'] = detailed_info['keyDetails']
        
        # Фото - извлекаем все media с category 0 (обычно это фото)
        if 'media' in listing:
            photos = []
            for media in listing['media']:
                # category 0 обычно означает фото
                if media.get('category', 0) == 0 and 'originalUrl' in media:
                    photo_url = media['originalUrl']
                    # Преобразуем относительные URL в полные
                    if photo_url.startswith('//'):
                        photo_url = 'https:' + photo_url
                    elif photo_url.startswith('/'):
                        photo_url = 'https://www.compass.com' + photo_url
                    
                    photos.append({
                        'url': photo_url,
                        'thumbnail': media.get('thumbnailUrl', ''),
                        'width': media.get('width', 0),
                        'height': media.get('height', 0)
                    })
            result['photos'] = photos
        
        # Brochure PDF - ищем только реальные PDF файлы
        if 'media' in listing:
            for media in listing['media']:
                original_url = media.get('originalUrl', '')
                # Проверяем, что это действительно PDF файл
                if original_url and original_url.lower().endswith('.pdf'):
                    brochure_url = original_url
                    if brochure_url.startswith('//'):
                        brochure_url = 'https:' + brochure_url
                    elif brochure_url.startswith('/'):
                        brochure_url = 'https://www.compass.com' + brochure_url
                    result['brochure_pdf'] = brochure_url
                    break
        
        # MLS - информация из databaseSource
        mls_info = {}
        if 'databaseSource' in listing:
            mls_data = listing['databaseSource']
            mls_info = {
                'source_name': mls_data.get('externalSourceName', ''),
                'source_display_name': mls_data.get('sourceDisplayName', ''),
                'contributing_datasets': mls_data.get('contributingDatasetList', [])
            }
        
        # Также добавляем mlsStatus если есть
        if 'mlsStatus' in listing:
            mls_info['status'] = listing.get('mlsStatus', '')
        
        # Проверяем isOffMLS
        if 'isOffMLS' in listing:
            mls_info['is_off_mls'] = listing.get('isOffMLS', False)
        
        # Добавляем информацию из transaction history если есть
        if 'transactionHistory' in listing and listing['transactionHistory']:
            latest_transaction = listing['transactionHistory'][0]
            if 'source' in latest_transaction:
                source = latest_transaction['source']
                if 'externalSourceId' in source:
                    mls_info['mls_id'] = source.get('externalSourceId', '')
                if 'sourceDisplayName' in source and not mls_info.get('source_display_name'):
                    mls_info['source_display_name'] = source.get('sourceDisplayName', '')
        
        result['mls'] = mls_info if mls_info else None
        
        # Агенты - из fullContacts
        if 'fullContacts' in listing:
            agents = []
            for contact in listing['fullContacts']:
                agent_info = {
                    'name': contact.get('contactName', ''),
                    'email': contact.get('email', ''),
                    'phone': contact.get('phone', ''),
                    'contact_type': contact.get('contactType', ''),
                    'license': contact.get('licenseNum', ''),
                    'profile_url': contact.get('websiteURL', ''),
                    'company': contact.get('company', '')
                }
                # Преобразуем относительные URL в полные
                if agent_info['profile_url'].startswith('/'):
                    agent_info['profile_url'] = 'https://www.compass.com' + agent_info['profile_url']
                agents.append(agent_info)
            result['agents'] = agents
        
        return result
        
    except Exception as e:
        print(f"Ошибка при извлечении данных листинга: {e}")
        import traceback
        traceback.print_exc()
        return None


async def parse_listing(client: httpx.AsyncClient, url: str, semaphore: asyncio.Semaphore) -> dict | None:
    """
    Парсит одно объявление по URL
    """
    async with semaphore:
        try:
            headers = {
                'User-Agent': UserAgent().random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            
            response = await client.get(url, headers=headers, timeout=30.0, follow_redirects=True)
            response.raise_for_status()
            
            html = response.text
            initial_data = extract_initial_data(html)
            
            if not initial_data:
                print(f"⚠ Не удалось извлечь __INITIAL_DATA__ из {url}")
                return None
            
            listing_data = extract_listing_data(initial_data, url)
            if listing_data:
                return listing_data
            else:
                print(f"⚠ Не удалось извлечь данные листинга из {url}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка при парсинге {url}: {e}")
            return None


async def parse_listings_async(listing_urls: list[str], concurrency: int = 10, limit: int = None) -> list[dict]:
    """
    Асинхронно парсит список объявлений
    
    Args:
        listing_urls: Список URL объявлений
        concurrency: Количество одновременных запросов
        limit: Ограничение количества объявлений для обработки (для теста)
    
    Returns:
        list: Список словарей с данными объявлений
    """
    if limit:
        listing_urls = listing_urls[:limit]
    
    print(f"\nНачинаем парсинг {len(listing_urls)} объявлений...")
    
    semaphore = asyncio.Semaphore(concurrency)
    results = []
    
    async with httpx.AsyncClient() as client:
        tasks = [parse_listing(client, url, semaphore) for url in listing_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Фильтруем успешные результаты
    parsed_listings = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"❌ Ошибка при обработке объявления {i+1}: {result}")
        elif result:
            parsed_listings.append(result)
            print(f"✓ Обработано объявление {len(parsed_listings)}/{len(listing_urls)}: {result.get('url', '')}")
    
    print(f"\nУспешно обработано: {len(parsed_listings)} из {len(listing_urls)}")
    return parsed_listings


def parse_listings(listing_urls: list[str], concurrency: int = 10, limit: int = None) -> list[dict]:
    """
    Синхронная обертка для парсинга объявлений
    """
    return asyncio.run(parse_listings_async(listing_urls, concurrency, limit))


# Пример использования
if __name__ == "__main__":
    # Код работает с любым регионом Compass, например:
    # - 'https://www.compass.com/homes-for-sale/arizona/'
    # - 'https://www.compass.com/homes-for-sale/california/'
    # - 'https://www.compass.com/homes-for-sale/new-york/'
    # - Или с URL содержащим mapview: 'https://www.compass.com/homes-for-sale/arizona/mapview=37.0,-109.0,31.0,-114.0/'
    
    location_url = 'https://www.compass.com/homes-for-sale/hawaii/'
    
    # Шаг 1: Собираем ссылки на объявления
    print("=" * 60)
    print("ШАГ 1: Сбор ссылок на объявления")
    print("=" * 60)
    links = get_all_listing_links(location_url, concurrency=10)
    
    print(f"\nВсего собрано ссылок: {len(links)}")
    
    # Шаг 2: Парсим объявления (для теста ограничиваем до 10)
    print("\n" + "=" * 60)
    print("ШАГ 2: Парсинг объявлений")
    print("=" * 60)
    TEST_LIMIT = 10
    listings_data = parse_listings(links, concurrency=10, limit=TEST_LIMIT)
    
    # Шаг 3: Сохраняем в JSON
    print("\n" + "=" * 60)
    print("ШАГ 3: Сохранение результатов")
    print("=" * 60)
    output_file = 'listings_data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(listings_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ Данные сохранены в файл '{output_file}'")
    print(f"✓ Обработано объявлений: {len(listings_data)}")
    
    # Показываем пример первого объявления
    if listings_data:
        print(f"\nПример данных первого объявления:")
        print(json.dumps(listings_data[0], ensure_ascii=False, indent=2)[:500] + "...")