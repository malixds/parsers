import requests
from fake_useragent import UserAgent
import xml.etree.ElementTree as ET
import json
import uuid
import asyncio
import httpx
import re
from schema import DbDTO, AgentData
from datetime import date, datetime
from typing import Optional

# Константы для URL
BASE_URL = 'https://www.compass.com'
SITEMAPS_BASE_PATH = f'{BASE_URL}/sitemaps'

sitemaps = [
    f'{SITEMAPS_BASE_PATH}/for-sale/index.xml',
    f'{SITEMAPS_BASE_PATH}/for-rent/index.xml'
]

# Парсинг всех sitemap index
namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}


def get_new_user_agent() -> str:
    """Генерирует новый случайный User-Agent"""
    return UserAgent().random


def update_user_agent_in_headers(headers: dict) -> dict:
    """Обновляет User-Agent в заголовках"""
    headers = headers.copy()
    headers['User-Agent'] = get_new_user_agent()
    return headers


def process_sitemaps_generator(headers: dict = None):
    """
    Генератор, который постепенно обрабатывает все sitemap файлы и возвращает URL по мере их получения.
    Это позволяет обрабатывать данные потоково, не накапливая все URL в памяти.
    
    Yields:
        str: URL страницы из sitemap
    """
    if headers is None:
        headers = {
            'User-Agent': UserAgent().random,
        }
    
    total_urls_count = 0
    
    for sitemap_url in sitemaps:
        print(f"\n{'='*60}")
        print(f"Обработка sitemap: {sitemap_url}")
        print(f"{'='*60}")
        
        # Пытаемся получить sitemap с retry при 403
        max_retries = 3
        response = None
        for attempt in range(max_retries):
            response = requests.get(sitemap_url, headers=headers)
            if response.status_code == 200:
                break
            elif response.status_code == 403:
                if attempt < max_retries - 1:
                    print(f"  403 ошибка, попытка {attempt + 1}/{max_retries}: заменяем User-Agent...")
                    headers = update_user_agent_in_headers(headers)
                else:
                    print(f"  403 ошибка после {max_retries} попыток")
            else:
                break
        
        if response and response.status_code == 200:
            # Парсим XML
            root = ET.fromstring(response.content)
            
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
            
            # Парсим все sitemap файлы для получения URL страниц
            if sitemap_links:
                for idx, sitemap_info in enumerate(sitemap_links, 1):
                    print(f"\nПарсим sitemap {idx}/{len(sitemap_links)}: {sitemap_info['url']}")
                    # Пытаемся получить sitemap файл с retry при 403
                    max_retries = 3
                    sitemap_response = None
                    for attempt in range(max_retries):
                        sitemap_response = requests.get(sitemap_info['url'], headers=headers)
                        if sitemap_response.status_code == 200:
                            break
                        elif sitemap_response.status_code == 403:
                            if attempt < max_retries - 1:
                                print(f"  403 ошибка, попытка {attempt + 1}/{max_retries}: заменяем User-Agent...")
                                headers = update_user_agent_in_headers(headers)
                            else:
                                print(f"  403 ошибка после {max_retries} попыток")
                        else:
                            break
                    
                    if sitemap_response and sitemap_response.status_code == 200:
                        sitemap_root = ET.fromstring(sitemap_response.content)
                        urls_count = 0
                        for url_elem in sitemap_root.findall('ns:url', namespace):
                            loc = url_elem.find('ns:loc', namespace)
                            if loc is not None:
                                # Возвращаем URL по мере получения
                                yield loc.text
                                urls_count += 1
                                total_urls_count += 1
                        
                        print(f"  Найдено URL страниц: {urls_count}")
                    else:
                        print(f"  Ошибка при получении sitemap {sitemap_info['url']}: {sitemap_response.status_code}")
        else:
            print(f"Ошибка при получении sitemap {sitemap_url}: {response.status_code}")
    
    print(f"\nВсего обработано URL из всех sitemap файлов: {total_urls_count}")

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
        # Пытаемся выполнить запрос с retry при 403
        max_retries = 3
        response = None
        for attempt in range(max_retries):
            response = await client.post(
                api_url,
                params=params,
                json=json_data,
                headers=post_headers,
                timeout=30.0
            )
            if response.status_code == 200:
                break
            elif response.status_code == 403:
                if attempt < max_retries - 1:
                    print(f"  403 ошибка, попытка {attempt + 1}/{max_retries}: заменяем User-Agent...")
                    post_headers = update_user_agent_in_headers(post_headers)
                else:
                    print(f"  403 ошибка после {max_retries} попыток")
                    response.raise_for_status()
                    break
            else:
                response.raise_for_status()
                break
        
        if not response:
            raise Exception("Не удалось получить ответ от сервера")
        
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
                    full_url = f'{BASE_URL}{navigation_link}'
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
        'Origin': BASE_URL,
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

                # Пытаемся выполнить запрос с retry при 403
                max_retries = 3
                response = None
                for attempt in range(max_retries):
                    response = await client.post(api_url, params=params, json=json_data, headers=post_headers, timeout=30.0)
                    if response.status_code == 200:
                        break
                    elif response.status_code == 403:
                        if attempt < max_retries - 1:
                            print(f"  403 ошибка, попытка {attempt + 1}/{max_retries}: заменяем User-Agent...")
                            post_headers = update_user_agent_in_headers(post_headers)
                        else:
                            print(f"  403 ошибка после {max_retries} попыток")
                            response.raise_for_status()
                            break
                    else:
                        response.raise_for_status()
                        break
                
                if not response:
                    raise Exception("Не удалось получить ответ от сервера")
                
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
                            # Убираем начальный слэш, если есть, чтобы избежать двойного слэша
                            nav_path = navigation_link.lstrip('/')
                            full_url = f'{BASE_URL}/{nav_path}'
                            first_links.append(full_url)
                
                all_links = first_links.copy()
                print(f"Первая страница: найдено {len(first_links)} ссылок")
                
                # Пытаемся определить общее количество страниц
                total_items = 0
                if 'lolResults' in data:
                    total_items = data['lolResults'].get('totalItems', 0)
                elif 'data' in data and isinstance(data['data'], dict):
                    total_items = data['data'].get('totalItems', 0)
                
                # Также проверяем другие возможные поля
                if total_items == 0:
                    if 'total' in data:
                        total_items = data.get('total', 0)
                    if 'totalCount' in data:
                        total_items = data.get('totalCount', 0)
                
                if total_items > 0:
                    total_pages = (total_items + num_per_page - 1) // num_per_page
                    print(f"Всего объявлений: {total_items}, страниц: {total_pages} (по {num_per_page} на страницу)")
                else:
                    # Если не знаем общее количество, будем запрашивать до пустого ответа
                    total_pages = None
                    print("Не удалось определить общее количество страниц, будем запрашивать до пустого ответа")
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
            page = 1
            start = num_per_page
            
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


def extract_listing_data(initial_data: dict, url: str = '') -> DbDTO | None:
    """
    Извлекает нужные поля из window.__INITIAL_DATA__ и возвращает DbDTO объект
    """
    try:
        # Находим данные листинга
        listing = None
        if 'props' in initial_data and 'listingRelation' in initial_data['props']:
            listing = initial_data['props']['listingRelation'].get('listing')
        
        if not listing:
            return None
        
        # Исправляем URL (убираем двойной слэш)
        fixed_url = url.replace(f'{BASE_URL}//', f'{BASE_URL}/')
        
        # Извлекаем базовые идентификаторы
        listing_id = listing.get('listingIdSHA', '') or listing.get('compassPropertyId', '') or listing.get('feedListingId', '')
        if not listing_id:
            # Используем URL как fallback для ID
            listing_id = fixed_url.split('/')[-2] if '/' in fixed_url else fixed_url
        listing_id = str(listing_id) if listing_id else 'unknown'
        
        # Адрес и локация
        location = listing.get('location', {})
        address = location.get('prettyAddress', '')
        if not address:
            # Формируем адрес из компонентов
            parts = []
            if location.get('streetNumber'):
                parts.append(location['streetNumber'])
            if location.get('street'):
                parts.append(location['street'])
            if location.get('streetType'):
                parts.append(location['streetType'])
            if location.get('unitNumber'):
                parts.append(f"{location.get('unitType', 'Unit')} {location['unitNumber']}")
            if parts:
                address = ', '.join(parts)
            if location.get('city'):
                address += f", {location['city']}"
            if location.get('state'):
                address += f" {location['state']}"
            if location.get('zipCode'):
                address += f" {location['zipCode']}"
        
        # Координаты
        coordinates = None
        if location.get('latitude') and location.get('longitude'):
            coordinates = f"{location['latitude']},{location['longitude']}"
        
        # Тип объявления
        listing_type_num = listing.get('listingType', 0)
        listing_type_str = 'For Lease' if listing_type_num == 1 else 'For Sale'
        
        # Статус
        listing_status = listing.get('localizedStatus', '')
        if not listing_status and 'status' in listing:
            status_map = {
                0: 'Active',
                9: 'Active',
                12: 'Active',
                14: 'Coming Soon',
                10: 'Sold',
                8: 'Contract Signed',
            }
            listing_status = status_map.get(listing['status'], f"Status {listing['status']}")
        
        # Цена
        sale_price = None
        lease_price = None
        if 'price' in listing:
            price_data = listing['price']
            price_formatted = price_data.get('formatted', '')
            if listing_type_num == 1:
                lease_price = price_formatted
            else:
                sale_price = price_formatted
        
        # Площадь
        square_feet = 0
        size_str = None
        if 'size' in listing:
            size_data = listing['size']
            square_feet = size_data.get('squareFeet', 0)
            if square_feet:
                size_str = f"{square_feet:,} sqft"
        
        # Если не нашли в size, проверяем в detailedInfo
        if not square_feet and 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
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
                                                value_str = str(values[0]).replace(',', '').replace(' ', '')
                                                square_feet = float(value_str)
                                                size_str = f"{int(square_feet):,} sqft"
                                                break
                                            except (ValueError, TypeError):
                                                pass
        
        # Lot size
        lot_size_str = None
        if 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'keyDetails' in detailed_info:
                for key_detail in detailed_info['keyDetails']:
                    key = key_detail.get('key', '').lower()
                    if 'lot size' in key or 'lot' in key:
                        value = key_detail.get('value', '')
                        if value and value != '-':
                            lot_size_str = value
                            break
            
            # Если не нашли в keyDetails, проверяем в listingDetails
            if not lot_size_str and 'listingDetails' in detailed_info:
                for detail_group in detailed_info['listingDetails']:
                    if 'subCategories' in detail_group:
                        for subcat in detail_group['subCategories']:
                            if 'fields' in subcat:
                                for field in subcat['fields']:
                                    key = field.get('key', '').lower()
                                    if 'lot size' in key or 'lot' in key:
                                        values = field.get('values', [])
                                        if values:
                                            lot_size_str = str(values[0])
                                            break
                                if lot_size_str:
                                    break
                            if lot_size_str:
                                break
                    if lot_size_str:
                        break
        
        # Описание
        description = None
        if 'description' in listing and listing['description']:
            description = listing['description']
            if description.startswith('I would like more information about'):
                description = None
        
        if not description and 'dealInfo' in listing:
            deal_info = listing['dealInfo']
            if 'description' in deal_info and deal_info['description']:
                description = deal_info['description']
                if description.startswith('I would like more information about'):
                    description = None
        
        if not description and 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'description' in detailed_info and detailed_info['description']:
                description = detailed_info['description']
        
        # Listing details - преобразуем список в словарь, если нужно
        listing_details_dict = None
        if 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'listingDetails' in detailed_info:
                details_data = detailed_info['listingDetails']
                # Если это список, преобразуем в словарь
                if isinstance(details_data, list):
                    # Создаем словарь, используя индекс или имя как ключ
                    listing_details_dict = {}
                    for idx, item in enumerate(details_data):
                        if isinstance(item, dict):
                            # Используем 'name' как ключ, если есть, иначе индекс
                            key = item.get('name', f'item_{idx}')
                            listing_details_dict[key] = item
                        else:
                            listing_details_dict[f'item_{idx}'] = item
                elif isinstance(details_data, dict):
                    listing_details_dict = details_data
            elif 'keyDetails' in detailed_info:
                key_details_data = detailed_info['keyDetails']
                # Если это список, преобразуем в словарь
                if isinstance(key_details_data, list):
                    listing_details_dict = {}
                    for idx, item in enumerate(key_details_data):
                        if isinstance(item, dict):
                            key = item.get('name', f'item_{idx}')
                            listing_details_dict[key] = item
                        else:
                            listing_details_dict[f'item_{idx}'] = item
                elif isinstance(key_details_data, dict):
                    listing_details_dict = key_details_data
        
        # Фото - только URL строки
        photos_list = []
        if 'media' in listing:
            for media in listing['media']:
                if media.get('category', 0) == 0 and 'originalUrl' in media:
                    photo_url = media['originalUrl']
                    if photo_url.startswith('//'):
                        photo_url = 'https:' + photo_url
                    elif photo_url.startswith('/'):
                        photo_url = BASE_URL + photo_url
                    photos_list.append(photo_url)
        
        # Brochure PDF
        brochure_pdf = None
        if 'media' in listing:
            for media in listing['media']:
                original_url = media.get('originalUrl', '')
                if original_url and original_url.lower().endswith('.pdf'):
                    brochure_pdf = original_url
                    if brochure_pdf.startswith('//'):
                        brochure_pdf = 'https:' + brochure_pdf
                    elif brochure_pdf.startswith('/'):
                        brochure_pdf = BASE_URL + brochure_pdf
                    break
        
        # MLS номер
        mls_number = None
        if 'transactionHistory' in listing and listing['transactionHistory']:
            latest_transaction = listing['transactionHistory'][0]
            if 'source' in latest_transaction:
                source = latest_transaction['source']
                if 'externalSourceId' in source:
                    mls_number = source.get('externalSourceId', '')
        
        # Агенты - преобразуем в AgentData
        agents_list = []
        if 'fullContacts' in listing:
            for contact in listing['fullContacts']:
                profile_url = contact.get('websiteURL', '')
                if profile_url.startswith('/'):
                    profile_url = BASE_URL + profile_url
                
                # Обрабатываем email - только если он валидный
                email = contact.get('email')
                if not email or email.strip() == '':
                    email = None
                
                # Обрабатываем photo_url
                photo_url = contact.get('profileImageURL')
                if photo_url:
                    if photo_url.startswith('//'):
                        photo_url = 'https:' + photo_url
                    elif photo_url.startswith('/'):
                        photo_url = BASE_URL + photo_url
                
                agent = AgentData(
                    name=contact.get('contactName'),
                    license=contact.get('licenseNum'),
                    phone_primary=contact.get('phone'),
                    email=email,
                    photo_url=photo_url,
                    office_name=contact.get('company'),
                )
                agents_list.append(agent)
        
        # Property type
        property_type = None
        if 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'propertyType' in detailed_info:
                prop_type = detailed_info['propertyType']
                if 'masterType' in prop_type and 'GLOBAL' in prop_type['masterType']:
                    types = prop_type['masterType']['GLOBAL']
                    if types:
                        property_type = types[0]
        
        # Year built
        year_built = None
        if 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'keyDetails' in detailed_info:
                for key_detail in detailed_info['keyDetails']:
                    if key_detail.get('key') == 'Year Built':
                        value = key_detail.get('value', '')
                        if value and value != '-':
                            try:
                                year_built = int(value)
                            except (ValueError, TypeError):
                                pass
        
        # Даты
        listing_date_obj = None
        last_updated_obj = None
        days_on_market = None
        
        if 'date' in listing:
            date_data = listing['date']
            # updated может быть timestamp в миллисекундах
            if 'updated' in date_data:
                updated_ts = date_data['updated']
                if updated_ts:
                    try:
                        # Конвертируем из миллисекунд в секунды
                        dt = datetime.fromtimestamp(updated_ts / 1000)
                        last_updated_obj = dt.date()
                    except (ValueError, TypeError, OSError):
                        pass
            # listed может быть timestamp в миллисекундах
            if 'listed' in date_data:
                listed_ts = date_data['listed']
                if listed_ts:
                    try:
                        # Конвертируем из миллисекунд в секунды
                        dt = datetime.fromtimestamp(listed_ts / 1000)
                        listing_date_obj = dt.date()
                    except (ValueError, TypeError, OSError):
                        pass
        
        # Days on Market
        if 'detailedInfo' in listing:
            detailed_info = listing['detailedInfo']
            if 'keyDetails' in detailed_info:
                for key_detail in detailed_info['keyDetails']:
                    if 'Days on Market' in key_detail.get('key', ''):
                        days_on_market = key_detail.get('value', '')
        
        # Если days_on_market равно "-", проверяем daysOnMarket в listing
        if days_on_market == "-":
            days_on_market_num = listing.get('daysOnMarket')
            if days_on_market_num is not None:
                # Если есть числовое значение, используем его
                days_on_market = str(days_on_market_num)
            else:
                # Если нет числового значения, ставим None
                days_on_market = None
        
        # Создаем DbDTO объект
        dto = DbDTO(
            source_name="compass",
            listing_id=listing_id,
            listing_link=fixed_url,
            listing_type=listing_type_str,
            listing_status=listing_status,
            address=address if address else "Address not found",
            coordinates=coordinates,
            building_number=location.get('streetNumber'),
            street_name=location.get('street'),
            unit_number=location.get('unitNumber'),
            city=location.get('city'),
            state=location.get('state'),
            zipcode=location.get('zipCode'),
            sale_price=sale_price,
            lease_price=lease_price,
            size=size_str,
            lot_size=lot_size_str,
            property_type=property_type,
            property_description=description,
            listing_details=listing_details_dict,
            photos=photos_list if photos_list else None,
            brochure_pdf=brochure_pdf,
            mls_number=mls_number,
            agents=agents_list if agents_list else None,
            year_built=year_built,
            listing_date=listing_date_obj,
            last_updated=last_updated_obj,
            days_on_market=days_on_market,
        )
        
        return dto
        
    except Exception as e:
        print(f"Ошибка при извлечении данных листинга: {e}")
        import traceback
        traceback.print_exc()
        return None


async def parse_listing(client: httpx.AsyncClient, url: str, semaphore: asyncio.Semaphore) -> DbDTO | None:
    """
    Парсит одно объявление по URL и возвращает DbDTO объект
    """
    async with semaphore:
        try:
            headers = {
                'User-Agent': UserAgent().random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            
            # Пытаемся выполнить запрос с retry при 403
            max_retries = 3
            response = None
            for attempt in range(max_retries):
                response = await client.get(url, headers=headers, timeout=30.0, follow_redirects=True)
                if response.status_code == 200:
                    break
                elif response.status_code == 403:
                    if attempt < max_retries - 1:
                        headers = update_user_agent_in_headers(headers)
                    else:
                        response.raise_for_status()
                        break
                else:
                    response.raise_for_status()
                    break
            
            if not response:
                raise Exception("Не удалось получить ответ от сервера")
            
            response.raise_for_status()
            
            html = response.text
            initial_data = extract_initial_data(html)
            
            if not initial_data:
                print(f"⚠ Не удалось извлечь __INITIAL_DATA__ из {url}")
                return None
            
            dto = extract_listing_data(initial_data, url)
            if dto:
                return dto
            else:
                print(f"⚠ Не удалось извлечь данные листинга из {url}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка при парсинге {url}: {e}")
            return None


async def parse_listings_async(listing_urls: list[str], concurrency: int = 10, limit: int = None) -> list[DbDTO]:
    """
    Асинхронно парсит список объявлений
    
    Args:
        listing_urls: Список URL объявлений
        concurrency: Количество одновременных запросов
        limit: Ограничение количества объявлений для обработки (опционально)
    
    Returns:
        list: Список DbDTO объектов с данными объявлений
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
            print(f"✓ Обработано объявление {len(parsed_listings)}/{len(listing_urls)}: {result.listing_link}")
    
    print(f"\nУспешно обработано: {len(parsed_listings)} из {len(listing_urls)}")
    return parsed_listings


def parse_listings(listing_urls: list[str], concurrency: int = 10, limit: int = None) -> list[DbDTO]:
    """
    Синхронная обертка для парсинга объявлений
    """
    return asyncio.run(parse_listings_async(listing_urls, concurrency, limit))


# Пример использования
if __name__ == "__main__":
    # Шаг 0: Получаем location URLs из sitemap
    print("=" * 60)
    print("ШАГ 0: Получение location URLs из sitemap")
    print("=" * 60)
    
    # Шаг 1 и 2: Обрабатываем каждый location URL постепенно
    print("\n" + "=" * 60)
    print("ШАГ 1-2: Сбор ссылок и парсинг объявлений")
    print("=" * 60)
    
    all_listings_data = []
    total_location_urls = 0
    
    # Обрабатываем каждый location URL из sitemap постепенно
    for location_url in process_sitemaps_generator():
        total_location_urls += 1
        print(f"\n{'='*60}")
        print(f"Обработка location URL {total_location_urls}: {location_url}")
        print(f"{'='*60}")
        
        try:
            # Собираем ссылки на объявления для данного location
            print(f"\nСбор ссылок на объявления из {location_url}...")
            links = get_all_listing_links(location_url, concurrency=10)
            print(f"Собрано ссылок: {len(links)}")
            
            if links:
                # Парсим объявления
                print(f"Парсинг объявлений...")
                listings_data = parse_listings(links, concurrency=10)
                all_listings_data.extend(listings_data)
                print(f"Добавлено объявлений: {len(listings_data)}, всего: {len(all_listings_data)}")
        except Exception as e:
            print(f"Ошибка при обработке {location_url}: {e}")
            continue
    
    print(f"\n{'='*60}")
    print(f"Обработано location URLs: {total_location_urls}")
    print(f"Всего собрано объявлений: {len(all_listings_data)}")
    
    # Шаг 3: Сохраняем в JSON
    print("\n" + "=" * 60)
    print("ШАГ 3: Сохранение результатов")
    print("=" * 60)
    output_file = 'listings_data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        # Преобразуем DbDTO объекты в словари для JSON
        listings_dict = [dto.model_dump(exclude_none=True) for dto in all_listings_data]
        json.dump(listings_dict, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n✓ Данные сохранены в файл '{output_file}'")
    print(f"✓ Обработано объявлений: {len(all_listings_data)}")