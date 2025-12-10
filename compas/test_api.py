import requests
import json
from fake_useragent import UserAgent

headers = {
    'User-Agent': UserAgent().random,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.compass.com/homes-for-sale/new-york/mapview=45.2954092,-72.3285732,39.839376,-79.2115078/?searchQuery=%7B%22sort%22%3A%7B%22column%22%3A%22dom%22%2C%22direction%22%3A%22asc%22%7D%7D',
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
    'searchResultId': '196bfabe-7001-4d50-ab43-c7c0e0427468',
    'rawLolSearchQuery': {
        'listingTypes': [
            2,
        ],
        'nePoint': {
            'latitude': 45.3525295,
            'longitude': -72.3285732,
        },
        'swPoint': {
            'latitude': 39.9017281,
            'longitude': -79.2115078,
        },
        'saleStatuses': [
            12,
            9,
        ],
        'num': 41,
        'sortOrder': 46,
        'locationIds': [
            192339,
        ],
        'facetFieldNames': [
            'contributingDatasetList',
            'compassListingTypes',
            'comingSoon',
        ],
    },
    'viewport': {
        'northeast': {
            'lat': 45.2954092,
            'lng': -72.3285732,
        },
        'southwest': {
            'lat': 39.839376,
            'lng': -79.2115078,
        },
    },
    'viewportFrom': 'response',
    'height': 1350,
    'width': 1253,
    'isMapFullyInitialized': True,
    'purpose': 'search',
}

url = 'https://www.compass.com/homes-for-sale/new-york/mapview=45.2954092,-72.3285732,39.839376,-79.2115078/'

print("Отправляю POST запрос к API compass.com...")
print(f"URL: {url}")
print(f"Status code: ", end="")

try:
    response = requests.post(
        url,
        params=params,
        json=json_data,
        headers=headers,
        timeout=30
    )
    
    print(response.status_code)
    print(f"\nResponse headers:")
    for key, value in response.headers.items():
        print(f"  {key}: {value}")
    
    print(f"\nResponse content length: {len(response.content)} bytes")
    print(f"Response content type: {response.headers.get('Content-Type', 'unknown')}")
    
    # Пробуем распарсить как JSON
    try:
        data = response.json()
        print(f"\n✅ Ответ успешно распарсен как JSON")
        print(f"Тип данных: {type(data)}")
        
        if isinstance(data, dict):
            print(f"\nКлючи в ответе:")
            for key in data.keys():
                print(f"  - {key}")
            
            # Сохраняем полный ответ в файл
            import os
            os.makedirs('htmls', exist_ok=True)
            with open('htmls/api_response.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"\n💾 Полный ответ сохранен в: htmls/api_response.json")
            
            # Пробуем найти объявления
            if 'lolResults' in data:
                lol_results = data['lolResults']
                print(f"\n📋 lolResults найден!")
                if isinstance(lol_results, dict):
                    print(f"  Структура lolResults:")
                    for key in lol_results.keys():
                        val = lol_results[key]
                        if isinstance(val, list):
                            print(f"    - {key}: список из {len(val)} элементов")
                        elif isinstance(val, dict):
                            print(f"    - {key}: словарь с ключами {list(val.keys())[:5]}")
                        else:
                            print(f"    - {key}: {type(val).__name__}")
                    
                    # Ищем объявления
                    if 'listings' in lol_results:
                        listings = lol_results['listings']
                        print(f"\n✅ Найдено объявлений в lolResults.listings: {len(listings)}")
                        if len(listings) > 0:
                            print(f"\nПример первого объявления:")
                            first = listings[0]
                            if isinstance(first, dict):
                                print(f"  Ключи: {list(first.keys())[:10]}")
                                if 'listingIdSHA' in first:
                                    print(f"  listingIdSHA: {first['listingIdSHA']}")
                                if 'location' in first:
                                    loc = first['location']
                                    if isinstance(loc, dict) and 'prettyAddress' in loc:
                                        print(f"  Address: {loc['prettyAddress']}")
                    elif 'results' in lol_results:
                        results = lol_results['results']
                        print(f"\n✅ Найдено результатов в lolResults.results: {len(results)}")
                elif isinstance(lol_results, list):
                    print(f"  lolResults - список из {len(lol_results)} элементов")
            
            if 'listings' in data:
                print(f"\n📋 Найдено объявлений в корне: {len(data['listings'])}")
            elif 'results' in data:
                print(f"\n📋 Найдено результатов в корне: {len(data['results'])}")
            elif 'data' in data:
                if isinstance(data['data'], list):
                    print(f"\n📋 Найдено записей в data: {len(data['data'])}")
                elif isinstance(data['data'], dict):
                    print(f"\n📋 Структура data:")
                    for key in data['data'].keys():
                        print(f"  - {key}")
        elif isinstance(data, list):
            print(f"\n📋 Ответ - список из {len(data)} элементов")
            if len(data) > 0:
                print(f"Первый элемент: {type(data[0])}")
        
    except json.JSONDecodeError:
        print(f"\n⚠️  Ответ не является валидным JSON")
        print(f"Первые 500 символов ответа:")
        print(response.text[:500])
        
        # Сохраняем текст ответа
        import os
        os.makedirs('htmls', exist_ok=True)
        with open('htmls/api_response.txt', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"\n💾 Ответ сохранен в: htmls/api_response.txt")
        
except requests.exceptions.RequestException as e:
    print(f"\n❌ Ошибка при выполнении запроса: {e}")

