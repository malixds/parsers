import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import json

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:145.0) Gecko/20100101 Firefox/145.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': 'https://rwholmes.com/estate_property-sitemap.xml',
    'Sec-GPC': '1',
    'Connection': 'keep-alive',
    # 'Cookie': 'cookieyes-consent=consentid:M3JHMkphUXI0dDRBQnpCcWlmdWdIWUZLQTRNWmZZMnU,consent:no,action:,necessary:yes,functional:no,analytics:no,performance:no,advertisement:no,other:no',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Priority': 'u=0, i',
    # Requests doesn't support trailers
    # 'TE': 'trailers',
}

def test_func():
    """Тестовая функция для сохранения HTML в файл"""
    response = requests.get('https://rwholmes.com/properties/0-maple-street-bellingham/', headers=headers)
    response.raise_for_status()
    
    # Сохраняем HTML в файл
    with open('listing.html', 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    print(f"HTML сохранен в файл listing.html ({len(response.text)} символов)")

# test_func()  # Раскомментируйте для тестирования


def get_sitemap_urls(sitemap_url):
    """Получает все URL из sitemap.xml"""
    print(f"Получаю sitemap: {sitemap_url}")
    response = requests.get(sitemap_url, headers=headers)
    response.raise_for_status()
    
    # Парсим XML
    root = ET.fromstring(response.content)
    
    # Находим все URL (namespace для sitemap)
    namespace = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = []
    
    for url_elem in root.findall('.//sitemap:url', namespace):
        loc = url_elem.find('sitemap:loc', namespace)
        if loc is not None:
            urls.append(loc.text)
    
    print(f"Найдено {len(urls)} ссылок в sitemap")
    return urls


def get_listing_id_from_url(url):
    """Извлекает ID объявления из URL"""
    # Пример: https://rwholmes.com/properties/0-maple-street-bellingham/
    # Можно использовать последнюю часть пути как ID
    parsed = urlparse(url)
    path_parts = [p for p in parsed.path.split('/') if p]
    if path_parts and path_parts[-1]:
        return path_parts[-1]
    return None


def extract_mls(soup):
    """Извлекает MLS номер"""
    mls = None
    
    # Ищем паттерны MLS
    mls_patterns = [
        re.compile(r'MLS[#:\s]*([A-Z0-9\-]+)', re.I),
        re.compile(r'MLS\s*Number[#:\s]*([A-Z0-9\-]+)', re.I),
        re.compile(r'Multiple\s*Listing\s*Service[#:\s]*([A-Z0-9\-]+)', re.I),
    ]
    
    # Ищем в тексте страницы
    page_text = soup.get_text()
    for pattern in mls_patterns:
        match = pattern.search(page_text)
        if match:
            mls = match.group(1).strip()
            break
    
    # Ищем в специальных элементах
    if not mls:
        mls_elements = soup.find_all(string=re.compile(r'MLS', re.I))
        for elem in mls_elements:
            parent = elem.parent if hasattr(elem, 'parent') else None
            if parent:
                text = parent.get_text()
                for pattern in mls_patterns:
                    match = pattern.search(text)
                    if match:
                        mls = match.group(1).strip()
                        break
                if mls:
                    break
    
    return mls


def extract_details(soup):
    """Извлекает таблицу listing_details как словарь"""
    details = {}
    
    # 1. Ищем HTML таблицы
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                if key and value:
                    details[key] = value
    
    # 2. Ищем списки определений (dl)
    dl_lists = soup.find_all('dl')
    for dl in dl_lists:
        dts = dl.find_all('dt')
        dds = dl.find_all('dd')
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True).lower()
            value = dd.get_text(strip=True)
            if key and value:
                details[key] = value
    
    # 3. Ищем div'ы с парами ключ-значение
    detail_divs = soup.find_all(class_=re.compile(r'detail|spec|feature|property-info', re.I))
    for div in detail_divs:
        # Ищем паттерны типа "Key: Value"
        text = div.get_text()
        matches = re.findall(r'([^:]+):\s*([^\n]+)', text)
        for key, value in matches:
            key = key.strip().lower()
            value = value.strip()
            if key and value:
                details[key] = value
    
    return details


def extract_listing_status(soup):
    """Извлекает статус объявления (coming soon, available, sold, leased и т.д.)"""
    status = None
    
    status_keywords = {
        'coming soon': ['coming soon', 'coming-soon', 'soon'],
        'available': ['available', 'active', 'for sale', 'for lease', 'on market'],
        'sold': ['sold', 'sale pending'],
        'leased': ['leased', 'rented'],
        'off market': ['off market', 'off-market', 'withdrawn'],
        'pending': ['pending', 'under contract'],
    }
    
    page_text = soup.get_text().lower()
    
    # Ищем в специальных элементах
    status_elements = soup.find_all(class_=re.compile(r'status|availability|listing-status', re.I))
    for elem in status_elements:
        text = elem.get_text().lower()
        for status_name, keywords in status_keywords.items():
            if any(keyword in text for keyword in keywords):
                status = status_name
                break
        if status:
            break
    
    # Если не нашли, ищем в тексте страницы
    if not status:
        for status_name, keywords in status_keywords.items():
            if any(keyword in page_text for keyword in keywords):
                status = status_name
                break
    
    return status or 'available'  # По умолчанию available


def extract_brochure_pdf(soup, base_url):
    """Извлекает ссылку на brochure PDF"""
    brochure_url = None
    
    # Ищем ссылки на PDF
    pdf_links = soup.find_all('a', href=re.compile(r'\.pdf', re.I))
    for link in pdf_links:
        href = link.get('href', '')
        text = link.get_text().lower()
        
        # Проверяем, что это brochure
        if any(keyword in text for keyword in ['brochure', 'flyer', 'marketing', 'property']):
            brochure_url = urljoin(base_url, href)
            break
    
    # Если не нашли по тексту, берем первый PDF
    if not brochure_url and pdf_links:
        href = pdf_links[0].get('href', '')
        brochure_url = urljoin(base_url, href)
    
    return brochure_url


def parse_listing_html(html, url):
    """Парсит HTML страницы объявления и извлекает данные"""
    soup = BeautifulSoup(html, 'lxml')
    
    listing_id = get_listing_id_from_url(url) or url
    
    data = {
        'listing_id': listing_id,
        'listing_link': url,
        'source_name': 'rwholmes',
        'address': '',
        'property_name': '',
        'property_description': '',
        'photos': [],
        'sale_price': None,
        'lease_price': None,
        'price': None,  # Общая цена (sale или lease)
        'size': None,
        'listing_type': None,  # 'sale' или 'lease'
        'listing_status': None,
        'listing_details': {},
        'brochure_pdf': None,
        'mls_number': None,
        'property_type': None,
        'city': None,
        'state': None,
        'zipcode': None,
        'year_built': None,
        'lot_size': None,
        'features': [],
        'agents': [],
    }
    
    # 1. Извлекаем заголовок/название
    title_tag = soup.find('title')
    if title_tag:
        data['property_name'] = title_tag.get_text(strip=True)
    
    # 2. Извлекаем адрес - ищем в разных местах
    # H1 заголовок
    h1 = soup.find('h1')
    if h1:
        data['address'] = h1.get_text(strip=True)
    
    # Альтернативно: ищем в специальных классах
    if not data['address']:
        address_elem = soup.find(class_=re.compile(r'address|property-address|location', re.I))
        if address_elem:
            data['address'] = address_elem.get_text(strip=True)
    
    # 3. Мета-теги для описания
    meta_desc = soup.find('meta', {'name': 'description'})
    if meta_desc and meta_desc.get('content'):
        data['property_description'] = meta_desc['content']
    
    # Альтернативно: ищем описание в тексте страницы
    if not data['property_description']:
        desc_elem = soup.find(class_=re.compile(r'description|about|details', re.I))
        if desc_elem:
            data['property_description'] = desc_elem.get_text(strip=True)
    
    # 4. Извлекаем изображения - более тщательно
    # Основные изображения (обычно в галерее)
    gallery = soup.find(class_=re.compile(r'gallery|slider|images|photos', re.I))
    if gallery:
        gallery_imgs = gallery.find_all('img')
        for img in gallery_imgs:
            src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
            if src:
                full_url = urljoin(url, src)
                if full_url not in data['photos'] and 'placeholder' not in full_url.lower():
                    data['photos'].append(full_url)
    
    # Если не нашли в галерее, ищем все изображения
    if not data['photos']:
        images = soup.find_all('img')
        for img in images:
            src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
            if src:
                full_url = urljoin(url, src)
                # Фильтруем логотипы и иконки
                if (full_url not in data['photos'] and 
                    'logo' not in full_url.lower() and 
                    'icon' not in full_url.lower() and
                    'placeholder' not in full_url.lower()):
                    data['photos'].append(full_url)
    
    # 5. Ищем цены - более детально
    # Ищем все элементы с ценами
    price_elements = soup.find_all(string=re.compile(r'\$[\d,]+'))
    for price_text in price_elements:
        price_str = str(price_text).strip()
        if '$' in price_str:
            # Определяем тип цены по контексту
            parent = price_text.parent if hasattr(price_text, 'parent') else None
            context = parent.get_text() if parent else price_str
            
            if any(word in context.lower() for word in ['sale', 'for sale', 'price', 'asking']):
                data['sale_price'] = price_str
                data['price'] = price_str
                data['listing_type'] = 'sale'
            elif any(word in context.lower() for word in ['lease', 'rent', 'monthly', 'annual']):
                data['lease_price'] = price_str
                data['price'] = price_str
                data['listing_type'] = 'lease'
    
    # Альтернативно: ищем в специальных классах
    price_divs = soup.find_all(class_=re.compile(r'price|cost|amount', re.I))
    for price_div in price_divs:
        price_text = price_div.get_text(strip=True)
        if '$' in price_text:
            if not data['sale_price'] and not data['lease_price']:
                data['sale_price'] = price_text
                data['price'] = price_text
                # Пытаемся определить тип из класса
                class_name = ' '.join(price_div.get('class', [])).lower()
                if 'sale' in class_name or 'purchase' in class_name:
                    data['listing_type'] = 'sale'
                elif 'lease' in class_name or 'rent' in class_name:
                    data['listing_type'] = 'lease'
                else:
                    data['listing_type'] = 'sale'  # По умолчанию sale
    
    # 6. Ищем размер площади
    size_patterns = [
        re.compile(r'([\d,]+)[\s]*sq\.?ft\.?', re.I),
        re.compile(r'([\d,]+)[\s]*square[\s]*feet', re.I),
        re.compile(r'([\d,]+)[\s]*sf', re.I),
    ]
    
    for pattern in size_patterns:
        size_match = soup.find(string=pattern)
        if size_match:
            match = pattern.search(str(size_match))
            if match:
                data['size'] = match.group(0).strip()
                break
    
    # 7. Ищем тип недвижимости
    type_keywords = ['office', 'retail', 'industrial', 'land', 'residential', 'commercial']
    page_text = soup.get_text().lower()
    for keyword in type_keywords:
        if keyword in page_text:
            data['property_type'] = keyword.capitalize()
            break
    
    # 8. Парсим адрес для города, штата, ZIP
    address_text = data['address']
    if address_text:
        # Формат: "Street, City, State ZIP"
        parts = [p.strip() for p in address_text.split(',')]
        if len(parts) >= 2:
            data['city'] = parts[-2]
            # Последняя часть: "State ZIP"
            state_zip = parts[-1].strip().split()
            if len(state_zip) >= 1:
                data['state'] = state_zip[0]
            if len(state_zip) >= 2:
                data['zipcode'] = state_zip[1]
    
    # 9. Ищем год постройки
    year_pattern = re.compile(r'(19|20)\d{2}')
    year_match = soup.find(string=year_pattern)
    if year_match:
        year_text = str(year_match)
        year = year_pattern.search(year_text)
        if year:
            year_value = int(year.group(0))
            if 1900 <= year_value <= 2025:  # Разумный диапазон
                data['year_built'] = year_value
    
    # 10. Ищем размер участка
    lot_patterns = [
        re.compile(r'([\d,]+\.?\d*)[\s]*acres?', re.I),
        re.compile(r'lot[\s]*size[:\s]+([\d,]+)', re.I),
    ]
    for pattern in lot_patterns:
        lot_match = soup.find(string=pattern)
        if lot_match:
            match = pattern.search(str(lot_match))
            if match:
                data['lot_size'] = match.group(0).strip()
                break
    
    # 11. Извлекаем особенности (features)
    features_keywords = ['parking', 'elevator', 'heating', 'cooling', 'security', 'garage']
    for keyword in features_keywords:
        if keyword in page_text:
            data['features'].append(keyword.capitalize())
    
    # 12. Ищем информацию об агентах
    agent_elements = soup.find_all(class_=re.compile(r'agent|broker|contact', re.I))
    for agent_elem in agent_elements[:3]:  # Максимум 3 агента
        agent_name = agent_elem.find(class_=re.compile(r'name|title', re.I))
        if agent_name:
            data['agents'].append(agent_name.get_text(strip=True))
    
    # 13. Извлекаем обязательные поля
    data['mls_number'] = extract_mls(soup)
    data['listing_details'] = extract_details(soup)
    data['listing_status'] = extract_listing_status(soup)
    data['brochure_pdf'] = extract_brochure_pdf(soup, url)
    
    # Если listing_type не определили, пытаемся по наличию цен
    if not data['listing_type']:
        if data['sale_price']:
            data['listing_type'] = 'sale'
        elif data['lease_price']:
            data['listing_type'] = 'lease'
    
    return data


def fetch_and_parse_listing(url):
    """Получает HTML объявления и парсит его"""
    try:
        print(f"Обрабатываю: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = parse_listing_html(response.text, url)
        return data
    except Exception as e:
        print(f"Ошибка при обработке {url}: {e}")
        return None


def main():
    # URL sitemap
    sitemap_url = 'https://rwholmes.com/estate_property-sitemap.xml'
    
    # Получаем все ссылки из sitemap
    listing_urls = get_sitemap_urls(sitemap_url)
    
    if not listing_urls:
        print("Не найдено ссылок в sitemap")
        return
    
    # Обрабатываем каждое объявление
    results = []
    for i, url in enumerate(listing_urls, 1):
        print(f"\n[{i}/{len(listing_urls)}]")
        data = fetch_and_parse_listing(url)
        if data:
            results.append(data)
            print(f"✓ Извлечено: {data.get('address', 'N/A')}")
    
    print(f"\n\nОбработано объявлений: {len(results)}/{len(listing_urls)}")
    
    # Выводим пример первого результата
    if results:
        print("\nПример данных первого объявления:")
        print(json.dumps(results[0], indent=2, ensure_ascii=False))
    
    return results


if __name__ == '__main__':
    main()
