import requests
from fake_useragent import UserAgent
import xml.etree.ElementTree as ET
import json
import time
import uuid
import asyncio
import httpx
import re
from schema import DbDTO, AgentData
from datetime import date, datetime
from typing import Optional

sitemaps = [
    'https://property.jll.com/sitemap-properties.xml',
]


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://property.jll.com/search?tenureTypes=rent&propertyTypes=office&orderBy=asc&sortBy=rentPrice",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "If-None-Match": '"6uhwaqwe2q6get"',
    "Priority": "u=0, i",
}


def extract_next_data(html: str) -> dict | None:
    marker = '<script id="__NEXT_DATA__" type="application/json">'
    start = html.find(marker)
    if start == -1:
        return None

    start += len(marker)
    end = html.find("</script>", start)
    if end == -1:
        return None

    raw = html[start:end].strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def extract_listing_from_html(html: str) -> dict | None:
    data = extract_next_data(html)
    if not data:
        return None
    try:
        return data["props"]["pageProps"]
    except (KeyError, TypeError):
        return None


def convert_jll_to_dto(page_props: dict, url: str) -> DbDTO | None:
    """
    Преобразует данные из JLL pageProps в DbDTO объект
    """
    try:
        property_data = page_props.get("property", {})
        if not property_data:
            return None
        
        # Базовые идентификаторы
        listing_id = str(property_data.get("id", "") or property_data.get("refId", ""))
        if not listing_id:
            # Используем URL как fallback
            listing_id = url.split("/")[-1] if "/" in url else "unknown"
        
        # Адрес
        address_parts = []
        if property_data.get("address"):
            address_parts.append(property_data["address"])
        if property_data.get("city"):
            address_parts.append(property_data["city"])
        if property_data.get("state"):
            address_parts.append(property_data["state"])
        if property_data.get("postcode"):
            address_parts.append(property_data["postcode"])
        address = ", ".join(address_parts) if address_parts else "Address not found"
        
        # Координаты
        coordinates = None
        if property_data.get("latitude") and property_data.get("longitude"):
            coordinates = f"{property_data['latitude']},{property_data['longitude']}"
        
        # Тип объявления
        listing_type = None
        tenure_types = property_data.get("tenureTypes", [])
        if "rent" in tenure_types and "sale" in tenure_types:
            listing_type = "For Sale / For Lease"
        elif "rent" in tenure_types:
            listing_type = "For Lease"
        elif "sale" in tenure_types:
            listing_type = "For Sale"
        
        # Статус
        listing_status = None
        labels = property_data.get("labels", [])
        if labels:
            listing_status = ", ".join(labels)
        
        # Цены - проверяем разные форматы
        sale_price = property_data.get("salePrice")
        lease_price = property_data.get("rentPrice")
        
        # Если цена - объект, форматируем её
        if isinstance(sale_price, dict):
            # Может быть объект с полями amount, currency, unit
            amount = sale_price.get("amount")
            currency = sale_price.get("currency", "USD")
            unit = sale_price.get("unit", "")
            
            if amount is not None:
                # Форматируем цену
                if unit:
                    sale_price = f"{currency} {amount}/{unit}"
                else:
                    sale_price = f"{currency} {amount}"
            else:
                # Пробуем другие поля
                sale_price = sale_price.get("formatted") or sale_price.get("value") or sale_price.get("display")
                if sale_price and not isinstance(sale_price, str):
                    sale_price = str(sale_price)
        
        if isinstance(lease_price, dict):
            amount = lease_price.get("amount")
            currency = lease_price.get("currency", "USD")
            unit = lease_price.get("unit", "")
            
            if amount is not None:
                # Форматируем цену аренды
                if unit:
                    lease_price = f"{currency} {amount}/{unit}"
                else:
                    lease_price = f"{currency} {amount}"
            else:
                lease_price = lease_price.get("formatted") or lease_price.get("value") or lease_price.get("display")
                if lease_price and not isinstance(lease_price, str):
                    lease_price = str(lease_price)
        
        # Преобразуем в строку, если это число
        if sale_price and not isinstance(sale_price, str):
            sale_price = str(sale_price)
        if lease_price and not isinstance(lease_price, str):
            lease_price = str(lease_price)
        
        # Площадь
        size_str = None
        surface_area = property_data.get("surfaceArea", {})
        if surface_area and "value" in surface_area:
            value = surface_area["value"]
            unit = surface_area.get("unit", "feet")
            
            # Проверяем тип value - может быть словарем или числом
            if isinstance(value, dict):
                if "min" in value and "max" in value:
                    if value["min"] == value["max"]:
                        size_str = f"{value['min']:,} {unit}"
                    else:
                        size_str = f"{value['min']:,}-{value['max']:,} {unit}"
                elif "min" in value:
                    size_str = f"{value['min']:,}+ {unit}"
            elif isinstance(value, (int, float)):
                # Если value - просто число
                size_str = f"{int(value):,} {unit}"
        
        # Описание
        description = None
        description_sections = property_data.get("descriptionSections", [])
        if description_sections:
            descriptions = []
            for section in description_sections:
                if section.get("content"):
                    descriptions.append(section["content"])
            if descriptions:
                description = " ".join(descriptions)
                # Убираем HTML теги (простой вариант)
                import re
                description = re.sub(r'<[^>]+>', '', description)
        
        # Highlights
        highlights_list = None
        highlights = property_data.get("highlights", [])
        if highlights:
            highlights_list = [h.get("title", "") for h in highlights if h.get("title")]
        
        # Фото
        photos_list = property_data.get("images", [])
        
        # Brochure PDF - проверяем в property_data и в корне page_props
        brochure_pdf = None
        brochures = property_data.get("brochures", [])
        if not brochures:
            # Пробуем найти в корне page_props
            brochures = page_props.get("brochures", [])
        
        if brochures:
            # Берем первый PDF
            for brochure in brochures:
                if isinstance(brochure, str) and brochure.lower().endswith('.pdf'):
                    brochure_pdf = brochure
                    break
        
        # Если brochure не найден, используем стандартный путь
        if not brochure_pdf:
            brochure_pdf = url.rstrip('/') + '/brochure'
        
        # Virtual tour
        virtual_tour = None
        virtual_tours = property_data.get("virtualTours", [])
        if virtual_tours:
            virtual_tour = virtual_tours[0] if isinstance(virtual_tours[0], str) else None
        
        # Агенты
        agents_list = []
        brokers = property_data.get("brokers", [])
        if not brokers:
            # Пробуем из корня page_props
            brokers = page_props.get("brokers", [])
        
        for broker in brokers:
            email = broker.get("email")
            if not email or email.strip() == "":
                email = None
            
            # Получаем лицензию из brokerLicenses
            license_num = None
            broker_licenses = broker.get("brokerLicenses", [])
            if broker_licenses and len(broker_licenses) > 0:
                license_num = broker_licenses[0].get("licenseNumber")
            
            # Получаем офисную информацию из entityLicenses
            office_name = None
            office_phone = None
            entity_licenses = broker.get("entityLicenses", [])
            if entity_licenses and len(entity_licenses) > 0:
                office_name = entity_licenses[0].get("company")
                office_phone = entity_licenses[0].get("mainOfficePhone")
            
            agent = AgentData(
                name=broker.get("name"),
                title=broker.get("jobTitle"),
                license=license_num,
                phone_primary=broker.get("telephone"),
                email=email,
                photo_url=broker.get("photo"),
                office_name=office_name,
                office_phone=office_phone,
                social_media=broker.get("linkedin"),
            )
            agents_list.append(agent)
        
        # Property type
        property_type = property_data.get("propertyType")
        property_types = property_data.get("propertyTypes", [])
        if property_types and not property_type:
            property_type = property_types[0]
        
        # Building class
        building_class = property_data.get("buildingClass")
        
        # Year built (если есть в данных)
        year_built = None
        
        # Listing details - сохраняем всю структуру property как словарь
        listing_details_dict = property_data.copy()
        # Удаляем большие массивы, которые уже обработаны отдельно
        for key in ["brokers", "images", "brochures", "highlights", "descriptionSections"]:
            listing_details_dict.pop(key, None)
        
        # Создаем DbDTO
        dto = DbDTO(
            source_name="jll",
            listing_id=listing_id,
            listing_link=url,
            listing_type=listing_type,
            listing_status=listing_status,
            address=address,
            coordinates=coordinates,
            building_number=None,  # Можно попытаться извлечь из address
            street_name=property_data.get("address"),
            city=property_data.get("city"),
            state=property_data.get("state"),
            zipcode=property_data.get("postcode"),
            sale_price=str(sale_price) if sale_price else None,
            lease_price=str(lease_price) if lease_price else None,
            size=size_str,
            property_name=property_data.get("title"),
            property_type=property_type,
            building_class=building_class,
            property_description=description,
            property_highlights="; ".join(highlights_list) if highlights_list else None,
            location_highlights=highlights_list,
            listing_details=listing_details_dict if listing_details_dict else None,
            photos=photos_list if photos_list else None,
            brochure_pdf=brochure_pdf,
            virtual_tour=virtual_tour,
            agents=agents_list if agents_list else None,
            year_built=year_built,
        )
        
        return dto
        
    except Exception as e:
        print(f"Ошибка при преобразовании данных в DbDTO: {e}")
        import traceback
        traceback.print_exc()
        return None


def fetch_jll_listing(url: str) -> DbDTO | None:
    """
    Получает данные листинга JLL и преобразует в DbDTO
    """
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    page_props = extract_listing_from_html(resp.text)
    if not page_props:
        return None
    return convert_jll_to_dto(page_props, url)


def parse_sitemap(sitemap_url: str) -> list[str]:
    """
    Парсит sitemap XML и извлекает все ссылки из <loc> тегов
    """
    try:
        response = requests.get(sitemap_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        # Namespace для sitemap
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Получаем все URL
        urls = []
        for url_elem in root.findall('.//ns:url', namespace):
            loc_elem = url_elem.find('ns:loc', namespace)
            if loc_elem is not None and loc_elem.text:
                urls.append(loc_elem.text.strip())
        
        return urls
    except Exception as e:
        print(f"Ошибка при парсинге sitemap {sitemap_url}: {e}")
        return []


async def parse_listing_async(client: httpx.AsyncClient, url: str, semaphore: asyncio.Semaphore) -> tuple[DbDTO | None, str | None]:
    """
    Асинхронно парсит одно объявление по URL
    Возвращает кортеж (dto, error_message)
    """
    async with semaphore:
        try:
            response = await client.get(url, headers=HEADERS, timeout=30.0, follow_redirects=True)
            response.raise_for_status()
            
            page_props = extract_listing_from_html(response.text)
            if not page_props:
                error_msg = f"⚠ Не удалось извлечь __NEXT_DATA__ из {url}"
                print(error_msg)
                return None, error_msg
            
            dto = convert_jll_to_dto(page_props, url)
            if dto:
                return dto, None
            else:
                error_msg = f"⚠ Не удалось преобразовать данные в DbDTO из {url}"
                print(error_msg)
                return None, error_msg
                
        except httpx.HTTPStatusError as e:
            error_msg = f"❌ HTTP ошибка {e.response.status_code} при запросе {url}"
            print(error_msg)
            return None, error_msg
        except httpx.TimeoutException:
            error_msg = f"❌ Таймаут при запросе {url}"
            print(error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"❌ Ошибка при парсинге {url}: {e}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            return None, error_msg


async def parse_listings_async(listing_urls: list[str], concurrency: int = 10, limit: int = None) -> list[DbDTO]:
    """
    Асинхронно парсит список объявлений
    
    Args:
        listing_urls: Список URL объявлений
        concurrency: Количество одновременных запросов
        limit: Ограничение количества объявлений для обработки (для теста)
    
    Returns:
        list: Список DbDTO объектов с данными объявлений
    """
    if limit:
        listing_urls = listing_urls[:limit]
        print(f"Ограничение: обрабатываем первые {limit} объявлений")
    
    semaphore = asyncio.Semaphore(concurrency)
    
    async with httpx.AsyncClient() as client:
        tasks = [parse_listing_async(client, url, semaphore) for url in listing_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Фильтруем успешные результаты и собираем ошибки
    parsed_listings = []
    errors = []
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            error_msg = f"❌ Исключение при обработке объявления {i+1} ({listing_urls[i]}): {result}"
            print(error_msg)
            errors.append(error_msg)
        elif isinstance(result, tuple):
            dto, error_msg = result
            if dto:
                parsed_listings.append(dto)
                print(f"✓ Обработано объявление {len(parsed_listings)}/{len(listing_urls)}: {dto.listing_link}")
            elif error_msg:
                errors.append(f"{i+1}. {error_msg}")
        else:
            error_msg = f"❌ Неожиданный тип результата для объявления {i+1}: {type(result)}"
            print(error_msg)
            errors.append(error_msg)
    
    print(f"\n{'='*60}")
    print(f"Успешно обработано: {len(parsed_listings)} из {len(listing_urls)}")
    if errors:
        print(f"\nОшибки ({len(errors)}):")
        for error in errors:
            print(f"  {error}")
    print(f"{'='*60}")
    
    return parsed_listings


def parse_listings(listing_urls: list[str], concurrency: int = 10, limit: int = None) -> list[DbDTO]:
    """
    Синхронная обертка для парсинга объявлений
    """
    return asyncio.run(parse_listings_async(listing_urls, concurrency, limit))


if __name__ == "__main__":
    # Шаг 1: Парсим sitemap и собираем ссылки
    print("=" * 60)
    print("ШАГ 1: Парсинг sitemap и сбор ссылок")
    print("=" * 60)
    
    all_urls = []
    for sitemap_url in sitemaps:
        print(f"Парсим sitemap: {sitemap_url}")
        urls = parse_sitemap(sitemap_url)
        all_urls.extend(urls)
        print(f"Найдено ссылок: {len(urls)}")
    
    print(f"\nВсего собрано ссылок: {len(all_urls)}")
    
    # Шаг 2: Парсим объявления (для теста ограничиваем до 10)
    print("\n" + "=" * 60)
    print("ШАГ 2: Парсинг объявлений")
    print("=" * 60)
    TEST_LIMIT = 10  # Для теста ограничиваем до 10 объявлений
    listings_data = parse_listings(all_urls, concurrency=10, limit=TEST_LIMIT)
    
    # Шаг 3: Сохраняем в JSON
    print("\n" + "=" * 60)
    print("ШАГ 3: Сохранение результатов")
    print("=" * 60)
    output_file = 'listings_data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        # Преобразуем DbDTO объекты в словари для JSON
        listings_dict = [dto.model_dump(exclude_none=True) for dto in listings_data]
        json.dump(listings_dict, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n✓ Данные сохранены в файл '{output_file}'")
    print(f"✓ Обработано объявлений: {len(listings_data)}")
    
    # Статистика по полям
    if listings_data:
        print(f"\n{'='*60}")
        print("СТАТИСТИКА ПО ПОЛЯМ")
        print(f"{'='*60}")
        
        total = len(listings_data)
        
        # Базовые поля
        with_address = sum(1 for dto in listings_data if dto.address and dto.address != "Address not found")
        with_coordinates = sum(1 for dto in listings_data if dto.coordinates)
        with_listing_type = sum(1 for dto in listings_data if dto.listing_type)
        with_listing_status = sum(1 for dto in listings_data if dto.listing_status)
        
        # Цены
        with_sale_price = sum(1 for dto in listings_data if dto.sale_price)
        with_lease_price = sum(1 for dto in listings_data if dto.lease_price)
        with_any_price = sum(1 for dto in listings_data if dto.sale_price or dto.lease_price)
        
        # Площадь и тип
        with_size = sum(1 for dto in listings_data if dto.size)
        with_property_type = sum(1 for dto in listings_data if dto.property_type)
        with_building_class = sum(1 for dto in listings_data if dto.building_class)
        
        # Описание и highlights
        with_description = sum(1 for dto in listings_data if dto.property_description)
        with_highlights = sum(1 for dto in listings_data if dto.location_highlights and len(dto.location_highlights) > 0)
        
        # Медиа
        with_photos = sum(1 for dto in listings_data if dto.photos and len(dto.photos) > 0)
        with_brochure = sum(1 for dto in listings_data if dto.brochure_pdf)
        with_virtual_tour = sum(1 for dto in listings_data if dto.virtual_tour)
        
        # Агенты
        with_agents = sum(1 for dto in listings_data if dto.agents and len(dto.agents) > 0)
        
        # Listing details
        with_listing_details = sum(1 for dto in listings_data if dto.listing_details)
        
        def print_stat(label, count, total_count):
            percentage = (count / total_count * 100) if total_count > 0 else 0
            print(f"  {label:30} {count:4}/{total_count:4} ({percentage:5.1f}%)")
        
        print("\n📋 Базовые поля:")
        print_stat("Адрес", with_address, total)
        print_stat("Координаты", with_coordinates, total)
        print_stat("Тип объявления", with_listing_type, total)
        print_stat("Статус", with_listing_status, total)
        
        print("\n💰 Цены:")
        print_stat("Цена продажи", with_sale_price, total)
        print_stat("Цена аренды", with_lease_price, total)
        print_stat("Любая цена", with_any_price, total)
        
        print("\n📐 Площадь и характеристики:")
        print_stat("Площадь", with_size, total)
        print_stat("Тип недвижимости", with_property_type, total)
        print_stat("Класс здания", with_building_class, total)
        
        print("\n📝 Описание:")
        print_stat("Описание", with_description, total)
        print_stat("Highlights", with_highlights, total)
        
        print("\n🖼️  Медиа:")
        print_stat("Фото", with_photos, total)
        print_stat("Brochure PDF", with_brochure, total)
        print_stat("Virtual Tour", with_virtual_tour, total)
        
        print("\n👥 Агенты:")
        print_stat("Агенты", with_agents, total)
        
        print("\n📄 Детали:")
        print_stat("Listing Details", with_listing_details, total)
        
        print(f"\n{'='*60}")
        
        # Показываем пример первого объявления
        print(f"\nПример данных первого объявления:")
        first_dict = listings_data[0].model_dump(exclude_none=True)
        print(json.dumps(first_dict, ensure_ascii=False, indent=2, default=str)[:500] + "...")