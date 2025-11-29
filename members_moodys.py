import asyncio
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urljoin, urlparse

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


class RwholmesParser:
    """
    Парсер для rwholmes.com
    1. Индексация - сбор всех URL /property/{listing_id}
    2. Получение HTML для каждого листинга
    3. Парсинг HTML и извлечение обязательных полей
    """
    
    def __init__(
        self,
        client: httpx.AsyncClient,
        source_name: str = "rwholmes",
        concurrency: int = 10,
        save_html_every: int = 20,
        html_save_dir: str = "htmls",
    ) -> None:
        self.client = client
        self.source_name = source_name
        self.semaphore = asyncio.Semaphore(concurrency)
        
        self.sitemap_url = "https://rwholmes.com/estate_property-sitemap.xml"
        self.base_url = "https://rwholmes.com"
        
        # Настройки сохранения HTML
        self.save_html_every = save_html_every
        self.html_save_dir = html_save_dir
        self.html_counter = 0
        
        # Создаем папку для сохранения HTML, если её нет
        if not os.path.exists(self.html_save_dir):
            os.makedirs(self.html_save_dir)
            logger.info(f"Создана папка для сохранения HTML: {self.html_save_dir}")

    # ---------------------- NETWORK ----------------------

    def get_headers(self) -> dict[str, str]:
        return {
            "User-Agent": UserAgent().random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://rwholmes.com/estate_property-sitemap.xml",
            "Sec-GPC": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
        }

    async def get_html(self, url: str) -> str | None:
        """Получает HTML страницы"""
        try:
            async with self.semaphore:
                resp = await self.client.get(
                    url,
                    headers=self.get_headers(),
                    timeout=10.0,
                    follow_redirects=True,  # Следовать редиректам
                )
                resp.raise_for_status()
                return resp.text
        except Exception as e:
            logger.error(f"GET {url} failed: {e}")
            return None

    # ---------------------- ЭТАП 1: ИНДЕКСАЦИЯ ----------------------

    async def get_listing_urls(self) -> list[str]:
        """
        ЭТАП 1: Собирает все URL вида /property/{listing_id} из sitemap
        """
        logger.info(f"[1] Получаю sitemap: {self.sitemap_url}")
        
        try:
            resp = await self.client.get(
                self.sitemap_url, 
                headers=self.get_headers(),
                follow_redirects=True
            )
            resp.raise_for_status()
            
            # Парсим XML
            root = ET.fromstring(resp.content)
            namespace = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            urls = []
            
            for url_elem in root.findall('.//sitemap:url', namespace):
                loc = url_elem.find('sitemap:loc', namespace)
                if loc is not None:
                    # Получаем текст, учитывая CDATA
                    url = loc.text
                    # Если текст None, пробуем получить через tostring
                    if url is None:
                        url_text = ET.tostring(loc, encoding='unicode', method='text')
                        # Извлекаем текст между CDATA или просто текст
                        url = url_text.strip() if url_text else None
                    
                    if url and '/propert' in url.lower():  # Ищем /property или /properties
                        urls.append(url.strip())
            
            logger.info(f"[1] Найдено {len(urls)} ссылок на объявления")
            return urls
        except Exception as e:
            logger.error(f"[1] Ошибка при получении sitemap: {e}")
            return []

    @staticmethod
    def extract_listing_id_from_url(url: str) -> str | None:
        """Извлекает listing_id из URL"""
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        if path_parts:
            # Ищем часть после /property/ или /properties/
            for i, part in enumerate(path_parts):
                if part.lower() in ['property', 'properties'] and i + 1 < len(path_parts):
                    return path_parts[i + 1]
            # Если не нашли, берем последнюю часть
            return path_parts[-1]
        return None

    # ---------------------- ЭТАП 2: ПОЛУЧЕНИЕ HTML ----------------------

    async def get_listing_html(self, url: str) -> str | None:
        """
        ЭТАП 2: Получает HTML для листинга
        """
        return await self.get_html(url)

    # ---------------------- ЭТАП 3: ПАРСИНГ ОБЯЗАТЕЛЬНЫХ ПОЛЕЙ ----------------------

    @staticmethod
    def extract_mls(soup: BeautifulSoup) -> str | None:
        """Извлекает MLS номер"""
        mls = None
        
        mls_patterns = [
            re.compile(r'MLS[#:\s]*([A-Z0-9\-]+)', re.I),
            re.compile(r'MLS\s*Number[#:\s]*([A-Z0-9\-]+)', re.I),
            re.compile(r'Multiple\s*Listing\s*Service[#:\s]*([A-Z0-9\-]+)', re.I),
        ]
        
        page_text = soup.get_text()
        for pattern in mls_patterns:
            match = pattern.search(page_text)
            if match:
                mls = match.group(1).strip()
                break
        
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

    @staticmethod
    def extract_details(soup: BeautifulSoup) -> dict[str, str]:
        """Извлекает таблицу listing_details как словарь"""
        details = {}
        
        # 1. HTML таблицы
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
        
        # 2. Списки определений (dl)
        dl_lists = soup.find_all('dl')
        for dl in dl_lists:
            dts = dl.find_all('dt')
            dds = dl.find_all('dd')
            for dt, dd in zip(dts, dds):
                key = dt.get_text(strip=True).lower()
                value = dd.get_text(strip=True)
                if key and value:
                    details[key] = value
        
        # 3. Div'ы с парами ключ-значение
        detail_divs = soup.find_all(class_=re.compile(r'detail|spec|feature|property-info', re.I))
        for div in detail_divs:
            text = div.get_text()
            matches = re.findall(r'([^:]+):\s*([^\n]+)', text)
            for key, value in matches:
                key = key.strip().lower()
                value = value.strip()
                if key and value:
                    details[key] = value

        # 4. Фоллбек: парсим элементы <li> с шаблоном "ключ: значение"
        # Это помогает, если детали заданы в списках, а не в таблице.
        for li in soup.find_all("li"):
            text = li.get_text(" ", strip=True)
            if ":" not in text:
                continue
            key, value = text.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            # Немного фильтруем шум: ключ не слишком длинный и есть значение
            if key and value and len(key) <= 60:
                # Не перезаписываем уже найденные ключи
                if key not in details:
                    details[key] = value

        # 5. Фоллбек: парсим <b>/<strong> внутри абзацев, как в блоке описания:
        # <p><b>Available: </b>2,200 – 4,752 SF<br><b>Building Size: </b>30,000 SF ...</p>
        for bold in soup.find_all(["b", "strong"]):
            parent = bold.parent
            if not parent:
                continue

            raw_key = bold.get_text(" ", strip=True)
            if not raw_key:
                continue

            key = raw_key.rstrip(":").strip().lower()
            # Фильтруем слишком короткие/длинные ключи
            if not key or len(key) < 3 or len(key) > 60:
                continue

            # Собираем значение из соседей до следующего <br> или следующего <b>/<strong>
            parts: list[str] = []
            for sib in bold.next_siblings:
                # Останавливаемся на <br> или новом ключе
                if hasattr(sib, "name") and sib.name is not None:
                    if sib.name.lower() in ["br", "b", "strong"]:
                        break
                    # Теги с текстом (например, span)
                    text = sib.get_text(" ", strip=True)
                    if text:
                        parts.append(text)
                else:
                    # Текстовый узел
                    text = str(sib).strip()
                    if text:
                        parts.append(text)

            value = re.sub(r"\s+", " ", " ".join(parts)).strip()
            if value and key not in details:
                details[key] = value

        return details

    @staticmethod
    def extract_price(soup: BeautifulSoup) -> tuple[str | None, str | None]:
        """
        Извлекает цену и тип (sale/lease)
        Возвращает: (price_value, listing_type)
        """
        price_value = None
        listing_type = None
        
        price_pattern = re.compile(r'\$[\d,]+(?:\.[\d]+)?')
        price_elements = soup.find_all(string=price_pattern)
        
        for price_text in price_elements:
            price_str = str(price_text).strip()
            match = price_pattern.search(price_str)
            if match:
                parent = price_text.parent if hasattr(price_text, 'parent') else None
                context = parent.get_text().lower() if parent else price_str.lower()
                
                if any(word in context for word in ['sale', 'for sale', 'asking', 'purchase', 'buy']):
                    price_value = match.group(0)
                    listing_type = 'sale'
                    break
                elif any(word in context for word in ['lease', 'rent', 'rental', 'monthly', 'annual', 'per sqft']):
                    price_value = match.group(0)
                    listing_type = 'lease'
                    break
        
        if not price_value:
            price_divs = soup.find_all(class_=re.compile(r'price|cost|amount', re.I))
            for price_div in price_divs:
                price_text = price_div.get_text(strip=True)
                match = price_pattern.search(price_text)
                if match:
                    price_value = match.group(0)
                    class_name = ' '.join(price_div.get('class', [])).lower()
                    if 'sale' in class_name or 'purchase' in class_name:
                        listing_type = 'sale'
                    elif 'lease' in class_name or 'rent' in class_name:
                        listing_type = 'lease'
                    break
        
        return price_value, listing_type

    @staticmethod
    def extract_listing_type_from_page(soup: BeautifulSoup) -> str | None:
        """
        Дополнительный способ определить тип сделки (sale/lease),
        даже если цену не нашли.
        """
        # Пробуем по спец. блокам
        type_blocks = [
            soup.find(class_=re.compile(r'property_categories_type1_wrapper', re.I)),
            soup.find(class_=re.compile(r'action_tag_wrapper', re.I)),
        ]
        for block in type_blocks:
            if not block:
                continue
            text = block.get_text(" ", strip=True).lower()
            if 'for lease' in text or 'lease' in text or 'for rent' in text:
                return 'lease'
            if 'for sale' in text or 'sale' in text:
                return 'sale'

        # Фоллбек по всей странице
        page_text = soup.get_text(" ", strip=True).lower()
        # Сначала проверяем более специфичный lease, чтобы не путать с "for sale or lease"
        if 'for lease' in page_text or 'for rent' in page_text:
            return 'lease'
        if 'for sale' in page_text:
            return 'sale'

        return None

    @staticmethod
    def extract_size(soup: BeautifulSoup) -> str | None:
        """Извлекает площадь в квадратных футах"""
        size = None
        
        size_patterns = [
            re.compile(r'([\d,]+(?:\.[\d]+)?)[\s]*sq\.?ft\.?', re.I),
            re.compile(r'([\d,]+(?:\.[\d]+)?)[\s]*square[\s]*feet', re.I),
            re.compile(r'([\d,]+(?:\.[\d]+)?)[\s]*sf\b', re.I),
            re.compile(r'size[:\s]+([\d,]+(?:\.[\d]+)?)', re.I),
        ]
        
        page_text = soup.get_text()
        for pattern in size_patterns:
            match = pattern.search(page_text)
            if match:
                size = match.group(0).strip()
                break
        
        return size

    @staticmethod
    def extract_address(soup: BeautifulSoup) -> str:
        """Извлекает адрес недвижимости (обязательное поле)"""
        address = None
        
        # 1. H1 заголовок (часто содержит адрес)
        h1 = soup.find('h1')
        if h1:
            h1_text = h1.get_text(strip=True)
            # Проверяем, что это похоже на адрес (содержит цифры и улицу)
            if re.search(r'\d+', h1_text) and len(h1_text) > 10:
                address = h1_text
        
        # 2. Специальные классы для адреса
        if not address:
            address_selectors = [
                soup.find(class_=re.compile(r'address|property-address|location|street', re.I)),
                soup.find(id=re.compile(r'address|property-address|location', re.I)),
                soup.find('div', {'itemprop': 'address'}),
                soup.find('span', {'itemprop': 'address'}),
            ]
            
            for selector in address_selectors:
                if selector:
                    addr_text = selector.get_text(strip=True)
                    if len(addr_text) > 5 and re.search(r'\d+', addr_text):
                        address = addr_text
                        break
        
        # 3. Title тег (может содержать адрес)
        if not address:
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                # Ищем адрес в title (обычно в начале или конце)
                if re.search(r'\d+.*(street|st|avenue|ave|road|rd|drive|dr|boulevard|blvd|way|ln)', title_text, re.I):
                    address = title_text.split('|')[0].strip()
        
        # 4. Мета-теги
        if not address:
            meta_address = soup.find('meta', {'property': 'og:street-address'})
            if meta_address and meta_address.get('content'):
                address = meta_address['content']
        
        # 5. Если ничего не нашли, пытаемся извлечь из описания или первого параграфа
        if not address:
            desc = soup.find(class_=re.compile(r'description|summary|details', re.I))
            if desc:
                desc_text = desc.get_text()
                # Ищем паттерн адреса в описании
                addr_match = re.search(r'(\d+\s+[\w\s]+(?:street|st|avenue|ave|road|rd|drive|dr|boulevard|blvd|way|ln)[,\s]+[\w\s]+[,\s]+[A-Z]{2}\s+\d{5})', desc_text, re.I)
                if addr_match:
                    address = addr_match.group(1).strip()
        
        # Возвращаем адрес или fallback на URL если ничего не нашли
        return address or "Address not found"

    @staticmethod
    def extract_description(soup: BeautifulSoup) -> str | None:
        """Извлекает описание недвижимости"""
        description = None
        
        # 1. Мета-тег description
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            description = meta_desc['content'].strip()
        
        # 2. Специальные блоки
        if not description:
            desc_selectors = [
                soup.find(class_=re.compile(r'description|about|details|summary', re.I)),
                soup.find(id=re.compile(r'description|about|details', re.I)),
                soup.find('div', {'itemprop': 'description'}),
            ]
            
            for desc_elem in desc_selectors:
                if desc_elem:
                    description = desc_elem.get_text(strip=True)
                    if len(description) > 50:
                        break
        
        # 3. Параграфы
        if not description:
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 100:
                    description = text
                    break
        
        return description

    @staticmethod
    def extract_listing_status(soup: BeautifulSoup) -> str:
        """Извлекает статус объявления"""
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
        
        status_elements = soup.find_all(class_=re.compile(r'status|availability|listing-status', re.I))
        for elem in status_elements:
            text = elem.get_text().lower()
            for status_name, keywords in status_keywords.items():
                if any(keyword in text for keyword in keywords):
                    status = status_name
                    break
            if status:
                break
        
        if not status:
            for status_name, keywords in status_keywords.items():
                if any(keyword in page_text for keyword in keywords):
                    status = status_name
                    break
        
        return status or 'available'

    @staticmethod
    def extract_photos(soup: BeautifulSoup, base_url: str) -> list[str]:
        """Извлекает ссылки на фото"""
        photos = []
        
        # 1. Галерея
        gallery = soup.find(class_=re.compile(r'gallery|slider|carousel|images|photos', re.I))
        if gallery:
            gallery_imgs = gallery.find_all('img')
            for img in gallery_imgs:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy-src') or img.get('data-original')
                if src:
                    full_url = urljoin(base_url, src)
                    if full_url not in photos and 'placeholder' not in full_url.lower():
                        photos.append(full_url)
        
        # 2. Все изображения
        if not photos:
            images = soup.find_all('img')
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy-src') or img.get('data-original')
                if src:
                    full_url = urljoin(base_url, src)
                    if (full_url not in photos and 
                        'logo' not in full_url.lower() and 
                        'icon' not in full_url.lower() and
                        'placeholder' not in full_url.lower() and
                        'avatar' not in full_url.lower()):
                        photos.append(full_url)
        
        return photos

    @staticmethod
    def extract_brochure_pdf(soup: BeautifulSoup, base_url: str) -> str | None:
        """Извлекает ссылку на brochure PDF"""
        brochure_url = None
        
        pdf_links = soup.find_all('a', href=re.compile(r'\.pdf', re.I))
        for link in pdf_links:
            href = link.get('href', '')
            text = link.get_text().lower()
            
            if any(keyword in text for keyword in ['brochure', 'flyer', 'marketing', 'property']):
                brochure_url = urljoin(base_url, href)
                break
        
        if not brochure_url and pdf_links:
            href = pdf_links[0].get('href', '')
            brochure_url = urljoin(base_url, href)
        
        return brochure_url

    @staticmethod
    def extract_agents(soup: BeautifulSoup, base_url: str) -> list[AgentData]:
        """
        Извлекает агентов (основного и других) в список AgentData.
        Ориентируемся на блоки sidebar + mobile agent area + секцию \"Other Agents\".
        """
        agents: dict[str, AgentData] = {}

        def add_agent(
            name: str | None,
            title: str | None = None,
            photo_url: str | None = None,
            phone: str | None = None,
            link: str | None = None,
        ) -> None:
            if not name:
                return
            key = name.strip().lower()
            if key not in agents:
                agents[key] = AgentData(
                    name=name.strip(),
                    title=title.strip() if title else None,
                    phone_primary=phone.strip() if phone else None,
                    photo_url=photo_url.strip() if photo_url else None,
                    social_media=link.strip() if link else None,
                )
            else:
                agent = agents[key]
                if title and not agent.title:
                    agent.title = title.strip()
                if phone and not agent.phone_primary:
                    agent.phone_primary = phone.strip()
                if photo_url and not agent.photo_url:
                    agent.photo_url = photo_url.strip()
                if link and not agent.social_media:
                    agent.social_media = link.strip()

        # --------- 1. Sidebar агент ---------
        sidebar_unit = soup.find("div", class_=re.compile(r"agent_unit_widget_sidebar_wrapper_unit"))
        if sidebar_unit:
            # имя + ссылка
            name = None
            link = None
            h4 = sidebar_unit.find("h4")
            if h4:
                a = h4.find("a")
                if a:
                    name = a.get_text(strip=True)
                    link = a.get("href")
                else:
                    name = h4.get_text(strip=True)

            # должность
            position_el = sidebar_unit.find(class_=re.compile(r"agent_position"))
            title = position_el.get_text(strip=True) if position_el else None

            # фото (background-image)
            photo_url = None
            photo_div = sidebar_unit.find(class_=re.compile(r"agent_unit_widget_sidebar"))
            if photo_div:
                style = photo_div.get("style", "")
                # style="background-image: url(https://...jpg)"
                match = re.search(r"url\(['\"]?([^'\")]+)", style)
                if match:
                    photo_url = match.group(1)
                    if not photo_url.startswith("http"):
                        photo_url = urljoin(base_url, photo_url)

            # телефон из кнопки Call
            phone = None
            call_link = soup.find("a", class_=re.compile(r"realtor_call"))
            if call_link:
                # сначала пробуем текст внутри <span class="agent_call_no">
                span_phone = call_link.find(class_=re.compile(r"agent_call_no"))
                if span_phone:
                    phone = span_phone.get_text(strip=True)
                else:
                    href = call_link.get("href", "")
                    # href="tel:(508) 651-9017"
                    tel_match = re.search(r"tel:(.+)$", href)
                    if tel_match:
                        phone = tel_match.group(1).strip()

            add_agent(name=name, title=title, photo_url=photo_url, phone=phone, link=link)

        # --------- 2. Мобильный блок агента ---------
        mobile_blocks = soup.find_all("div", class_=re.compile(r"mobile_agent_area_wrapper"))
        for block in mobile_blocks:
            # имя + ссылка
            name = None
            link = None
            name_link = block.find("a")
            if name_link:
                name = name_link.get_text(strip=True)
                link = name_link.get("href")

            # фото
            photo_url = None
            pict_div = block.find("div", class_=re.compile(r"agentpict"))
            if pict_div:
                style = pict_div.get("style", "")
                match = re.search(r"url\(['\"]?([^'\")]+)", style)
                if match:
                    photo_url = match.group(1)
                    if not photo_url.startswith("http"):
                        photo_url = urljoin(base_url, photo_url)

            # телефон – тот же, что и в sidebar (если есть)
            phone = None
            call_link = soup.find("a", class_=re.compile(r"realtor_call"))
            if call_link:
                span_phone = call_link.find(class_=re.compile(r"agent_call_no"))
                if span_phone:
                    phone = span_phone.get_text(strip=True)
                else:
                    href = call_link.get("href", "")
                    tel_match = re.search(r"tel:(.+)$", href)
                    if tel_match:
                        phone = tel_match.group(1).strip()

            add_agent(name=name, photo_url=photo_url, phone=phone, link=link)

        # --------- 3. Секция \"Other Agents\" (property_other_agents) ---------
        # Структура по примеру https://rwholmes.com/properties/11-huron-drive-natick/
        other_section = soup.find(id=re.compile(r"property_other_agents", re.I))
        if other_section:
            # Ищем заголовки h3/h4 под этим блоком — там имена агентов
            for heading in other_section.find_all(["h3", "h4"]):
                name = heading.get_text(strip=True)
                if not name:
                    continue
                lower = name.lower()
                # Пропускаем сам заголовок \"Other Agents\" и похожее
                if "other agents" in lower:
                    continue

                # Попробуем взять должность как ближайший непустой текст после заголовка
                title = None
                sib = heading.find_next_sibling()
                while sib is not None and sib.name not in ["h3", "h4"]:
                    text = sib.get_text(" ", strip=True) if hasattr(sib, "get_text") else str(sib).strip()
                    if text:
                        title = text
                        break
                    sib = sib.find_next_sibling()

                add_agent(name=name, title=title, photo_url=None, phone=None, link=None)

        # Возвращаем список AgentData
        return list(agents.values())

    def _save_html_if_needed(self, html: str, listing_id: str, url: str) -> None:
        """Сохраняет HTML в файл, если нужно (каждый N-й)"""
        self.html_counter += 1
        
        if self.html_counter % self.save_html_every == 0:
            # Создаем безопасное имя файла из listing_id
            safe_filename = re.sub(r'[^\w\-_\.]', '_', listing_id)
            if not safe_filename or safe_filename == '_':
                safe_filename = f"listing_{self.html_counter}"
            
            filepath = os.path.join(self.html_save_dir, f"{safe_filename}.html")
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(html)
                logger.info(f"💾 Сохранен HTML [{self.html_counter}]: {filepath}")
            except Exception as e:
                logger.error(f"Ошибка при сохранении HTML {filepath}: {e}")

    async def parse_listing(self, url: str) -> DbDTO | None:
        """
        ЭТАП 3: Парсит HTML и извлекает обязательные поля
        Возвращает DbDTO объект
        """
        # Получаем HTML
        html = await self.get_listing_html(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'lxml')
        listing_id = self.extract_listing_id_from_url(url) or url
        
        # Сохраняем каждый N-й HTML
        self._save_html_if_needed(html, listing_id, url)
        
        # Извлекаем обязательные поля
        price, listing_type = self.extract_price(soup)
        if not listing_type:
            # Пытаемся определить тип по тексту страницы (For Lease / For Sale и т.п.)
            listing_type = self.extract_listing_type_from_page(soup)
        size = self.extract_size(soup)
        description = self.extract_description(soup)
        listing_status = self.extract_listing_status(soup)
        listing_details = self.extract_details(soup)
        photos = self.extract_photos(soup, self.base_url)
        brochure_pdf = self.extract_brochure_pdf(soup, self.base_url)
        mls_number = self.extract_mls(soup)
        address = self.extract_address(soup)
        agents = self.extract_agents(soup, self.base_url)
        
        # Разделяем цену на sale_price и lease_price в зависимости от типа
        sale_price = None
        lease_price = None
        if price:
            if listing_type == 'sale':
                sale_price = price
            elif listing_type == 'lease':
                lease_price = price
            else:
                # Если тип не определен, пытаемся угадать по контексту
                # Или записываем в оба поля
                sale_price = price
                lease_price = price
        
        # Создаем и возвращаем DbDTO объект
        try:
            dto = DbDTO(
                source_name=self.source_name,
                listing_id=listing_id,
                listing_link=url,
                listing_type=listing_type,
                listing_status=listing_status,
                address=address,  # Обязательное поле
                sale_price=sale_price,
                lease_price=lease_price,
                size=size,
                property_description=description,
                listing_details=listing_details if listing_details else None,
                photos=photos if photos else None,
                brochure_pdf=brochure_pdf,
                mls_number=mls_number,
                agents=agents or None,
                agency_phone=agents[0].phone_primary if agents and agents[0].phone_primary else None,
            )
            return dto
        except Exception as e:
            logger.error(f"Ошибка при создании DbDTO для {url}: {e}")
            return None

    # ---------------------- ОСНОВНОЙ ПРОЦЕСС ----------------------

    async def run(self) -> list[DbDTO]:
        """
        Основной процесс:
        1. Индексация - сбор всех URL
        2. Получение HTML для каждого листинга
        3. Парсинг обязательных полей
        Возвращает список DbDTO объектов
        """
        # ЭТАП 1: Индексация
        listing_urls = await self.get_listing_urls()
        
        if not listing_urls:
            logger.warning("Не найдено ссылок на объявления")
            return []
        
        # ЭТАП 2 и 3: Обработка каждого листинга
        results: list[DbDTO] = []
        tasks = [self.parse_listing(url) for url in listing_urls]
        
        logger.info(f"[2-3] Начинаю обработку {len(listing_urls)} объявлений...")
        
        parsed_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(parsed_results):
            if isinstance(result, Exception):
                logger.error(f"Ошибка при обработке {listing_urls[i]}: {result}")
                continue
            
            if result and isinstance(result, DbDTO):
                results.append(result)
                logger.info(f"✓ [{len(results)}/{len(listing_urls)}] {result.listing_id}")
        
        logger.info(f"\nОбработано объявлений: {len(results)}/{len(listing_urls)}")
        return results


# ---------------------- ПРИМЕР ИСПОЛЬЗОВАНИЯ ----------------------

async def main():
    async with httpx.AsyncClient() as client:
        parser = RwholmesParser(client, concurrency=10)
        results = await parser.run()
        
        if results:
            print("\nПример данных первого объявления:")
            # Преобразуем DbDTO в словарь для вывода
            first_dto = results[0]
            print(json.dumps(first_dto.model_dump(), indent=2, ensure_ascii=False))
        
        return results


if __name__ == '__main__':
    asyncio.run(main())
