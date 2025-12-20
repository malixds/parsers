# JLL Property Parser

Парсер для сбора объявлений с сайта property.jll.com. Собирает ссылки на объявления через sitemap XML и извлекает данные из `__NEXT_DATA__` на страницах объявлений.

## Установка

```bash
pip install -r requirements.txt
```

## Зависимости

- `httpx` - для асинхронных HTTP запросов
- `fake-useragent` - для генерации User-Agent заголовков
- `requests` - для синхронных запросов
- `pydantic` - для валидации данных через схему

## Использование

### Базовое использование

Запустите `test.py` для парсинга объявлений:

```bash
python test.py
```

По умолчанию скрипт:
1. Парсит sitemap XML из `sitemaps` массива
2. Извлекает все ссылки на объявления из `<loc>` тегов
3. Парсит первые 10 объявлений (для теста)
4. Сохраняет результаты в `listings_data.json`
5. Выводит детальную статистику по заполненности полей

### Настройка параметров

Откройте `test.py` и измените параметры в блоке `if __name__ == "__main__"`:

```python
# Измените sitemap URLs
sitemaps = [
    'https://property.jll.com/sitemap-properties.xml',
]

# Измените лимит объявлений для парсинга (или уберите limit для всех)
TEST_LIMIT = 10  # Для теста ограничиваем до 10 объявлений
listings_data = parse_listings(all_urls, concurrency=10, limit=TEST_LIMIT)

# Для обработки всех объявлений уберите limit:
listings_data = parse_listings(all_urls, concurrency=10, limit=None)
```

### Настройка параллелизма

Измените параметр `concurrency` для контроля количества одновременных запросов:

```python
listings_data = parse_listings(all_urls, concurrency=20, limit=TEST_LIMIT)
```

## Функциональность

### 1. Парсинг Sitemap

Функция `parse_sitemap()`:
- Загружает XML sitemap по указанному URL
- Извлекает все ссылки из `<loc>` тегов
- Поддерживает стандартный namespace sitemap XML
- Возвращает список всех URL объявлений

### 2. Парсинг объявлений

Функция `parse_listings()`:
- Загружает HTML страницы объявлений асинхронно
- Извлекает данные из `__NEXT_DATA__` (Next.js структура)
- Парсит нужные поля из JSON структуры
- Работает асинхронно с настраиваемым параллелизмом
- Преобразует данные в `DbDTO` объекты согласно схеме

## Извлекаемые данные

Каждое объявление содержит следующие поля:

- **source_name** - "jll"
- **listing_id** - ID объявления
- **listing_link** - полная ссылка на объявление
- **listing_type** - тип объявления: "For Sale", "For Lease", или "For Sale / For Lease"
- **listing_status** - статус объявления (например, "Lease", "Sublease")
- **address** - полный адрес
- **coordinates** - координаты в формате "latitude,longitude"
- **building_number**, **street_name**, **unit_number** - компоненты адреса
- **city**, **state**, **zipcode** - город, штат, почтовый индекс
- **sale_price** / **lease_price** - цена продажи/аренды (форматированная строка)
- **size** - площадь в формате "5,596-17,517 feet"
- **property_name** - название недвижимости
- **property_type** - тип недвижимости (office, retail, industrial и т.д.)
- **building_class** - класс здания (A, B, C, Unclassified)
- **property_description** - описание недвижимости (HTML теги удаляются)
- **property_highlights** - highlights в виде строки
- **location_highlights** - highlights в виде списка
- **listing_details** - детальная информация в виде структурированного словаря
- **photos** - массив ссылок на фотографии
- **brochure_pdf** - ссылка на PDF брошюру (или `listing_link + /brochure` если не найдена)
- **virtual_tour** - ссылка на виртуальный тур
- **agents** - массив агентов с полной информацией:
  - `name` - имя агента
  - `title` - должность
  - `license` - номер лицензии
  - `phone_primary` - телефон
  - `email` - email
  - `photo_url` - фото агента
  - `office_name` - название офиса
  - `office_phone` - телефон офиса
  - `social_media` - ссылка на LinkedIn

## Технические особенности

### Извлечение __NEXT_DATA__

Парсер использует простой поиск маркера для извлечения JSON данных:
- Находит `<script id="__NEXT_DATA__" type="application/json">` в HTML
- Извлекает JSON между открывающим и закрывающим тегами `<script>`
- Парсит JSON и извлекает `props.pageProps`

### Обработка данных

- Автоматическое форматирование цен из объектов (`amount`, `currency`, `unit`) в читаемые строки
- Обработка площади в разных форматах (словарь с min/max или просто число)
- Удаление HTML тегов из описаний
- Преобразование highlights в список строк
- Fallback для brochure PDF: если не найден, используется `listing_link + /brochure`
- Поиск агентов в `property.brokers` и корне `pageProps.brokers`

### Асинхронная обработка

- Параллельные запросы для парсинга объявлений
- Настраиваемый уровень параллелизма (по умолчанию 10)
- Использование семафоров для контроля нагрузки
- Детальное логирование ошибок

## Результаты

Результаты сохраняются в файл `listings_data.json` в формате JSON с отступами для удобного чтения.

После завершения парсинга выводится детальная статистика по заполненности всех полей:
- Базовые поля (адрес, координаты, тип, статус)
- Цены (продажа, аренда)
- Площадь и характеристики
- Описание и highlights
- Медиа (фото, brochure PDF, virtual tour)
- Агенты
- Listing details

## Пример структуры данных

```json
{
  "source_name": "jll",
  "listing_id": "725720",
  "listing_link": "https://property.jll.com/listings/8360-w-sahara-ave-las-vegas",
  "listing_type": "For Lease",
  "listing_status": "Lease",
  "address": "8360 W Sahara Ave, Las Vegas, NV, 89117",
  "coordinates": "36.145564,-115.274142",
  "street_name": "8360 W Sahara Ave",
  "city": "Las Vegas",
  "state": "NV",
  "zipcode": "89117",
  "property_type": "office",
  "building_class": "Unclassified",
  "lease_price": "USD 1.09/SF",
  "size": "5,596-17,517 feet",
  "property_description": "Описание недвижимости...",
  "location_highlights": ["Cafe with outdoor seating", "Fitness Center"],
  "photos": ["https://res.cloudinary.com/..."],
  "brochure_pdf": "https://property.jll.com/listings/.../brochure",
  "agents": [
    {
      "name": "Nick Barber",
      "title": "Managing Director, Brokerage",
      "license": "S.0058027",
      "phone_primary": "+1 702 360 4927",
      "email": "nick.barber@jll.com"
    }
  ]
}
```

## Производительность

- Скорость парсинга: ~10-20 объявлений/сек (при concurrency=10)
- Параллелизм: настраивается через параметр `concurrency`
- Обработка больших sitemap: поддерживает тысячи ссылок

## Ограничения

- Для тестирования по умолчанию установлен лимит в 10 объявлений
- Уберите параметр `limit` в функции `parse_listings()` для обработки всех объявлений
- Рекомендуется использовать разумные значения `concurrency` (10-20) чтобы не перегружать сервер

## Примечания

- Все данные валидируются через Pydantic схему (`schema.py`)
- Относительные ссылки автоматически преобразуются в абсолютные
- Если brochure PDF не найден в данных, используется стандартный путь `/brochure`
- Цены автоматически форматируются из объектов в читаемые строки
- HTML теги удаляются из описаний
- Статистика по полям выводится после каждого запуска для контроля качества данных

## Схема данных

Парсер использует общую схему `schema.py` с классом `DbDTO` для валидации и структурирования данных. Это обеспечивает:
- Валидацию типов данных
- Единый формат для всех источников
- Легкую интеграцию с базами данных

