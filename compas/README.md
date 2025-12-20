# Compass.com Parser

Парсер для сбора объявлений с сайта compass.com. Собирает ссылки на объявления через API и извлекает данные из `window.__INITIAL_DATA__` на страницах объявлений.

## Установка

```bash
pip install -r requirements.txt
```

## Зависимости

- `httpx` - для асинхронных HTTP запросов
- `fake-useragent` - для генерации User-Agent заголовков
- `requests` - для синхронных запросов (опционально)

## Использование

### Базовое использование

Запустите `test.py` для парсинга объявлений:

```bash
python test.py
```

По умолчанию скрипт:
1. Собирает ссылки на объявления из региона Hawaii
2. Парсит первые 10 объявлений (для теста)
3. Сохраняет результаты в `listings_data.json`

### Настройка параметров

Откройте `test.py` и измените параметры в блоке `if __name__ == "__main__"`:

```python
# Измените URL региона
location_url = 'https://www.compass.com/homes-for-sale/hawaii/'

# Измените количество одновременных запросов
links = get_all_listing_links(location_url, concurrency=10)

# Измените лимит объявлений для парсинга (или уберите limit для всех)
TEST_LIMIT = 10
listings_data = parse_listings(links, concurrency=10, limit=TEST_LIMIT)
```

### Примеры регионов

Скрипт работает с любым регионом Compass:

```python
# Штаты
location_url = 'https://www.compass.com/homes-for-sale/arizona/'
location_url = 'https://www.compass.com/homes-for-sale/california/'
location_url = 'https://www.compass.com/homes-for-sale/new-york/'
location_url = 'https://www.compass.com/homes-for-sale/hawaii/'

# Города
location_url = 'https://www.compass.com/homes-for-sale/honolulu-hi/'
location_url = 'https://www.compass.com/homes-for-sale/phoenix-az/'

# С координатами mapview
location_url = 'https://www.compass.com/homes-for-sale/arizona/mapview=37.0,-109.0,31.0,-114.0/'
```

## Функциональность

### 1. Сбор ссылок на объявления

Функция `get_all_listing_links()`:
- Делает POST запросы к API Compass
- Обрабатывает пагинацию автоматически
- Собирает все ссылки на объявления из указанного региона
- Работает асинхронно для высокой скорости
- Поддерживает настройку уровня параллелизма

### 2. Парсинг объявлений

Функция `parse_listings()`:
- Загружает HTML страницы объявлений
- Извлекает данные из `window.__INITIAL_DATA__` используя алгоритм подсчета скобок
- Парсит нужные поля из JSON структуры
- Работает асинхронно с настраиваемым параллелизмом

## Извлекаемые данные

Каждое объявление содержит следующие поля:

- **url** - полная ссылка на объявление
- **price** - информация о цене:
  - `formatted` - отформатированная цена (например, "$680,000")
  - `value` - числовое значение цены
  - `per_square_foot` - цена за квадратный фут (вычисляется автоматически, если не указана)
- **square_feet** - площадь в квадратных футах
- **listing_type** - тип объявления: `"sale"` (продажа) или `"rent"` (аренда)
- **description** - описание недвижимости
- **listing_status** - статус объявления (например, "Coming Soon", "Active", "Sold")
- **listing_details** - детальная информация в виде структурированных таблиц
- **photos** - массив ссылок на фотографии:
  - `url` - полная ссылка на оригинальное изображение
  - `thumbnail` - ссылка на миниатюру
  - `width`, `height` - размеры изображения
- **brochure_pdf** - ссылка на PDF брошюру (если доступна)
- **mls** - информация о MLS:
  - `source_name` - название источника
  - `source_display_name` - отображаемое название
  - `contributing_datasets` - список источников данных
  - `status` - статус MLS
  - `is_off_mls` - флаг, находится ли объявление вне MLS
  - `mls_id` - ID в MLS (если доступен)
- **agents** - массив агентов:
  - `name` - имя агента
  - `email` - email
  - `phone` - телефон
  - `contact_type` - тип контакта (например, "Listing Agent")
  - `license` - номер лицензии
  - `profile_url` - ссылка на профиль
  - `company` - компания

## Технические особенности

### Извлечение window.__INITIAL_DATA__

Парсер использует алгоритм подсчета скобок для правильного извлечения больших JSON объектов из HTML:
- Находит маркер `__INITIAL_DATA__` в HTML
- Определяет начало JSON объекта (первая `{`)
- Подсчитывает вложенные скобки, учитывая строки и экранированные символы
- Извлекает полный JSON объект до соответствующей закрывающей скобки

### Обработка данных

- Автоматическое исправление URL (убирает двойные слэши)
- Фильтрация шаблонных сообщений из описаний
- Проверка расширения файлов для brochure PDF
- Автоматический расчет цены за квадратный фут
- Поиск площади в разных местах структуры данных

### Асинхронная обработка

- Параллельные запросы для сбора ссылок
- Параллельные запросы для парсинга объявлений
- Настраиваемый уровень параллелизма (по умолчанию 10)
- Использование семафоров для контроля нагрузки

## Результаты

Результаты сохраняются в файл `listings_data.json` в формате JSON с отступами для удобного чтения.

## Пример структуры данных

```json
[
  {
    "url": "https://www.compass.com/homedetails/583-Kamoku-St-Unit-1907-Honolulu-HI-96826/LYKEY_pid/",
    "price": {
      "formatted": "$680,000",
      "value": 680000,
      "per_square_foot": 481.25
    },
    "square_feet": 1413,
    "listing_type": "sale",
    "description": "Описание недвижимости...",
    "listing_status": "Coming Soon",
    "listing_details": [...],
    "photos": [
      {
        "url": "https://www.compass.com/m/0/.../origin.jpg",
        "thumbnail": "https://www.compass.com/m/0/.../1500x1000.jpg",
        "width": 2394,
        "height": 1544
      }
    ],
    "brochure_pdf": null,
    "mls": {
      "source_name": "listing_editor_manual",
      "source_display_name": "Manual",
      "status": "Coming Soon",
      "is_off_mls": true
    },
    "agents": [
      {
        "name": "Ben Fieman",
        "email": "ben.fieman@compass.com",
        "phone": "8084007007",
        "contact_type": "Listing Agent",
        "license": "RB-23470",
        "profile_url": "https://www.compass.com/agents/ben-fieman/",
        "company": "Compass"
      }
    ]
  }
]
```

## Производительность

- Скорость сбора ссылок: зависит от количества страниц и параллелизма
- Скорость парсинга: ~10-20 объявлений/сек (при concurrency=10)
- Параллелизм: настраивается через параметр `concurrency`

## Ограничения

- Для тестирования по умолчанию установлен лимит в 10 объявлений
- Уберите параметр `limit` в функции `parse_listings()` для обработки всех объявлений
- Рекомендуется использовать разумные значения `concurrency` (10-20) чтобы не перегружать сервер

## Примечания

- Все URL автоматически преобразуются в полный формат
- Относительные ссылки на фото и профили агентов преобразуются в абсолютные
- Если описание содержит только шаблонное сообщение, оно не сохраняется
- Brochure PDF проверяется по расширению файла (.pdf)
- Цена за квадратный фут вычисляется автоматически, если не указана в данных
