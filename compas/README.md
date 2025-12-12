# Compass.com Parser

Парсер для сбора объявлений с сайта compass.com

## Установка

```bash
pip install -r requirements.txt
```

## Использование

### 1. Парсинг одного города

#### С лимитом результатов:
```bash
python run_full_parse.py new-york 1000
```

#### Без лимита (собирает все объявления):
```bash
python run_full_parse.py new-york all
# или
python run_full_parse.py new-york none
# или
python run_full_parse.py new-york
```

#### Примеры других городов:
```bash
python run_full_parse.py los-angeles all
python run_full_parse.py miami all
python run_full_parse.py san-francisco all
python run_full_parse.py chicago all
```

### 2. Парсинг всех городов

#### Парсинг всех популярных городов (без лимита):
```bash
python run_all_cities.py
```

#### С лимитом на город:
```bash
python run_all_cities.py --max-results 5000
```

#### Парсинг конкретных городов:
```bash
python run_all_cities.py --cities new-york los-angeles miami
```

#### Настройка параллелизма:
```bash
python run_all_cities.py --concurrency 5
```

#### Изменение директории для результатов:
```bash
python run_all_cities.py --output-dir my_results
```

### 3. Полный синтаксис

#### run_full_parse.py:
```bash
python run_full_parse.py <город> [лимит]
```

Где:
- `<город>` - название города (например, "new-york", "los-angeles")
- `[лимит]` - опционально, максимальное количество результатов. Если не указано или указано "all"/"none" - без лимита

#### run_all_cities.py:
```bash
python run_all_cities.py [--cities ГОРОД1 ГОРОД2 ...] [--max-results ЧИСЛО] [--output-dir ДИРЕКТОРИЯ] [--concurrency ЧИСЛО]
```

Параметры:
- `--cities` - список городов для парсинга (по умолчанию: все популярные)
- `--max-results` - максимум результатов на город (по умолчанию: без лимита)
- `--output-dir` - директория для сохранения результатов (по умолчанию: "results_all_cities")
- `--concurrency` - количество одновременных запросов к разным городам (по умолчанию: 3)

## Примеры городов

Популярные города:
- `new-york`
- `los-angeles`
- `chicago`
- `miami`
- `san-francisco`
- `boston`
- `seattle`
- `denver`
- `atlanta`
- `houston`

Штаты (для более широкого покрытия):
- `california`
- `texas`
- `florida`
- `new-york`
- `illinois`

Округа:
- `new-york-county-ny`
- `los-angeles-county-ca`
- `miami-dade-county-fl`

## Результаты

### run_full_parse.py
Результаты сохраняются в файл:
```
parsed_results_<город>_<дата>_<время>.json
```

### run_all_cities.py
Результаты сохраняются в директорию:
- По каждому городу отдельно: `<город>_<дата>_<время>.json`
- Общий файл со всеми результатами: `all_cities_<дата>_<время>.json`

## Особенности

- ✅ Асинхронная обработка для высокой скорости
- ✅ Правильная пагинация через параметр `start` в URL
- ✅ Grid-подход для параллельной обработки разных областей
- ✅ Автоматическое удаление дубликатов
- ✅ Сохранение HTML для отладки (каждые 20 объявлений)
- ✅ Детальная статистика по результатам

## Производительность

- Скорость: ~2-5 объявлений/сек (зависит от сети и сервера)
- Параллелизм: до 10 одновременных запросов внутри одного города
- Параллелизм городов: до 3 одновременных городов (настраивается)

## Структура данных

Каждое объявление содержит:
- `source_name` - источник ("compass")
- `listing_id` - ID объявления
- `listing_link` - ссылка на объявление
- `listing_type` - тип ("sale" или "lease")
- `listing_status` - статус ("Active", "Coming Soon", etc.)
- `address` - адрес
- `sale_price` / `lease_price` - цена
- `size` - площадь
- `property_description` - описание
- `listing_details` - детали (словарь)
- `photos` - список ссылок на фото
- `brochure_pdf` - ссылка на PDF брошюру
- `mls_number` - MLS номер
- `agents` - список агентов

## Примечания

- Парсер использует только API compass.com (без Selenium)
- Все запросы асинхронные для максимальной скорости
- HTML сохраняется периодически для отладки парсинга
- При ошибках парсер продолжает работу и логирует проблемы
