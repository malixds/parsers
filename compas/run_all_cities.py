"""
Скрипт для парсинга всех городов compass.com
Обходит список городов и собирает все объявления
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from compass import CompassParser

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Список популярных городов США для парсинга
POPULAR_CITIES = [
    # Крупные города
    "new-york",
    "los-angeles",
    "chicago",
    "houston",
    "phoenix",
    "philadelphia",
    "san-antonio",
    "san-diego",
    "dallas",
    "san-jose",
    "austin",
    "jacksonville",
    "fort-worth",
    "columbus",
    "charlotte",
    "san-francisco",
    "indianapolis",
    "seattle",
    "denver",
    "washington",
    "boston",
    "el-paso",
    "detroit",
    "nashville",
    "portland",
    "oklahoma-city",
    "las-vegas",
    "memphis",
    "louisville",
    "baltimore",
    "milwaukee",
    "albuquerque",
    "tucson",
    "fresno",
    "sacramento",
    "kansas-city",
    "mesa",
    "atlanta",
    "omaha",
    "colorado-springs",
    "raleigh",
    "virginia-beach",
    "miami",
    "oakland",
    "minneapolis",
    "tulsa",
    "cleveland",
    "wichita",
    "arlington",
    "new-orleans",
    "honolulu",
    # Штаты (для более широкого покрытия)
    "california",
    "texas",
    "florida",
    "new-york",
    "illinois",
    "pennsylvania",
    "ohio",
    "georgia",
    "north-carolina",
    "michigan",
    "new-jersey",
    "virginia",
    "washington",
    "arizona",
    "massachusetts",
    "tennessee",
    "indiana",
    "missouri",
    "maryland",
    "wisconsin",
    "colorado",
    "minnesota",
    "south-carolina",
    "alabama",
    "louisiana",
    "kentucky",
    "oregon",
    "oklahoma",
    "connecticut",
    "utah",
    "iowa",
    "nevada",
    "arkansas",
    "mississippi",
    "kansas",
    "new-mexico",
    "nebraska",
    "west-virginia",
    "idaho",
    "hawaii",
    "new-hampshire",
    "maine",
    "montana",
    "rhode-island",
    "delaware",
    "south-dakota",
    "north-dakota",
    "alaska",
    "vermont",
    "wyoming",
]

# Можно также добавить округа (counties)
POPULAR_COUNTIES = [
    "new-york-county-ny",
    "los-angeles-county-ca",
    "cook-county-il",
    "harris-county-tx",
    "maricopa-county-az",
    "san-diego-county-ca",
    "orange-county-ca",
    "miami-dade-county-fl",
    "dallas-county-tx",
    "king-county-wa",
    "san-francisco-county-ca",
    "broward-county-fl",
    "riverside-county-ca",
    "wayne-county-mi",
    "clark-county-nv",
]


async def parse_city(parser: CompassParser, location: str, max_results: int | None = None) -> list:
    """Парсит один город"""
    try:
        logger.info(f"📍 Начинаю парсинг: {location}")
        results = await parser.run(location=location, max_results=max_results or 999999999)
        logger.info(f"✅ {location}: получено {len(results)} объявлений")
        return results
    except Exception as e:
        logger.error(f"❌ Ошибка при парсинге {location}: {e}")
        import traceback
        traceback.print_exc()
        return []


async def run_all_cities(
    cities: list[str] | None = None,
    max_results_per_city: int | None = None,
    output_dir: str = "results_all_cities",
    concurrency: int = 3  # Одновременно обрабатываем несколько городов
):
    """
    Парсит все города из списка
    
    Args:
        cities: Список городов для парсинга. Если None - использует POPULAR_CITIES
        max_results_per_city: Максимум результатов на город. Если None - без лимита
        output_dir: Директория для сохранения результатов
        concurrency: Количество одновременных запросов к разным городам
    """
    if cities is None:
        cities = POPULAR_CITIES
    
    print("\n" + "=" * 70)
    print("🌍 ПАРСИНГ ВСЕХ ГОРОДОВ COMPASS.COM")
    print("=" * 70)
    print(f"📍 Всего городов: {len(cities)}")
    if max_results_per_city is None:
        print(f"📊 Лимит на город: БЕЗ ЛИМИТА")
    else:
        print(f"📊 Лимит на город: {max_results_per_city}")
    print(f"🚀 Concurrency: {concurrency}")
    print(f"📁 Результаты будут сохранены в: {output_dir}/")
    print("=" * 70 + "\n")
    
    # Создаем директорию для результатов
    Path(output_dir).mkdir(exist_ok=True)
    
    # Создаем парсер
    parser = CompassParser(concurrency=10)
    
    start_time = datetime.now()
    all_results = []
    city_stats = {}
    
    # Создаем семафор для ограничения параллельных запросов к разным городам
    city_semaphore = asyncio.Semaphore(concurrency)
    
    async def parse_with_semaphore(location: str):
        async with city_semaphore:
            results = await parse_city(parser, location, max_results_per_city)
            city_stats[location] = len(results)
            return results
    
    # Обрабатываем города параллельно
    tasks = [parse_with_semaphore(city) for city in cities]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Собираем результаты
    for idx, result in enumerate(results_list):
        if isinstance(result, Exception):
            logger.error(f"❌ Ошибка при обработке города {cities[idx]}: {result}")
            city_stats[cities[idx]] = 0
        else:
            all_results.extend(result)
            # Сохраняем результаты по каждому городу отдельно
            if result:
                city_file = os.path.join(output_dir, f"{cities[idx]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(city_file, 'w', encoding='utf-8') as f:
                    json.dump(
                        [r.model_dump() for r in result],
                        f,
                        indent=2,
                        ensure_ascii=False,
                        default=str
                    )
                logger.info(f"💾 Сохранено {len(result)} объявлений для {cities[idx]}: {city_file}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Сохраняем общий файл со всеми результатами
    all_results_file = os.path.join(output_dir, f"all_cities_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
    with open(all_results_file, 'w', encoding='utf-8') as f:
        json.dump(
            [r.model_dump() for r in all_results],
            f,
            indent=2,
            ensure_ascii=False,
            default=str
        )
    
    # Статистика
    print("\n" + "=" * 70)
    print("📊 ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 70)
    print(f"✅ Всего обработано городов: {len(cities)}")
    print(f"✅ Всего собрано объявлений: {len(all_results)}")
    print(f"⏱️  Время выполнения: {duration:.2f} секунд ({duration/60:.2f} минут, {duration/3600:.2f} часов)")
    if duration > 0:
        print(f"📈 Скорость: {len(all_results)/duration:.2f} объявлений/сек")
    
    print(f"\n📋 Статистика по городам (топ-10):")
    sorted_stats = sorted(city_stats.items(), key=lambda x: x[1], reverse=True)
    for city, count in sorted_stats[:10]:
        print(f"  - {city}: {count} объявлений")
    
    print(f"\n💾 Общий файл со всеми результатами: {all_results_file}")
    file_size = len(json.dumps([r.model_dump() for r in all_results], default=str)) / 1024 / 1024
    print(f"📊 Размер файла: {file_size:.2f} MB")
    
    print("\n" + "=" * 70)
    print("✅ ПАРСИНГ ВСЕХ ГОРОДОВ ЗАВЕРШЕН!")
    print("=" * 70)
    
    return all_results


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Парсинг всех городов compass.com')
    parser.add_argument('--cities', nargs='+', help='Список городов для парсинга (по умолчанию: все популярные)')
    parser.add_argument('--max-results', type=int, default=None, help='Максимум результатов на город (по умолчанию: без лимита)')
    parser.add_argument('--output-dir', type=str, default='results_all_cities', help='Директория для сохранения результатов')
    parser.add_argument('--concurrency', type=int, default=3, help='Количество одновременных запросов к разным городам')
    
    args = parser.parse_args()
    
    try:
        asyncio.run(run_all_cities(
            cities=args.cities,
            max_results_per_city=args.max_results,
            output_dir=args.output_dir,
            concurrency=args.concurrency
        ))
    except KeyboardInterrupt:
        print("\n\n⚠️  Парсинг прерван пользователем")

