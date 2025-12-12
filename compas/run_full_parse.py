"""
Скрипт для полного парсинга всех объявлений с compass.com
Сохраняет результаты в JSON файл
"""
import asyncio
import json
import logging
import sys
from datetime import datetime

from compass import CompassParser

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_full_parse(location: str = "new-york", max_results: int = 1000):
    """Полный парсинг всех объявлений (асинхронно)"""
    print("\n" + "=" * 70)
    print("🚀 ПОЛНЫЙ ПАРСИНГ ВСЕХ ОБЪЯВЛЕНИЙ COMPASS.COM (API - ASYNC)")
    print("=" * 70)
    
    start_time = datetime.now()
    output_file = f"parsed_results_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
    
    # Создаем парсер (использует только API через httpx)
    parser = CompassParser(concurrency=10)
    
    print(f"\n⏱️  Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📍 Локация: {location}")
    print(f"📊 Максимум результатов: {max_results}")
    print(f"🚀 Concurrency: {parser.concurrency}")
    print(f"📁 Результаты будут сохранены в: {output_file}\n")
    
    try:
        # Запускаем парсинг через API (асинхронно)
        results = await parser.run(location=location, max_results=max_results)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print("📊 СТАТИСТИКА")
        print("=" * 70)
        print(f"✅ Обработано объявлений: {len(results)}")
        print(f"⏱️  Время выполнения: {duration:.2f} секунд ({duration/60:.2f} минут)")
        if duration > 0:
            print(f"📈 Скорость: {len(results)/duration:.2f} объявлений/сек")
        
        if results:
            # Статистика по полям
            with_address = sum(1 for r in results if r.address and r.address != "Address not found")
            with_price = sum(1 for r in results if r.sale_price or r.lease_price)
            with_description = sum(1 for r in results if r.property_description)
            with_photos = sum(1 for r in results if r.photos and len(r.photos) > 0)
            
            print(f"\n📋 Заполненность полей:")
            print(f"  - С адресом: {with_address}/{len(results)} ({with_address/len(results)*100:.1f}%)")
            print(f"  - С ценой: {with_price}/{len(results)} ({with_price/len(results)*100:.1f}%)")
            print(f"  - С описанием: {with_description}/{len(results)} ({with_description/len(results)*100:.1f}%)")
            print(f"  - С фото: {with_photos}/{len(results)} ({with_photos/len(results)*100:.1f}%)")
            
            # Статистика по типам
            sale_count = sum(1 for r in results if r.listing_type == 'sale')
            lease_count = sum(1 for r in results if r.listing_type == 'lease')
            print(f"\n🏷️  Типы объявлений:")
            print(f"  - For Sale: {sale_count}")
            print(f"  - For Lease: {lease_count}")
            
            # Сохраняем результаты
            print(f"\n💾 Сохраняю результаты в {output_file}...")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(
                    [r.model_dump() for r in results],
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=str
                )
            
            print(f"✅ Результаты успешно сохранены!")
            print(f"📁 Файл: {output_file}")
            file_size = len(json.dumps([r.model_dump() for r in results], default=str)) / 1024 / 1024
            print(f"📊 Размер: {file_size:.2f} MB")
            
            # Показываем пример первого результата
            print(f"\n📄 Пример первого объявления:")
            first = results[0]
            print(f"  - ID: {first.listing_id}")
            print(f"  - Address: {first.address}")
            print(f"  - Type: {first.listing_type}")
            print(f"  - Status: {first.listing_status}")
            print(f"  - Price: {first.sale_price or first.lease_price or 'N/A'}")
            print(f"  - Link: {first.listing_link}")
    
    except Exception as e:
        print(f"\n\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 70)
    print("✅ ПАРСИНГ ЗАВЕРШЕН!")
    print("=" * 70)
    
    return results


if __name__ == '__main__':
    import sys
    
    location = "new-york"
    max_results = 1000
    
    if len(sys.argv) > 1:
        location = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            max_results = int(sys.argv[2])
        except ValueError:
            print(f"⚠️  Неверное значение max_results: {sys.argv[2]}, использую 1000")
    
    try:
        asyncio.run(run_full_parse(location=location, max_results=max_results))
    except KeyboardInterrupt:
        print("\n\n⚠️  Парсинг прерван пользователем")
