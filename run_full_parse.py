"""
Скрипт для полного парсинга всех объявлений с rwholmes.com
Сохраняет результаты в JSON файл
"""
import asyncio
import json
import logging
from datetime import datetime

import httpx
from rwholmes import RwholmesParser

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_full_parse():
    """Полный парсинг всех объявлений"""
    print("\n" + "=" * 70)
    print("🚀 ПОЛНЫЙ ПАРСИНГ ВСЕХ ОБЪЯВЛЕНИЙ RWHOLMES.COM")
    print("=" * 70)
    
    start_time = datetime.now()
    output_file = f"parsed_results_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Создаем парсер с оптимальной конкурентностью
        parser = RwholmesParser(client, concurrency=10, source_name="rwholmes")
        
        print(f"\n⏱️  Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📁 Результаты будут сохранены в: {output_file}\n")
        
        # Запускаем парсинг
        results = await parser.run()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print("📊 СТАТИСТИКА")
        print("=" * 70)
        print(f"✅ Обработано объявлений: {len(results)}")
        print(f"⏱️  Время выполнения: {duration:.2f} секунд ({duration/60:.2f} минут)")
        print(f"📈 Скорость: {len(results)/duration:.2f} объявлений/сек")
        
        if results:
            # Статистика по полям
            with_address = sum(1 for r in results if r.address and r.address != "Address not found")
            with_price = sum(1 for r in results if r.sale_price or r.lease_price)
            with_description = sum(1 for r in results if r.property_description)
            with_photos = sum(1 for r in results if r.photos and len(r.photos) > 0)
            with_mls = sum(1 for r in results if r.mls_number)
            
            print(f"\n📋 Заполненность полей:")
            print(f"  - С адресом: {with_address}/{len(results)} ({with_address/len(results)*100:.1f}%)")
            print(f"  - С ценой: {with_price}/{len(results)} ({with_price/len(results)*100:.1f}%)")
            print(f"  - С описанием: {with_description}/{len(results)} ({with_description/len(results)*100:.1f}%)")
            print(f"  - С фото: {with_photos}/{len(results)} ({with_photos/len(results)*100:.1f}%)")
            print(f"  - С MLS номером: {with_mls}/{len(results)} ({with_mls/len(results)*100:.1f}%)")
            
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
            print(f"📊 Размер: {len(json.dumps([r.model_dump() for r in results], default=str)) / 1024 / 1024:.2f} MB")
            
            # Показываем пример первого результата
            print(f"\n📄 Пример первого объявления:")
            first = results[0]
            print(f"  - ID: {first.listing_id}")
            print(f"  - Address: {first.address}")
            print(f"  - Type: {first.listing_type}")
            print(f"  - Status: {first.listing_status}")
            print(f"  - Link: {first.listing_link}")
        
        print("\n" + "=" * 70)
        print("✅ ПАРСИНГ ЗАВЕРШЕН!")
        print("=" * 70)
        
        return results


if __name__ == '__main__':
    try:
        results = asyncio.run(run_full_parse())
        print(f"\n🎉 Все готово! Обработано {len(results)} объявлений.")
    except KeyboardInterrupt:
        print("\n\n⚠️  Парсинг прерван пользователем")
    except Exception as e:
        print(f"\n\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

