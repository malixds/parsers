"""Тест парсера через API"""
from compass import CompassParser

parser = CompassParser()
print("Тестирую получение объявлений через API...")
urls = parser.get_listing_urls_from_api("new-york", max_results=50)

print(f"\n✅ Получено {len(urls)} ссылок")
print("\nПервые 5 ссылок:")
for i, url in enumerate(urls[:5], 1):
    print(f"  {i}. {url}")

