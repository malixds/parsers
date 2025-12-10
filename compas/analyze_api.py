import json

with open('htmls/api_response.json', encoding='utf-8') as f:
    data = json.load(f)

listings = data['lolResults']['data']
print(f'Всего объявлений: {len(listings)}')

if listings:
    listing = listings[0]['listing']
    print('\nКлючи в listing:')
    print(list(listing.keys())[:40])
    
    print('\nПримеры значений:')
    print(f'listingIdSHA: {listing.get("listingIdSHA", "N/A")}')
    
    loc = listing.get('location', {})
    print(f'location type: {type(loc)}')
    if isinstance(loc, dict):
        print(f'  prettyAddress: {loc.get("prettyAddress", "N/A")}')
        print(f'  seoId: {loc.get("seoId", "N/A")}')
    
    price = listing.get('price', {})
    print(f'price type: {type(price)}')
    if isinstance(price, dict):
        print(f'  listed: {price.get("listed", "N/A")}')
        print(f'  lastKnown: {price.get("lastKnown", "N/A")}')
    
    media = listing.get('media', [])
    print(f'media count: {len(media)}')
    if media:
        print(f'  first media type: {media[0].get("type", "N/A")}')
        print(f'  first media url: {media[0].get("url", "N/A")[:80]}...')
    
    print(f'\nlistingType: {listing.get("listingType", "N/A")}')
    print(f'localizedStatus: {listing.get("localizedStatus", "N/A")}')
    print(f'description: {listing.get("description", "N/A")[:100] if listing.get("description") else "N/A"}...')
    
    size = listing.get('size', {})
    if isinstance(size, dict):
        print(f'size: {size}')
    
    print(f'\nВсе ключи listing (полный список):')
    for key in sorted(listing.keys()):
        val = listing[key]
        if isinstance(val, dict):
            print(f'  {key}: dict с ключами {list(val.keys())[:5]}')
        elif isinstance(val, list):
            print(f'  {key}: list из {len(val)} элементов')
        else:
            print(f'  {key}: {type(val).__name__} = {str(val)[:50]}')

