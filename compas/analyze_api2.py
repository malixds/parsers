import json

with open('htmls/api_response.json', encoding='utf-8') as f:
    data = json.load(f)

listings = data['lolResults']['data']
listing = listings[0]['listing']

print('=== STRUCTURED DATA ===')
sd = listing.get('structuredData', {})
print(f'structuredData type: {type(sd)}')
if isinstance(sd, dict):
    print(f'structuredData keys: {list(sd.keys())}')
    if 'singleFamilyResidence' in sd:
        sfr = sd['singleFamilyResidence']
        print(f'singleFamilyResidence type: {type(sfr)}')
        if isinstance(sfr, dict):
            print(f'  singleFamilyResidence keys: {list(sfr.keys())}')
            if 'address' in sfr:
                print(f'  address: {sfr["address"]}')
        else:
            print(f'  singleFamilyResidence: {sfr}')

print('\n=== LOCATION ===')
loc = listing.get('location', {})
print(f'location keys: {list(loc.keys())}')
print(f'location: {loc}')

print('\n=== SUBSTATS ===')
substats = listing.get('subStats', [])
for i, stat in enumerate(substats):
    print(f'  subStats[{i}]: {stat}')

print('\n=== FOOTER ===')
footer = listing.get('footer', [])
for i, item in enumerate(footer):
    print(f'  footer[{i}]: {item}')

print('\n=== MEDIA ===')
media = listing.get('media', [])
if media:
    print(f'  media[0]: {media[0]}')

print('\n=== CLUSTER SUMMARY ===')
cs = listing.get('clusterSummary', {})
print(f'clusterSummary keys: {list(cs.keys())}')
if 'priceRange' in cs:
    print(f'  priceRange: {cs["priceRange"]}')
if 'propertyType' in cs:
    print(f'  propertyType: {cs["propertyType"]}')

print('\n=== PAGE LINK ===')
print(f'pageLink: {listing.get("pageLink", "N/A")}')
print(f'navigationPageLink: {listing.get("navigationPageLink", "N/A")}')

