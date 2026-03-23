import pandas as pd
from datetime import datetime
import time
import requests
import sys
import os
import json

### Add parents path in sys ###
current_dir = os.path.abspath('__file__')
while current_dir != os.path.dirname(current_dir):
    sys.path.append(current_dir)
    current_dir = os.path.dirname(current_dir)

### Add local path for testing ###
sys.path.append(r'A:\Corpa\python')

### Python scripts ###
from AuxScripts.db_insert_visibility import main
from AuxScripts.db_select import read_table

start_time = datetime.now()
today = datetime.today().strftime("%d.%m.%Y")
columns_list = ['keyword', 'product_link', 'offer_id', 'brand', 'product_title', 'rank', 'sponsored', 'platform_id', 'ean', 'date']  # Define column names 
result = pd.DataFrame(columns=columns_list)

'''Choosing if we crawl Monthly or Weekly VA'''
typ = 'Weekly'

'''Reading data from DB'''
data = read_table('public', 'keywords')
print(data)

def get_data(iteration, data):
    key = data.iloc[iteration, 3]
    print(key)
    api_key = "OGU0MzkxNGQ4YTA3NTk1YjcwOGIyMDMwOTgxNDMwM2YxOTMyMDVhODc2NTZkMjMxNjdmNDJkZTNhZDFlYTQ3OGF0dHJpYnV0ZXNUb1JldHJpZXZlPWJhY2tib3hfZ3JhZGVfbGFiZWwlMkNiYWNrYm94X2dyYWRlX3ZhbHVlJTJDYmFja21hcmtldElEJTJDYnJhbmQlMkNicmFuZF9jbGVhbiUyQ2NhbWVyYV9zY29yZSUyQ2NhdF9pZCUyQ2NhdGVnb3J5XzElMkNjYXRlZ29yeV8yJTJDY2F0ZWdvcnlfMyUyQ2NvbGxlY3Rpb25fYWklMkNjb2xvciUyQ2Nvbm5lY3RvciUyQ2N1cnJlbmN5JTJDZmFjZV9pZCUyQ2dsb2JhbF9zY29yZSUyQ2hkbWlfb3V0cHV0JTJDaWQlMkNpbWFnZTElMkNpcGFkX2Nvbm5lY3RvciUyQ2xpZmVfZXhwZWN0YW5jeV9zY29yZSUyQ2xpbmtfZ3JhZGVfdjIlMkNsaXN0X3ZpZXclMkNsaXN0aW5nSUQlMkNtZXJjaGFudF9pZCUyQ21vZGVsJTJDbW9kZWxfY2xlYW4lMkNtdWx0aW1lZGlhX3Njb3JlJTJDb2JqZWN0SUQlMkNwZXJmb3JtYW5jZXNfc2NvcmUlMkNwcmljZSUyQ3ByaWNlX25ldyUyQ3ByaWNlX3dpdGhfY3VycmVuY3klMkNwcmljZV9uZXdfd2l0aF9jdXJyZW5jeSUyQ3JlZmVyZW5jZVByaWNlJTJDcmV2aWV3UmF0aW5nJTJDc2NyZWVuX3F1YWxpdHlfc2NvcmUlMkNzaW1fbG9jayUyQ3NwZWNpYWxfb2ZmZXJfdHlwZSUyQ3N0b2NrUmF3JTJDc3ViX3RpdGxlX2VsZW1lbnRzJTJDdGl0bGUlMkN0aXRsZV9tb2RlbCUyQ3RvdWNoX2JhciUyQ3RvdWNoX2lkJTJDdmFyaWFudF9maWVsZHMlMkN3YXJyYW50eSZmaWx0ZXJzPU5PVCtiYWNrYm94X2dyYWRlX3ZhbHVlJTNEOSZyZXN0cmljdEluZGljZXM9cHJvZF8lMkE="
    app_id = "9X8ZUDUNN9"


    url = f"https://{app_id}.algolia.net/1/indexes/prod_index_backbox_de-de/query"

    # Query parameter (already included in the URL)
    query_param = {"x-algolia-agent": "Algolia for JavaScript (4.24.0); Browser"}

    # Payload data
    payload = {
        "query": f"{key}",
        "distinct": 1,
        "clickAnalytics": True,
        "filters": "(special_offer_type=0)",
        "facets": [
            "price", "page", "q", "sort", "brand", "model", "backbox_grade",
            "storage", "color", "year_date_release", "shipping_delay",
            "payment_methods", "warranty_with_unit", "keyboard_type_language",
            "price_ranges.sm-1", "price_ranges.sm-2", "price_ranges.md-1",
            "price_ranges.md-1b", "price_ranges.md-1c", "price_ranges.md-2",
            "price_ranges.lg-1", "price_ranges.lg-2", "price_ranges.lg-3"
        ],
        "page": 0,
        "hitsPerPage": 100
    }

    # Headers with API key and application ID
    headers = {
        "X-Algolia-Api-Key": api_key,
        "X-Algolia-Application-Id": app_id,
        "Content-Type": "application/x-www-form-urlencoded"  # Might not be necessary here
    }

    # Send POST request with query parameter in URL and payload in body
    response = requests.post(url, params=query_param, headers=headers, json=payload)

    if response.status_code == 200:
        try:
            data = response.json()
            print(data)
        except json.JSONDecodeError as e:
            print("Error parsing JSON:", e)
    else:
        print(f"Request failed with status code {response.status_code}")
        print(response.text)
    
    keyword, product_link, product_id, brand, product_title, rank, sponsored, platform, ean, date = [],[],[],[],[],[],[],[],[],[]
    
    articles = data['hits']
    count = 1
    for article in articles:
        keyword.append(payload['query'])
        pid = article['id']
        product_id.append(pid)
        product_link.append(f'https://www.backmarket.de/de-de/p/downdream/{pid}')
        brand.append(article['brand'])
        product_title.append(article['title'])
        sponsored.append('false')
        platform.append('2')
        ean.append(article['backmarketID'])
        rank.append(count)
        date.append(today)
        count += 1
    
    output = pd.DataFrame(list(zip(keyword, product_link, product_id, brand, product_title, rank, sponsored, platform, ean, date)), columns=columns_list)
    print(output)
    return output


'''Use the function above to crawl for all keywords'''
for iteration in range(len(data)):
    print("******************************************************************")
    print('Crawling on the {} keyword (total: {})'.format(iteration+1, len(data)))
    output = get_data(iteration,data)
    if not output.empty:
        result = pd.concat([result,output], ignore_index=True)
    time.sleep(1)

"""Connect to DB and upload result"""
result = result.drop_duplicates()
main(result,'Backmarket')


end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))