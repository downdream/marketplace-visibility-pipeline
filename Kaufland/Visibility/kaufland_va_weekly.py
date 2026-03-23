import pandas as pd
from datetime import datetime
import time
import requests
import sys
import os
import json
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

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

'''The function for checking wether the backend link blocked or not(need more improvement with the logic)'''
def test_backend(uri, headers, payload, retries=3):
    
    for _ in range(retries + 1):
        try:
            # Attempt using requests for efficiency
            articles = requests.request("GET", uri, headers=headers, data=payload)
            if articles.status_code == 200:
                articles = articles.json()
                return articles

            # Handle expected errors gracefully and switch to Selenium
            if articles.status_code == 403:
                print(f'Error (API): {articles}')
                break  # Exit the loop to switch to Selenium
        except requests.exceptions.RequestException as e:
            print(f'Error using requests: {e}')

    # Use Selenium if requests consistently fail
    try:
        driver_loca = r'A:\Corpa\chromedriver'
        op = webdriver.ChromeOptions()
        service = Service(executable_path=r'{}\chromedriver.exe'.format(driver_loca))
        driver = webdriver.Chrome(service=service,options=op)
        driver.get(uri)
        articles = driver.find_element(By.TAG_NAME, 'pre').get_attribute('innerHTML')
        articles = json.loads(articles)
        return articles
    except Exception as e:
        print(f'Error using Selenium: {e}')  
                
               
'''The function for crawling 1 keyword'''
def get_data(iteration, data):
    key = data.iloc[iteration, 3]
    print(key)
    headers = {'Cookie': 'x-country=DE; x-storefront=de; __cf_bm=W8VTqaff5UIwEOFZ6zNIRJ7G1sHrF5Aw.coBWjHSW0I-1733833483-1.0.1.1-iirmuGM7WkzCJ.xokmE1P.5OL8SqHKzWVI3dQx5H17ETz_8CISLkJUB9Afdv_xSMIocLn7vnAZq7Ui4T1Tsu2w'}
    payload = {}
    page = 1
    keyword, product_link, product_id, brand, product_title, rank, sponsored, platform, ean, date = [],[],[],[],[],[],[],[],[],[]

    while len(product_id) < 100:
        uri = 'https://www.kaufland.de/api/search/v2/products/?requestType=load&productOffset=100&loadType=pagination&page={}&pageType=search&searchValue={}&deviceType=desktop&useNewUrls=true'.format(page, key)
        print(uri)

        articles = test_backend(uri, headers, payload)

        if not articles:
            print('Failed to retrieve product data.')
            break

        try:
            results_n = articles['page']['trackingData']['search']['results_number']
            print(f'{results_n} results')

            if len(product_id) == results_n:
                break

            products = articles['products']
            for product in products:
                platform.append(1)
                date.append(today) 
                keyword.append(key)
                rank.append(0)
                try:
                    pid = str(int(product['id']))
                except:
                    pid = str(product['id'])
                product_id.append(pid)
                ean.append(str(int(product['ean'])))
                product_link.append("https://www.kaufland.de/product/" + pid)
                product_title.append(product['title'])
                try:
                    tag = product['tags'][0]
                    sponsored.append('true' if tag == 'sponsored' else 'false')
                except:
                    sponsored.append('false')
                try:
                    brand.append(product['manufacturer']['title'])
                except:
                    brand.append('-')
                
            print(f'len(product_id) = {len(product_id)}')
            page += 1

        except KeyError:
            print('Missing key in product data.')

    output = pd.DataFrame(list(zip(keyword, product_link, product_id, brand, product_title, rank, sponsored, platform, ean, date)), columns=columns_list)
    rank = 1
    for i in range(len(output)):
        if output.loc[i, 'sponsored'] == 'false':
            output.loc[i, 'rank'] = rank
            rank += 1
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
main(result,'Kaufland')


end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))
