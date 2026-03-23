from datetime import datetime
import json
import time
from typing import Dict, Optional

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from src.database.db_insert_visibility import main as insert_visibility_data
from src.database.db_select import read_table


PLATFORM_NAME = "Kaufland"
PLATFORM_ID = 1
TODAY = datetime.today().strftime("%d.%m.%Y")
COLUMNS = [
    "keyword",
    "product_link",
    "offer_id",
    "brand",
    "product_title",
    "rank",
    "sponsored",
    "platform_id",
    "ean",
    "date",
]


def load_keywords() -> pd.DataFrame:
    return read_table("public", "keywords")


def fetch_with_requests(uri: str, headers: Dict) -> Optional[Dict]:
    try:
        response = requests.get(uri, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 403:
            print("Blocked with 403, switching to Selenium fallback.")
            return None
        print(f"Unexpected status code: {response.status_code}")
        return None
    except requests.RequestException as err:
        print(f"Request error: {err}")
        return None


def fetch_with_selenium(uri: str, driver_path: str) -> Optional[Dict]:
    try:
        options = webdriver.ChromeOptions()
        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(uri)
        raw_json = driver.find_element(By.TAG_NAME, "pre").get_attribute("innerHTML")
        driver.quit()
        return json.loads(raw_json)
    except Exception as err:
        print(f"Selenium fallback failed: {err}")
        return None


def fetch_kaufland_results(uri: str, headers: Dict, driver_path: Optional[str] = None) -> Optional[Dict]:
    data = fetch_with_requests(uri, headers)
    if data is not None:
        return data

    if driver_path:
        return fetch_with_selenium(uri, driver_path)

    return None


def build_search_url(keyword: str, page: int) -> str:
    return (
        "https://www.kaufland.de/api/search/v2/products/"
        f"?requestType=load&productOffset=100&loadType=pagination&page={page}"
        f"&pageType=search&searchValue={keyword}&deviceType=desktop&useNewUrls=true"
    )


def transform_products(keyword: str, articles: Dict) -> pd.DataFrame:
    rows = []
    products = articles.get("products", [])

    for product in products:
        try:
            pid = str(int(product["id"]))
        except Exception:
            pid = str(product.get("id", ""))

        try:
            ean = str(int(product["ean"]))
        except Exception:
            ean = str(product.get("ean", ""))

        try:
            tag = product["tags"][0]
            sponsored = "true" if tag == "sponsored" else "false"
        except Exception:
            sponsored = "false"

        try:
            brand = product["manufacturer"]["title"]
        except Exception:
            brand = "-"

        rows.append(
            {
                "keyword": keyword,
                "product_link": f"https://www.kaufland.de/product/{pid}",
                "offer_id": pid,
                "brand": brand,
                "product_title": product.get("title"),
                "rank": 0,
                "sponsored": sponsored,
                "platform_id": PLATFORM_ID,
                "ean": ean,
                "date": TODAY,
            }
        )

    output = pd.DataFrame(rows, columns=COLUMNS)

    rank_value = 1
    for i in range(len(output)):
        if output.loc[i, "sponsored"] == "false":
            output.loc[i, "rank"] = rank_value
            rank_value += 1

    return output


def get_data_for_keyword(keyword: str, driver_path: Optional[str] = None) -> pd.DataFrame:
    headers = {
        "Cookie": "REPLACE_ME"
    }

    all_results = pd.DataFrame(columns=COLUMNS)
    page = 1
    total_products = 0

    while total_products < 100:
        uri = build_search_url(keyword, page)
        print(uri)

        articles = fetch_kaufland_results(uri, headers, driver_path=driver_path)
        if not articles:
            print("Failed to retrieve product data.")
            break

        try:
            results_count = articles["page"]["trackingData"]["search"]["results_number"]
            print(f"{results_count} results")
        except KeyError:
            print("Missing result count in response.")
            break

        output = transform_products(keyword, articles)
        if output.empty:
            break

        all_results = pd.concat([all_results, output], ignore_index=True)
        total_products = len(all_results)

        if total_products >= results_count:
            break

        print(f"Collected products: {total_products}")
        page += 1

    return all_results.head(100)


def run_weekly_visibility(driver_path: Optional[str] = None) -> pd.DataFrame:
    keywords_df = load_keywords()
    result = pd.DataFrame(columns=COLUMNS)

    for iteration in range(len(keywords_df)):
        print("******************************************************************")
        print(f'Crawling keyword {iteration + 1} of {len(keywords_df)}')

        keyword = keywords_df.iloc[iteration, 3]
        print(f"Keyword: {keyword}")

        output = get_data_for_keyword(keyword, driver_path=driver_path)
        if not output.empty:
            result = pd.concat([result, output], ignore_index=True)

        time.sleep(1)

    return result.drop_duplicates()


def main() -> None:
    start_time = datetime.now()

    # Example: driver_path="C:/path/to/chromedriver.exe"
    result = run_weekly_visibility(driver_path=None)

    if not result.empty:
        insert_visibility_data(result, PLATFORM_NAME)

    end_time = datetime.now()
    print(f"Duration: {end_time - start_time}")


if __name__ == "__main__":
    main()