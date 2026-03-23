from datetime import datetime
import time
from typing import Dict, List

import pandas as pd
import requests

from src.database.db_insert_visibility import main as insert_visibility_data
from src.database.db_select import read_table


APP_ID = "9X8ZUDUNN9"
API_KEY = "REPLACE_ME"
PLATFORM_NAME = "Backmarket"
PLATFORM_ID = 2
HITS_PER_PAGE = 100
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


def build_request_payload(keyword: str) -> Dict:
    return {
        "query": keyword,
        "distinct": 1,
        "clickAnalytics": True,
        "filters": "(special_offer_type=0)",
        "facets": [
            "price", "page", "q", "sort", "brand", "model", "backbox_grade",
            "storage", "color", "year_date_release", "shipping_delay",
            "payment_methods", "warranty_with_unit", "keyboard_type_language",
            "price_ranges.sm-1", "price_ranges.sm-2", "price_ranges.md-1",
            "price_ranges.md-1b", "price_ranges.md-1c", "price_ranges.md-2",
            "price_ranges.lg-1", "price_ranges.lg-2", "price_ranges.lg-3",
        ],
        "page": 0,
        "hitsPerPage": HITS_PER_PAGE,
    }


def fetch_backmarket_results(keyword: str) -> Dict:
    url = f"https://{APP_ID}.algolia.net/1/indexes/prod_index_backbox_de-de/query"
    params = {"x-algolia-agent": "Algolia for JavaScript (4.24.0); Browser"}
    headers = {
        "X-Algolia-Api-Key": API_KEY,
        "X-Algolia-Application-Id": APP_ID,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = build_request_payload(keyword)
    response = requests.post(url, params=params, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def transform_hits_to_dataframe(keyword: str, response_data: Dict) -> pd.DataFrame:
    rows: List[Dict] = []
    hits = response_data.get("hits", [])

    for rank, article in enumerate(hits, start=1):
        product_id = article.get("id")
        rows.append(
            {
                "keyword": keyword,
                "product_link": f"https://www.backmarket.de/de-de/p/downdream/{product_id}",
                "offer_id": product_id,
                "brand": article.get("brand"),
                "product_title": article.get("title"),
                "rank": rank,
                "sponsored": "false",
                "platform_id": PLATFORM_ID,
                "ean": article.get("backmarketID"),
                "date": TODAY,
            }
        )

    return pd.DataFrame(rows, columns=COLUMNS)


def run_weekly_visibility() -> pd.DataFrame:
    keywords_df = load_keywords()
    result = pd.DataFrame(columns=COLUMNS)

    for iteration in range(len(keywords_df)):
        print("******************************************************************")
        print(f'Crawling keyword {iteration + 1} of {len(keywords_df)}')

        keyword = keywords_df.iloc[iteration, 3]
        print(f"Keyword: {keyword}")

        try:
            response_data = fetch_backmarket_results(keyword)
            output = transform_hits_to_dataframe(keyword, response_data)
            if not output.empty:
                result = pd.concat([result, output], ignore_index=True)
        except Exception as err:
            print(f"Failed for keyword '{keyword}': {err}")

        time.sleep(1)

    return result.drop_duplicates()


def main() -> None:
    start_time = datetime.now()
    result = run_weekly_visibility()

    if not result.empty:
        insert_visibility_data(result, PLATFORM_NAME)

    end_time = datetime.now()
    print(f"Duration: {end_time - start_time}")


if __name__ == "__main__":
    main()