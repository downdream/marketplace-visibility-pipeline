import math
from typing import Any, Dict, List
from typing import Optional
import requests
import psycopg2
from psycopg2.extras import Json

from src.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from src.config_games import GAMES


BASE_URL = "https://sls.g2g.com/offer/search"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0",
}


def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def build_params(
    seo_term: str,
    sort: str,
    page: int,
    page_size: int = 48,
    currency: str = "EUR",
    country: str = "DE",
    filter_attr: Optional[str] = None,
) -> Dict[str, Any]:
    params = {
        "seo_term": seo_term,
        "sort": sort,
        "page": page,
        "page_size": page_size,
        "currency": currency,
        "country": country,
        "include_localization": 0,
        "v": "v2",
    }

    if filter_attr:
        params["filter_attr"] = filter_attr

    return params


def fetch_offers_page(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)

    if response.status_code == 400:
        print(f"Reached invalid page: {params.get('page')}. Stopping pagination.")
        return None

    response.raise_for_status()
    return response.json()


def extract_offers(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    return response_json.get("payload", {}).get("results", [])


def extract_total_results(response_json: Dict[str, Any]) -> Optional[int]:
    payload = response_json.get("payload", {})
    return (
        payload.get("total_results")
        or payload.get("total_offer")
        or payload.get("total")
        or payload.get("count")
    )


def insert_raw_offers(
    conn,
    offers: List[Dict[str, Any]],
    source: str,
    game_name: str,
    ) -> None:
    query = """
        INSERT INTO public.offers_raw (source, offer_id, raw_json, game_name, snapshot_date)
        VALUES (%s, %s, %s, %s, CURRENT_DATE)
    """
    with conn.cursor() as cursor:
        for offer in offers:
            cursor.execute(
                query,
                (
                    source,
                    offer.get("offer_id"),
                    Json(offer),
                    game_name,
                ),
            )
    conn.commit()


def fetch_and_store_all_pages(
    seo_term: str,
    sort: str,
    filter_attr: Optional[str],
    source: str,
    game_name: str,
    page_size: int = 48,
    currency: str = "EUR",
    country: str = "DE",
) -> int:
    conn = get_db_connection()
    total_inserted = 0

    try:
        first_page_params = build_params(
            seo_term=seo_term,
            sort=sort,
            filter_attr=filter_attr,
            page=1,
            page_size=page_size,
            currency=currency,
            country=country,
        )
        first_page_json = fetch_offers_page(first_page_params)

        if first_page_json is None:
            print("First page returned no valid data.")
            return total_inserted

        first_offers = extract_offers(first_page_json)
        total_results = extract_total_results(first_page_json)

        if total_results:
            max_pages = math.ceil(total_results / page_size)
            print(f"Detected total results: {total_results}")
            print(f"Detected max pages: {max_pages}")
        else:
            max_pages = None
            print("Could not detect total pages. Will stop on empty page or invalid page.")

        if first_offers:
            insert_raw_offers(conn, first_offers, source=source, game_name=game_name)
            total_inserted += len(first_offers)
            print(f"Inserted {len(first_offers)} offers from page 1")

        page = 2
        while True:
            if max_pages is not None and page > max_pages:
                break

            params = build_params(
                seo_term=seo_term,
                sort=sort,
                filter_attr=filter_attr,
                page=page,
                page_size=page_size,
                currency=currency,
                country=country,
            )
            page_json = fetch_offers_page(params)

            if page_json is None:
                print(f"Stopping at page {page}.")
                break

            offers = extract_offers(page_json)

            if not offers:
                print(f"No offers on page {page}. Stopping.")
                break

            insert_raw_offers(conn, offers, source=source, game_name=game_name)
            total_inserted += len(offers)
            print(f"Inserted {len(offers)} offers from page {page}")

            page += 1

    finally:
        conn.close()

    print(f"Total inserted: {total_inserted}")
    return total_inserted


def main():
    for game in GAMES:
        print(f"Running game: {game['name']}")
        fetch_and_store_all_pages(
            seo_term=game["seo_term"],
            sort=game["sort"],
            filter_attr=game["filter_attr"],
            source=game["source"],
            game_name=game["name"],
        )


if __name__ == "__main__":
    main()