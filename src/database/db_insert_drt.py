from datetime import datetime
from typing import Any, Optional

from src.database.db_connection import get_db_connection


TABLE_MAP = {
    "amazon": "Amazon_Vendor",
    "bol": "Bol",
    "but": "But",
    "cdiscount": "Cdiscount",
    "check24": "Check24",
    "conforama": "Conforama",
    "darty": "Darty",
    "home24_de": "Home24_DE",
    "home24_at": "Home24_AT",
    "kaufland_de": "Kaufland_DE",
    "mdm": "MaisonsDuMonde",
    "otto": "Otto",
}


def _normalize_date(date_value: str) -> str:
    day, month, year = date_value.split(".")
    if len(year) == 2:
        year = f"20{year}"
    return f"{year}-{month}-{day}"


def _empty_to_none(value: Any) -> Optional[Any]:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _to_int_or_none(value: Any) -> Optional[int]:
    value = _empty_to_none(value)
    if value is None:
        return None
    return int(value)


def insert_data_into_table(conn, dataframe, mp: str) -> None:
    schema = TABLE_MAP[mp.lower()]
    cursor = conn.cursor()

    print(f'-- IN INSERT: Inserting into "{schema}"."DRT" ---')

    amazon_query = """
        INSERT INTO "Amazon_Vendor"."DRT"
        (date, offer_id, platform_id, url, review, score, rank1, rank2, ean)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    default_query = f'''
        INSERT INTO "{schema}"."DRT"
        (date, offer_id, platform_id, url, review, score, ean)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''

    for _, row in dataframe.iterrows():
        date_value = _normalize_date(row["date"])
        url = _empty_to_none(row.get("url"))
        offer = row.get("offer_id")
        platform = row.get("platform_id")
        review = row.get("review")
        score = row.get("score")
        ean = _empty_to_none(row.get("ean"))

        try:
            if mp.lower() == "amazon":
                values = (
                    date_value,
                    offer,
                    platform,
                    url,
                    review,
                    score,
                    _to_int_or_none(row.get("rank1")),
                    _to_int_or_none(row.get("rank2")),
                    ean,
                )
                cursor.execute(amazon_query, values)
            else:
                values = (
                    date_value,
                    offer,
                    platform,
                    url,
                    review,
                    score,
                    ean,
                )
                cursor.execute(default_query, values)
        except Exception as err:
            print("Insert failed for row:")
            print(row)
            print(err)
            break

    conn.commit()
    cursor.close()


def main(dataframe, mp: str, db_name: str = "software_dev") -> None:
    print("-- IN MAIN --")
    start = datetime.now()

    conn = get_db_connection(dbname=db_name)
    try:
        insert_data_into_table(conn, dataframe, mp)
    finally:
        conn.close()

    end = datetime.now()
    print("--------------------------------------------------------------------")
    print(f"Duration: {end - start}")