import datetime
from typing import Any

from src.database.db_connection import get_db_connection


def _normalize_date(date_value: str) -> str:
    day, month, year = date_value.split(".")
    return f"{year}-{month}-{day}"


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).encode("utf-8", errors="ignore").decode("utf-8").strip()


def insert_data_into_table(conn, dataframe, mp: str) -> None:
    schema = mp
    table_name = f'{schema}_va'
    cursor = conn.cursor()

    print(f'-- IN INSERT: Inserting into "{schema}"."{table_name}" ---')

    insert_query = f'''
        INSERT INTO "{schema}"."{table_name}"
        (keyword, url, offer_id, seller, title, rank, sponsored, platform_id, ean, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    for _, row in dataframe.iterrows():
        date_value = _normalize_date(row["date"])
        keyword = _safe_text(row.get("keyword"))
        offer = row.get("offer_id")
        platform = row.get("platform_id")
        url = row.get("product_link")
        title = _safe_text(row.get("product_title")).replace("'", "`")

        seller_raw = row.get("seller")
        if seller_raw is not None:
            seller = _safe_text(seller_raw).split("(")[0].strip()
        else:
            seller = _safe_text(row.get("brand"))
        seller = seller.replace("'", "''")

        rank = row.get("rank")
        ean = row.get("ean")
        sponsored = row.get("sponsored", "")

        values = (
            keyword,
            url,
            offer,
            seller,
            title,
            rank,
            sponsored,
            platform,
            ean,
            date_value,
        )

        try:
            cursor.execute(insert_query, values)
        except Exception as err:
            print("Insert failed for row:")
            print(values)
            print(err)
            break

    conn.commit()
    cursor.close()


def main(dataframe, mp: str, db_name: str = "postgres") -> None:
    print("-- IN MAIN --")
    start = datetime.datetime.now()

    conn = get_db_connection(dbname=db_name)
    try:
        insert_data_into_table(conn, dataframe, mp)
    finally:
        conn.close()

    end = datetime.datetime.now()
    print("--------------------------------------------------------------------")
    print(f"Duration: {end - start}")