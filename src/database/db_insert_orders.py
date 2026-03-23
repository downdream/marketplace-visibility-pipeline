from datetime import datetime
from typing import Optional

import psycopg2

from src.database.db_connection import get_db_connection


def _parse_date(date_str: str) -> str:
    return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")


def _parse_optional_date(date_str) -> Optional[str]:
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str), "%d.%m.%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def delete_all_rows_from_table(conn, mp: str) -> None:
    with conn, conn.cursor() as cursor:
        query = f'DELETE FROM "{mp}"."ORDERS_TEST"'
        cursor.execute(query)


def insert_into_table(mp: str, conn, dataframe) -> None:
    cursor = conn.cursor()
    print(f'IN INSERT {mp}')
    print(len(dataframe))

    insert_query = f'''
        INSERT INTO "{mp}"."ORDERS_TEST"
        (
            platform_id, marketplace, order_reference, order_lines, creation_date,
            sku, offer_id, quantity, unit_price, total_revenue,
            leadtime_to_ship, latest, channel, status_order, date_crawl
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    for _, row in dataframe.iterrows():
        try:
            unit_price = str(row["unit_price"]).replace(",", ".")
            total_revenue = str(row["total_revenue"]).replace(",", ".")
        except Exception:
            unit_price = row["unit_price"]
            total_revenue = row["total_revenue"]

        values = (
            row["platform_id"],
            row["marketplace"],
            row["order_reference"],
            row["order_lines"],
            _parse_date(row["creation_date"]),
            row["sku"],
            row["offer_id"],
            row["quantity"],
            unit_price,
            total_revenue,
            row["leadtime_to_ship"],
            _parse_optional_date(row.get("latest")),
            row["channel"],
            row["status"],
            row["date_crawl"],
        )

        cursor.execute(insert_query, values)

    conn.commit()
    cursor.close()


def main(mp: str, dataframe, db_name: str = "software_dev") -> None:
    print("IN MAIN")
    print(len(dataframe))

    conn = get_db_connection(dbname=db_name)
    try:
        delete_all_rows_from_table(conn, mp)
        insert_into_table(mp, conn, dataframe)
    except psycopg2.Error as err:
        print(f"PostgreSQL error: {err.pgcode}")
        raise
    finally:
        conn.close()