from src.database.db_connection import get_db_connection


def extract_offer_fields(raw_row):
    raw = raw_row["raw_json"]

    return (
        raw_row["snapshot_time"],
        raw_row["source"],
        raw.get("offer_id"),
        raw.get("title"),
        raw.get("description"),
        raw.get("unit_price"),
        raw.get("converted_unit_price"),
        raw.get("display_currency"),
        raw.get("offer_currency"),
        raw.get("available_qty"),
        raw.get("reserved_qty"),
        raw.get("seller_id"),
        raw.get("username"),
        raw.get("seller_ranking"),
        raw.get("user_level"),
        raw.get("satisfaction_rate"),
        raw.get("total_rating"),
        raw.get("total_completed_orders"),
        raw.get("total_success_order"),
        raw.get("status"),
        raw.get("is_online"),
        raw.get("created_at"),
        raw.get("updated_at"),
    )


def fetch_raw_offers(conn):
    query = """
        SELECT snapshot_time, source, offer_id, raw_json
        FROM public.offers_raw
        ORDER BY id
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def insert_clean_offers(conn, clean_rows):
    query = """
        INSERT INTO public.offers_clean (
            snapshot_time, source, offer_id, title, description,
            unit_price, converted_unit_price, display_currency, offer_currency,
            available_qty, reserved_qty, seller_id, username, seller_ranking,
            user_level, satisfaction_rate, total_rating, total_completed_orders,
            total_success_order, status, is_online, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cursor:
        for row in clean_rows:
            cursor.execute(query, row)
    conn.commit()


def main():
    conn = get_db_connection()
    try:
        raw_rows = fetch_raw_offers(conn)
        clean_rows = [extract_offer_fields(row) for row in raw_rows]
        insert_clean_offers(conn, clean_rows)
        print(f"Inserted {len(clean_rows)} clean offers")
    finally:
        conn.close()


if __name__ == "__main__":
    main()