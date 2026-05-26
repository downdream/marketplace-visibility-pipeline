from src.database.db_connection import get_db_connection
from src.utils.extract_offer_attributes import extract_offer_attribute_rows

def fetch_raw_offers(conn):
    query = """
        SELECT snapshot_time, snapshot_date, game_name, raw_json
        FROM public.offers_raw
        ORDER BY id
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def insert_offer_attributes_raw(conn, attribute_rows):
    query = """
        INSERT INTO public.offer_attributes_raw (
            snapshot_time,
            snapshot_date,
            game_name,
            offer_id,
            title,
            collection_id,
            dataset_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cursor:
        for row in attribute_rows:
            cursor.execute(
                query,
                (
                    row["snapshot_time"],
                    row["snapshot_date"],
                    row["game_name"],
                    row["offer_id"],
                    row["title"],
                    row["collection_id"],
                    row["dataset_id"],
                ),
            )
    conn.commit()


def main():
    conn = get_db_connection()
    try:
        raw_rows = fetch_raw_offers(conn)
        
        all_attribute_rows = []
        for raw_row in raw_rows:
            extracted_rows = extract_offer_attribute_rows(
                raw_offer=raw_row["raw_json"],
                game_name=raw_row["game_name"],
            )
            
            for row in extracted_rows:
                row["snapshot_time"] = raw_row["snapshot_time"]
                row["snapshot_date"] = raw_row["snapshot_date"]
                
            all_attribute_rows.extend(extracted_rows)
            
        insert_offer_attributes_raw(conn, all_attribute_rows)
        print(f"Inserted {len(all_attribute_rows)} attribute rows")
        
    finally:
        conn.close()
        
if __name__ == "__main__":
    main()