import pandas as pd

from src.database.db_connection import get_db_connection


def read_table(schema: str, table: str, db_name: str = "postgres") -> pd.DataFrame:
    query = f'SELECT * FROM "{schema}"."{table}"'
    conn = get_db_connection(dbname=db_name)

    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()