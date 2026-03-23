from typing import Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection

from src.config import DB_HOST, DB_NAME, DB_PORT, DB_USER, DB_PASSWORD


def get_db_connection(
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[str] = None,
) -> PgConnection:
    return psycopg2.connect(
        dbname=dbname or DB_NAME,
        user=user or DB_USER,
        password=password or DB_PASSWORD,
        host=host or DB_HOST,
        port=port or DB_PORT,
    )