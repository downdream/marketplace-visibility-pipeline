import os
from typing import Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection


def get_db_connection(
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[str] = None,
) -> PgConnection:
    return psycopg2.connect(
        dbname=dbname or os.getenv("DB_NAME", "postgres"),
        user=user or os.getenv("DB_USER"),
        password=password or os.getenv("DB_PASSWORD"),
        host=host or os.getenv("DB_HOST", "localhost"),
        port=port or os.getenv("DB_PORT", "5432"),
    )