from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.database.db_connection import get_db_connection


def extract_keywords(excel_path: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path)
    df["date"] = datetime.today().strftime("%Y-%m-%d")
    return df


def insert_data_into_table(conn, dataframe: pd.DataFrame) -> None:
    schema = "public"
    table_name = "keywords"
    cursor = conn.cursor()

    print(f'-- IN INSERT: Inserting into "{schema}"."{table_name}" ---')

    insert_query = f'''
        INSERT INTO "{schema}"."{table_name}" (country, type, keywords, date)
        VALUES (%s, %s, %s, %s)
    '''

    for _, row in dataframe.iterrows():
        values = (
            row["Country"],
            row["Type"],
            str(row["Keywords"]).strip(),
            row["date"],
        )
        cursor.execute(insert_query, values)

    conn.commit()
    cursor.close()


def main(excel_path: str, db_name: str = "postgres") -> None:
    start = datetime.now()
    dataframe = extract_keywords(excel_path)

    conn = get_db_connection(dbname=db_name)
    try:
        insert_data_into_table(conn, dataframe)
    finally:
        conn.close()

    end = datetime.now()
    print("--------------------------------------------------------------------")
    print(f"Duration: {end - start}")


if __name__ == "__main__":
    sample_path = Path("sample_data/Keywords.xlsx")
    if sample_path.exists():
        main(str(sample_path))
    else:
        print("Add your Excel file path when running this script.")