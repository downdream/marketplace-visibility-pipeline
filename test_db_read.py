from src.database.db_select import read_table

df = read_table("public", "keywords")
print(df.head())
print("Rows:", len(df))