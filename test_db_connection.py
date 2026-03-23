from src.database.db_connection import get_db_connection

conn = get_db_connection()
print("Connection created successfully")
conn.close()
print("Connection closed successfully")