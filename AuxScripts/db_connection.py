# -*- coding: utf-8 -*-

### Python libraries ###
import psycopg2
import sys

"""
Grabbing the username and password from Git stored data, will be used for later
### Python scripts ###
from AuxScripts.fetch_cicd_var import fetch_var

# Database Connection Parameters
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = fetch_var('PGADMIN_USERNAME')
DB_PASSWORD = fetch_var('PGADMIN_PASSWORD')
"""
# Database Connection Parameters
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = "admin"

'''Connect to the Database'''
def create_db_connection(DB_NAME):
    print('-- IN CONN --')
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        client_encoding='utf-8'
    )
    return conn

'''Print error message when executing query'''
def print_db_error(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")
    
    
# for testing purpose
conn = create_db_connection("postgres")
print(conn)
