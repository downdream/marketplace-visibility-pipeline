# -*- coding: utf-8 -*-

import sys
import os
from datetime import datetime
import pandas as pd

### Add parents path in sys ###
current_dir = os.path.abspath('__file__')
while current_dir != os.path.dirname(current_dir):
    sys.path.append(current_dir)
    current_dir = os.path.dirname(current_dir)

### Add local path for testing
sys.path.append(r'path here')
    
###Python scripts###
from AuxScripts.db_connection import create_db_connection,print_DB_error

# Database Connection Parameters
DB_NAME = "postgres"

def extract_keywords():
    df = pd.read_excel(r'A:\Corpa\python\Data\Keywords.xlsx')
    df['date'] = datetime.today().strftime('%Y-%m-%d')
    print(df)
    return df


'''Insert data from DataFrame into Database Table'''
def insert_data_into_table(conn, dataframe):
    schema = 'public'
    cursor = conn.cursor()    
    print(f'-- IN INSERT: Inserting into "{schema}"."keywords" ---')
    # Iterate over each row in the DataFrame and insert the values into the table
    for index, row in dataframe.iterrows():
        print(row)
        date = row['date']
        keyword = row['Keywords'].replace("'","''")
        country = row['Country']
        k_type = row['Type']
        # SQL query to insert a single row into the table 
        query = f"""
                INSERT INTO "{schema}"."keywords" (country, type, keywords, date)
                VALUES ('{country}', '{k_type}', '{keyword}', '{date}')
               
                """
        try:
            print(query)
            cursor.execute(query)
        except Exception as err:
            print(query)
            print_DB_error(err)
            break
    # Commit the changes to the database
    conn.commit()
    cursor.close()

'''Main function'''
def main(dataframe):
    print('-- IN MAIN --')
    start = datetime.now()
    # Connect to the Database
    conn = create_db_connection(DB_NAME)
    # Insert Data into Database Table
    try:
        insert_data_into_table(conn, dataframe)
    except:
        print('Error: failed to push into DB')
    # Close the Database Connection
    conn.close()
    end = datetime.now()
    print('--------------------------------------------------------------------')
    print('Duration: {}'.format(end - start))

df = extract_keywords()
main(df)