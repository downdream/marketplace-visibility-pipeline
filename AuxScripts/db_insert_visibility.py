# -*- coding: utf-8 -*-

import datetime
import sys

###Python scripts###
sys.path.append(r'path here')
from AuxScripts.db_connection import create_db_connection,print_db_error

# Database Connection Parameters
DB_NAME = "postgres"


'''Insert data from DataFrame into Database Table'''
def insert_data_into_table(conn, dataframe, mp):
    schema = mp
    print(schema)
    cursor = conn.cursor()
    print(f'-- IN INSERT: Inserting into "{schema}"."{schema}_va" ---')
    # Iterate over each row in the DataFrame and insert the values into the table
    for index, row in dataframe.iterrows():
        print(row)
        #Converting into postgres date format yyyy-mm-dd
        day = row['date'].split('.')[0]
        month = row['date'].split('.')[1]
        year = row['date'].split('.')[2]        
        date = f'{year}-{month}-{day}'
        #Getting values from df row
        try:
            keyword = row['keyword'].encode('utf-8', errors='ignore').decode('utf-8')
        except:
            keyword = row['keyword'] 
        offer = row['offer_id']
        platform = row['platform_id']
        url = row['product_link']
        title = row['product_title'].replace("'","`")
        try:
            seller = row['seller'].encode('utf-8', errors='ignore').decode('utf-8')
            seller = seller.split('(')[0].strip()
        except:
            seller = row['brand']
        seller = seller.replace("'","''")
        rank = row['rank']
        ean = row['ean']
        try:
            sponsored = row['sponsored']
        except:
            pass
        cursor.execute("SET client_encoding TO 'UTF8'")
        
        
        query = f"""
                INSERT INTO "{schema}"."{schema}_va" (keyword, url, offer_id, seller, title, rank, sponsored, platform_id, ean, date)
                VALUES ('{keyword}', '{url}', '{offer}', '{seller}', '{title}', {rank}, '{sponsored}', {platform},  '{ean}', '{date}')
                """
                    
        try:
            cursor.execute(query)
        except Exception as err:
            print(query)
            print_db_error(err)
            break 
    # Commit the changes to the database
    conn.commit()  
    cursor.close()

'''Main function'''
def main(dataframe, mp):
    print('-- IN MAIN --')
    start = datetime.datetime.now()
    # Connect to the Database
    conn = create_db_connection(DB_NAME)
    # Insert Data into Database Table
    try:
        insert_data_into_table(conn, dataframe, mp)
    except:
        print('Error: failed to push into DB')
    # Close the Database Connection
    conn.close()
    end = datetime.datetime.now()
    print('--------------------------------------------------------------------')
    print('Duration: {}'.format(end - start))