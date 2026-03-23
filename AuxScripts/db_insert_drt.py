# -*- coding: utf-8 -*-

import sys
from datetime import datetime

###Python scripts###
sys.path.append(r'path here')
from dbConn import create_db_connection,print_DB_error

# Database Connection Parameters
DB_NAME = "software_dev"

table_dict = {
    'amazon':'Amazon_Vendor',
    'bol':'Bol',
    'but':'But',
    'cdiscount':'Cdiscount',
    'check24':'Check24',
    'conforama':'Conforama',
    'darty':'Darty',
    'home24_de':'Home24_DE',
    'home24_at':'Home24_AT',
    'kaufland_de':'Kaufland_DE',
    'mdm':'MaisonsDuMonde',
    'otto':'Otto',
    }

#sponsored_mp = ['amazon','bol','cdiscount','kaufland_de','otto']

'''Insert data from DataFrame into Database Table'''
def insert_data_into_table(conn, dataframe, mp):
    schema = table_dict[mp.lower()]
    cursor = conn.cursor()    
    print(f'-- IN INSERT: Inserting into "{schema}"."DRT" ---')
    # Iterate over each row in the DataFrame and insert the values into the table
    for index, row in dataframe.iterrows():
        print(row)
        #Converting into postgres date format yyyy-mm-dd
        day = row['date'].split('.')[0]
        month = row['date'].split('.')[1]
        year = row['date'].split('.')[2]
        if len(year) == 2:
            year = '20' + year
        date = f'{year}-{month}-{day}'
        url = row['url'].replace("'","''")
        offer = row['offer_id']
        platform = row['platform_id']
        review = row['review']
        score = row['score']
        ean = row['ean']
        if mp == 'amazon':
            if row['rank1'] == '':
                rank1 = 'NULL'
            else:
                rank1 = int(row['rank1'])
            if row['rank2'] == '':
                rank2 = 'NULL'
            else:
                rank2 = int(row['rank2'])
            # SQL query to insert a single row into the table
            query = f"""
                    INSERT INTO "Amazon_Vendor"."DRT" (date, offer_id, platform_id, url, review, score, rank1, rank2, ean)
                    VALUES ('{date}', '{offer}', {platform}, '{url}', {review}, {score}, {rank1}, {rank2}, '{ean}')
                    
                    """
        else:
            # SQL query to insert a single row into the table 
            query = f"""
                    INSERT INTO "{schema}"."DRT" (date, offer_id, platform_id, url, review, score, ean)
                    VALUES ('{date}', '{offer}', {platform}, NULLIF('{url}',''), {review}, {score}, NULLIF('{ean}',''))
                   
                    """
        try:
            #print(query)
            cursor.execute(query)
        except Exception as err:
            print(query)
            print_DB_error(err)
            break
    # Commit the changes to the database
    conn.commit()
    cursor.close()

'''Main function'''
def main(dataframe, mp):
    print('-- IN MAIN --')
    start = datetime.now()
    # Connect to the Database
    conn = create_db_connection(DB_NAME)
    # Insert Data into Database Table
    try:
        insert_data_into_table(conn, dataframe, mp)
    except:
        print('Error: failed to push into DB')
    # Close the Database Connection
    conn.close()
    end = datetime.now()
    print('--------------------------------------------------------------------')
    print('Duration: {}'.format(end - start))