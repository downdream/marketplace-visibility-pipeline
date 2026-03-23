import psycopg2
from datetime import datetime
import sys

### Add local path for testing: ***CHANGE TO YOUR PERSONAL PROJECT PATH***
sys.path.append(r'path here')

### Python scripts ###
from AuxScripts.db_connection import create_db_connection, print_DB_error

# Database Connection Parameters
DB_NAME = "software_dev"

def delete_all_rows_from_table(conn, mp):
    print('IN DELETE')
    with conn, conn.cursor() as cursor:
        query = f"""
                DELETE FROM "{mp}"."ORDERS_TEST"
        
                """
        cursor.execute(query)

def insert_into_table(mp, conn, dataframe):
    print(len(dataframe))
    cursor = conn.cursor()
    print(f'IN INSERT {mp}')
    #Iterate over each row in the DataFrame and insert the values into the table
    for index, row in dataframe.iterrows():
        print(row)
        platformId = row['platform_id']
        orderReference = row['order_reference']
        orderLines = row['order_lines']
        # Parse the date using datetime
        creation_date = datetime.strptime(row['creation_date'], '%d.%m.%Y')
        # Format the date as a string in the postgresql format
        creation_date = creation_date.strftime('%Y-%m-%d')
        sku = row['sku']
        offerID = row['offer_id']
        quantity = row['quantity']
        try:
            unitPrice = row['unit_price'].replace(',','.')
            totalRevenue = row['total_revenue'].replace(',','.')
        except:
            unitPrice = row['unit_price']
            totalRevenue = row['total_revenue']
        leadtimeToShip = row['leadtime_to_ship']
        try:
            latest = datetime.strptime(row['latest'], '%d.%m.%Y')
            # Format the date as a string in the postgresql format
            #latest = latest.strftime('%Y-%m-%d')
            latest = str(latest.strftime('%Y-%m-%d'))
        except:
            #latest = datetime.date(1000, 10, 10)
            latest = ''
        channel = row['channel']
        statusOrder = row['status']
        marketplace = row['marketplace']
        dateCrawl = row['date_crawl']
        # SQL query to insert a single row into the table
        if latest == '':
            query = f"""
                    INSERT INTO "{mp}"."ORDERS_TEST" (platform_id, marketplace, order_reference, order_lines, creation_date, sku, offer_id, quantity, unit_price, total_revenue, leadtime_to_ship, latest, channel, status_order, date_crawl) 
                    VALUES ({platformId},'{marketplace}','{orderReference}', '{orderLines}', '{creation_date}', {sku}, '{offerID}', {quantity},{unitPrice},{totalRevenue},{leadtimeToShip},NULL,'{channel}','{statusOrder}','{dateCrawl}')
                    
                    """
        else:
            query = f"""
                    INSERT INTO "{mp}"."ORDERS_TEST" (platform_id, marketplace, order_reference, order_lines, creation_date, sku, offer_id, quantity, unit_price, total_revenue, leadtime_to_ship, latest, channel, status_order, date_crawl) 
                    VALUES ({platformId},'{marketplace}','{orderReference}', '{orderLines}', '{creation_date}', {sku}, '{offerID}', {quantity},{unitPrice},{totalRevenue},{leadtimeToShip},'{latest}','{channel}','{statusOrder}','{dateCrawl}')
                    
                    """
        try:
            cursor.execute(query)
        except Exception as err:
            print(query)
            print_DB_error(err)
    # Commit the changes to the database
    conn.commit()
    
    # Close the connection
    cursor.close()
    

    # Main Function
def main(mp,dataframe):
    print('IN MAIN')
    print(len(dataframe))
    # Connect to the Database
    conn = create_db_connection(DB_NAME)
    # Delete values in the table
    delete_all_rows_from_table(conn, mp)
    # Insert Data into Database Table
    # if 'Kaufland' in mp:
    #     try:
    #         kaufland(mp, conn, dataframe)
    #     except psycopg2.Error as e:
    #         # Handle the error
    #         error_message = e.pgcode  # PostgreSQL error code
    #         print(f"Error: {error_message}")
    # else:
    try:
        insert_into_table(mp, conn, dataframe)
    except psycopg2.Error as e:
        # Handle the error
        error_message = e.pgcode  # PostgreSQL error code
        print(f"Error: {error_message}")
    # Close the Database Connection
    conn.close()

# if __name__ == "__main__":
#     insert_into_table(mp,dataframe)


