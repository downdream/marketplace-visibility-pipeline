import matplotlib.pyplot as plt
import pandas as pd
import sys
import os


### Add parents path in sys ###
current_dir = os.path.abspath('__file__')
while current_dir != os.path.dirname(current_dir):
    sys.path.append(current_dir)
    current_dir = os.path.dirname(current_dir)

### Add local path for testing ###
sys.path.append(r'A:\Corpa\python')

from AuxScripts.db_select import read_table
from AuxScripts.db_connection import create_db_connection,print_db_error

'''Reading data from DB'''
data = read_table("Backmarket", "Backmarket_va")
offer_id = "f494a8a4-ef58-4a1c-9495-a64d21fed02f" #change offer_id as needed

def create_visual(data,offer_id):
    
    product_df = data[data['offer_id'] == offer_id]
    print(product_df)

    # Convert 'date' column to datetime format
    product_df['date'] = pd.to_datetime(product_df['date'], format='%Y-%m-%d')
    product_df.set_index('date', inplace=True)

    # Create the line plot
    plt.figure(figsize=(12, 6))  # Adjust figure size as needed
    plt.plot(product_df.index, product_df['rank'])
    plt.xlabel('Date')
    plt.ylabel('Rank')
    plt.title(f'Rank Changes for Offer ID: {offer_id}')
    plt.grid(True)
    plt.show()
    
    
create_visual(data,offer_id)
    


