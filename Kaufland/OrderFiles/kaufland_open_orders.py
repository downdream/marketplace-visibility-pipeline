import pandas as pd
from datetime import datetime
import time
import requests
import hmac
import hashlib
from datetime import timedelta
from dateutil.parser import isoparse
import sys
import os
import pytz

### Add parents path in sys ###
current_dir = os.path.abspath('__file__')
while current_dir != os.path.dirname(current_dir):
    sys.path.append(current_dir)
    current_dir = os.path.dirname(current_dir)

### Add local path for testing: ***CHANGE TO YOUR PERSONAL PROJECT PATH***
sys.path.append(r'C:\Users\Anne-Fleur Kerhousse\GitLab\python')

### Python scripts ###
from AuxScripts.fetch_cicd_var import fetch_var
from AuxScripts.db_insert_orders import main

timezone = pytz.timezone('Europe/Brussels')
today = datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")

def init():
    global start_time, start_date, columns_list, result, client_key, secret_key, method, body, mp, statuses, regions
    start_time = datetime.now()
    columns_list=['platform_id','marketplace','order_reference','order_lines','creation_date','sku','offer_id','quantity','unit_price','total_revenue','leadtime_to_ship','latest','channel','status','date_crawl']
    result = pd.DataFrame(columns=columns_list)
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")+'T00%3A00%3A00Z'

    '''Defining variables & function needed for API request'''
    # Call the fetch_var function with the desired variable key
    client_key = fetch_var('KAUFLAND_CLIENT')
    secret_key = fetch_var('KAUFLAND_SECRET')
    method = 'GET'
    body = ''
    mp = "Kaufland"

    '''Modify below for getting orders with specific status'''
    statuses = ['cancelled','need_to_be_sent','open','received','returned','returned_paid','sent','sent_and_autopaid']
    regions = ['de','cz']
    return

def sign_request(method, uri, body, timestamp, secret_key):
    plain_text = "\n".join([method, uri, body, timestamp])
    digest_maker = hmac.new(secret_key.encode(), None, hashlib.sha256)
    digest_maker.update(plain_text.encode())
    return digest_maker.hexdigest()

def make_headers(uri, body):
    timestamp = str(int(time.time()))
    headers = {
    'Accept': '*/*',
    'Content-Type': 'application/json',
    'Shop-Client-Key': client_key,
    'Shop-Timestamp': timestamp,
    'Shop-Signature': sign_request(method, uri, body, timestamp, secret_key),
    'User-Agent': "Inhouse Development",
    }
    return headers

'''The function for getting all order id'''
def get_orders(region,status):
    if region == 'de':
        pid = 1
    else:
        pid = 1.1
    print(status)
    platformId = []
    orderReference = []
    orderLines = []
    creationDate = []
    sku = []
    offerID = []
    quantity = []
    unitPrice = []
    totalRevenue = []
    leadtimeToShip = []
    latest= []
    channel = []
    statusOrder = []
    marketplace = []
    ean = []
    offset = 0
    total = 100 # setting default total as 100 for the while loop
    while len(orderReference)<total:
        try:
            uri = "https://sellerapi.kaufland.com/v2/order-units?storefront={}&status={}&ts_created_from_iso={}&fulfillment_type=fulfilled_by_merchant&limit=100&offset={}".format(region,status,start_date,str(offset))
            print(uri)
            dat = requests.get(uri, headers=make_headers(uri,body)).json()
            total = dat['pagination']['total'] # updating number of total orders
            print(total)
            #print('crawled: '+str(dat['pagination']['offset'])+', '+'total: '+str(total))
            for item in dat['data']:
                platformId.append(pid)
                orderReference.append(item["id_order"])
                orderLines.append(item["id_order_unit"])
                creationDate.append(isoparse(item["ts_created_iso"]).strftime("%d.%m.%Y"))
                latest.append(isoparse(item["delivery_time_expires_iso"]).strftime("%d.%m.%Y"))
                offerID.append(item['product']['id_product'])
                sku.append(item['id_offer'][:6])
                # ean.append(item['product']['eans'][0])
                unitPrice.append(item['price']/100)
                totalRevenue.append(item['revenue_gross']/100)
                leadtimeToShip.append(item['delivery_time_max'])
                channel.append(item["storefront"])
                quantity.append(1)
                statusOrder.append(item['status'])
                marketplace.append(mp)
        except:
            break
        offset += 100
    dateCrawl = [today]*len(platformId)
    output = pd.DataFrame(list(zip(platformId, marketplace, orderReference, orderLines, creationDate, sku, offerID, quantity, unitPrice, totalRevenue, leadtimeToShip, latest, channel, statusOrder, dateCrawl)),columns=columns_list)
    print(output)
    print(status + ': ' +str(total))
    return output

def get_status(unit):
    uri = "https://sellerapi.kaufland.com/v2/order-units/{}".format(str(unit))
    print(unit)
    dat = requests.get(uri, headers=make_headers(uri)).json()['data']  
    idd = dat['id_order']
    created = dat['ts_created_iso']
    status = dat['status']
    modified = dat['ts_updated_iso']
    ean = dat['product']['id_product']
    output = [created, unit, idd, status, modified, ean]
    print(output)
    return output
    
'''Exporting result to target folder + Uploading the result to Data Warehouse'''
def export():
    timestr = time.strftime("%d.%m.%Y-%H%M%S") 
    '''DE file'''
    res_DE = result.loc[result['channel'] == 'de']
    # name_res_DE = f"Kaufland_Openorder_DE_{timestr}"
    main('Kaufland_DE',res_DE)
    # res_DE.to_excel(r'Exports_Open/DE/{}.xlsx'.format(name_res_DE), index=False)
    '''CZ file'''
    # res_CZ = result.loc[result['channel'] == 'cz']
    # name_res_CZ = f"Kaufland_Openorder_CZ_{timestr}"
    # main('Kaufland_CZ',res_CZ)
    # res_CZ.to_excel(r'Exports_Open/CZ/{}.xlsx'.format(name_res_CZ), index=False)

init()
for region in regions:
    for status in statuses:
        output = get_orders(region,status)
        global result 
        result = pd.concat([result,output])
        print('♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦♦')
export()
end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))
