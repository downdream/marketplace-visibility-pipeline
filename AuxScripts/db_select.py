# -*- coding: utf-8 -*-

import sys
import os
import pandas as pd

### Add parents path in sys ###
current_dir = os.path.abspath('__file__')
while current_dir != os.path.dirname(current_dir):
    sys.path.append(current_dir)
    current_dir = os.path.dirname(current_dir)

### Add local path for testing
sys.path.append(r'path here')
    
###Python scripts###
from AuxScripts.db_connection import create_db_connection
from AuxScripts.db_connection import print_db_error

# Database Connection Parameters
DB_NAME = "postgres"

'''Read and extract DB table as DataFrame'''
def read_table(schema, table):
    conn = create_db_connection(DB_NAME)
    query = f'SELECT * FROM "{schema}"."{table}"'
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as err:
        print(query)
        print_DB_error(err)
        print('Error: failed to read DB')
    return df

