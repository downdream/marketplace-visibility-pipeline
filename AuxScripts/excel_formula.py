# -*- coding: utf-8 -*-

### Python libraries ###
import pandas as pd
import numpy as np

### Python scripts ###


'''XLOOKUP for DataFrame'''
def xlookup_df(lookup_value, df, lookup_col, return_col, if_not_found=None):
    lookup_value = str(lookup_value)
    df = df.astype(str)
    if pd.isna(lookup_value):
        return np.nan
    else:
        match_value = df.loc[df[lookup_col] == lookup_value, return_col]
        # [if not found]
        if match_value.empty and not if_not_found:  
            #return f'"{lookup_value}" not found!' 
            return ''
        elif match_value.empty and if_not_found:
            return if_not_found
        elif match_value.values[0] == 'nan':
            #return f'"{lookup_value}" not found!' 
            return ''
        else:
            if return_col in ['EAN','SKU']:
                try:
                    val_str = str(int(match_value.values[0]))
                except:
                    val_str = str(match_value.values[0])
                if val_str == '0':
                    val_str = ''
                return val_str
            return str(match_value.values[0])

