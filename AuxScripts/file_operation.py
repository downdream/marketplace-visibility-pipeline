# -*- coding: utf-8 -*-

### Python libraries ###
import pandas as pd
import glob, os
import win32com.client

### Python scripts ###


'''Find OS in folder'''
def find_OS(mp, file_format = 'xlsx', sheet_name = 'Offer Structure'):
    directory = r'directory path here'
    MPs_folder = [f.path for f in os.scandir(directory)]
    
    '''search for shortcut folder for specific MP by name'''
    for folderName in MPs_folder:
        if mp.lower() in folderName.lower():
            target = folderName
            #print(f'shortcut found:\n{target}')
            break
    '''grab the real path of the shortcut folder or folder path if not a shortcut'''
    try:
        shell = win32com.client.Dispatch("WScript.Shell")
        realPath = shell.CreateShortCut(target).Targetpath
    except:
        realPath = folderName        
    #print(f'real path:\n{realPath}')
    
    '''finally find the OS within the folder''' 
    '''for Ebay there are 3 OSs for different accounts so we are gonna combine them'''
    '''else there should be only 1 xlsx file within the folder, so [0] is used'''
    if mp.lower() == 'ebay':
        OSs = glob.glob(f'{realPath}\*.{file_format}')
        
        OS = pd.DataFrame()
        for x in OSs:
            OS = pd.concat([OS, pd.read_excel(x, sheet_name)])
    else:
        try:
            OS = glob.glob(f'{realPath}\*.{file_format}')[0]
        except:
            file_format = 'xlsm'
            OS = glob.glob(f'{realPath}\*.{file_format}')[0]
        #fileName = OS.split('\\')[-1]
        print(f'OS found:\n{OS}')
        try:
            OS = pd.read_excel(OS, sheet_name)
        except:
            OS = pd.read_excel(OS, 'OS')
    return OS


'''Clean OS by offer ID'''
def clean_OS(os_df, otto=False, webshop=False, ebay=False):
    '''Change EAN type to string'''
    os_df['EAN'] = os_df['EAN'].astype(str)
    os_df['EAN'] = os_df['EAN'].str.split('.').str[0]
    # if otto:
    #     clean_os = os_df.loc[os_df['Variant ID'] != '-']
    #     clean_os = clean_os[clean_os['Variant ID'].notnull()]
    #     clean_os['Variant ID'] = clean_os['Variant ID'].astype(str).str.replace('.0','')
    #     clean_os['SKU'] = clean_os['SKU'].astype(str).replace('.0','')
    #     return clean_os
    if webshop or ebay:
        clean_os = os_df.loc[os_df['EAN'] != 'nan']
        clean_os = clean_os[clean_os['EAN'].notnull()]
        clean_os['SKU'] = clean_os['SKU'].astype(str).str.replace('.0','',regex=False)
        clean_os['Offer ID'] = clean_os['Offer ID'].astype(str).str.replace('.0','',regex=False)
        return clean_os
    else:
        clean_os = os_df.loc[os_df['Offer ID'] != '-']
        clean_os = clean_os[clean_os['Offer ID'].notnull()]
        clean_os['Offer ID'] = clean_os['Offer ID'].astype(str).str.replace('.0','',regex=False)
        clean_os['SKU'] = clean_os['SKU'].astype(str).str.replace('.0','',regex=False)
        return clean_os

'''Find latest file in folder'''
def find_latest_file(path, file_format, contains=''):
    list_of_files = glob.glob('{}\*.{}'.format(path, file_format),recursive=False)
    list_of_files = [file for file in list_of_files]
    if contains != '':
        #print(f'check if filename contains {contains}')
        list_of_files = [file for file in list_of_files if contains.lower() in file.lower().split('\\')[-1]]
    #print(list_of_files)
    latest_file = max(list_of_files, key=os.path.getctime)
    print(f'latest {file_format} file: {latest_file}')
    return latest_file

'''Find folder based on name'''
def find_folder_by_name(path,contains):
    folders = os.listdir(path)
    for folder_name in folders:
        #print(f'check if folder name contains {contains}')
        if contains.lower() in folder_name.lower() and '.' not in folder_name.lower():
            return folder_name

    