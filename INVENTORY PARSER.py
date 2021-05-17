import pandas as pd
import numpy as np
import os
import re
from GSPREAD import open_gspread
import glob
from shutil import *
from tqdm import tqdm
from datetime import date

def pd_disp_op():

    ''' Set pandas display options '''

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

def read_xlsx(FOLDER_PATH):

    ''' Takes folder path, read *xlsx and returns DataFrame with some columns renamed
            str >>> DataFrame
    '''

    fileNames = glob.glob(f'{FOLDER_PATH}/*.xlsx')
    data = pd.read_excel(fileNames[0], engine='openpyxl', skiprows=2)  # engine='openpyxl' is old engine required for reading xlsx
    data = data.rename(columns={'Primary Product Manager': 'PPM',
                                'PO-L-S': 'PO',
                                'Quantity Available (including Soft Reserved)': "Qty_available",
                                'Carrier(s) Assigned': "Carrier"})

    # Clean col 'Container#', removing whitespaces and semicolon in the end of str
    data["Container#"] = data["Container#"].str.strip()
    data["Container#"] = data["Container#"].str.replace(" ", '')
    data['Container#'] = data['Container#'].fillna('').apply(lambda x: x[:-1])

    # Add columns: Total Costs, and SCAC
    data['Total Costs'] = data['Qty_available'] * data['Total Unit Cost']
    data['SCAC'] = np.zeros(data.shape[0])
    data.SCAC.iloc[:] = 'NaN'

    return data

def read_xls(FOLDER_PATH):

    ''' Takes folder path, read *xls and returns DataFrame with some columns renamed
            str >>> DataFrame['Container#','SCAC','Vessel']
    '''

    fileNames = glob.glob(f'{FOLDER_PATH}/*.xls')
    data = pd.read_excel(fileNames[0])
    data = data.rename(columns={'Container Number': 'Container#',
                                'Carrier SCAC': "SCAC",
                                'Vessel Name':'Vessel'})

    # Drop lines wih the same container number
    data = data.drop_duplicates(subset='Container#', keep='first', ignore_index=True)

    # Specify df columns which are to be displayed
    view_cols = ['Container#','SCAC','Vessel']

    return data[view_cols]

def filter(PO_or_ITEM,data,WH=''):

    ''' Takes either PO_num or ITEM_num and return filtered DataFrame
                str, df >>> DataFrame

        :param PO_num: str (example: WEG002121)
        :param ITEM: str (example: TEP,tep,CHDM-001)
        :param data: df returned from func read_xlsx
        :param WH: optional var for filtering the data by a specific warehouse
        :return: df['Item','Item Description','PO','Lot #','Container#',
                    'Subinventory','Inv Org','Qty_available','Carrier','SCAC']
        '''

    # Make input case insensitive
    PO_or_ITEM = PO_or_ITEM.upper()
    WH = WH.upper()

    # Check if input is PO_num or Item (hint: all PO_num contain 6 digits)
    count = 0
    for char in PO_or_ITEM:
        if char.isdigit(): count+=1

    # Check the condition if PO, filter by the col "PO", otherwise use col "Item"
    # (optional use: warehouse)
    if count == 6:
        mask_PO = data['PO'].str.contains(PO_or_ITEM).fillna(False)
        mask_WH = data['Inv Org'].str.contains(WH).fillna(False)
        df = data[mask_PO & mask_WH]
    else:
        mask_ITEM = data['Item'].str.contains(PO_or_ITEM).fillna(False)
        mask_WH = data['Inv Org'].str.contains(WH).fillna(False)
        df = data[mask_ITEM & mask_WH]

    # Specify df columns which are to be displayed
    view_cols = ['Item', 'Item Description', 'PO', 'Lot #',
                 'Container#', 'Subinventory', 'Inv Org', 'Qty_available',
                'Carrier','SCAC']

    # Sort our df by a specific column
    df = df[view_cols].sort_values(by='Subinventory', ascending=False)

    return df

def ExpSoon(data):

    ''' Takes data and return ExpSoon_Lots DataFrame sorted by PM
        df >>> DataFrame
    '''

    # Get current date in a specified format
    dateForm = "%Y-%m-%d"
    today = date.today().strftime(dateForm)

    # Retrieve items which are expired
    df = data[data['Expiration Date'] < today]

    # Specify df columns which are to be displayed
    view_cols = ['PPM','Item','PO','Lot #','Container#',
                 'Subinventory','Inv Org','Qty_available','Expiration Date']

    # Sort our df by a specific column
    df = df[view_cols].sort_values(by='PPM')

    return df

def totalCost(pmStr,data):

    ''' Str >>> Str returns str_list of inventory costs by item and its sum
    : pmStr - name of Product Manager
    '''

    # Return list of unique items for a specific Product manager
    itemList = data[data['PPM'].str.contains(pmStr).fillna(False)]['Item'].unique()

    # Return total sum of inventory for a specific PM item-wise

    totalSum = 0
    for item in sorted(itemList):

        # Return total cost for a specific item
        totalCost = data[data['Item'].str.contains(item).fillna(False)]['Total Costs'].sum()
        if totalCost > 0:
            print(f'Total cost of {item} is {totalCost:,.2f}')      # Total costs for a specific item
            totalSum += totalCost                                   # Accumulate the totalSum
    print('...................................')
    print(f'Total sum is ......... {totalSum:,.2f}')

def booked_not_booked_iso(data):

    '''
    Returns printed out pivot table of booked/not booked iso-containers for specific PM
    Example:
                             Not_booked
    PPM
    Amelia Greene            133.0
    Jason LoPipero            26.0

                             Booked
    PPM
    Amelia Greene             22.0
    Jason LoPipero            15.0
    '''

    mask_ITEM = data['Item'].str.contains('').fillna(False)
    mask_ITDESCR = data['Item Description'].str.contains('ISO').fillna(False)
    mask_LOT1 = data['Lot #'].isna()
    mask_LOT2 = data['Lot #'].notna()
    mask_SUBINV = data['Subinventory'].str.contains('Not').fillna(False)

    # Specify df columns which are to be displayed
    view_cols = ['PPM','Item','Item Description','PO','Lot #','Container#','Subinventory','Inv Org','Qty_available','Carrier']

    booked = round(data[mask_ITEM & mask_ITDESCR & mask_LOT1 & mask_SUBINV][view_cols].groupby(by='PPM').sum()/20000)
    not_booked = round(data[mask_ITEM & mask_ITDESCR & mask_LOT2 & ~mask_SUBINV][view_cols].groupby(by='PPM').sum()/20000)

    booked = booked.sort_values(by='Qty_available',ascending=False)
    not_booked = not_booked.sort_values(by='Qty_available',ascending=False)

    print(booked)
    print(not_booked)

def scac(cont):

    ''' Returns df1 filter by a specific container #'''
    return data1[data1['Container#'].str.contains(cont)]

def add_scacs(data,data1):

    ''' df1, df2 >>> df1 with scacs returns df1 with SCA-CODES
        '''

    # Form a dictionary where SCAC is a key and idx list - values (example: 'ZIMU': ['417', '...', etc]
    our_dict = {}
    for i in range(data1.shape[0]):

        contNum = data1['Container#'][i]                        # Get a container number from df1
        mask_CONT = data['Container#'].str.contains(contNum)    # Form a mask for df using contNum

        # Check if the container(s) from df1 is/are in df. Return matched idx in native format if any
        if len(data[mask_CONT].index.to_native_types()) > 0:
            SCAC = data1['SCAC'][i]                             # Get a SCAC for a specific container from df1

            if SCAC not in our_dict:

                # Form a list of values
                values = data[data['Container#'].str.contains(contNum)].index.to_native_types().tolist()
                values = list(map(int,values))          # return list of ints
                our_dict[SCAC] = values                 # assign values in this case
            else:
                # Form a list of values
                values = data[data['Container#'].str.contains(contNum)].index.to_native_types().tolist()
                values = list(map(int, values))         # return list of ints
                our_dict[SCAC].extend(values)           # extend values in this case

    # Final step. Assign SCAC value for zero value "SCAC" in df

    for key in our_dict:
        mask = our_dict[key]                            # retrieve idx to form a mask
        data.SCAC.iloc[sorted(mask)] = key              # assign codes for specific rows in df

    return data

if __name__ == '__main__':

    pd_disp_op()                # set display options
    GSPREAD_NAME_2 = r'ORDERS'
    FOLDER_PATH = r'C:\Users\promy\Desktop\Temp'
    data = read_xlsx(FOLDER_PATH)
    data1 = read_xls(FOLDER_PATH)
    data = add_scacs(data,data1)

    # data = data.replace(np.nan,'')
    # rows_to_insert = [data.keys().tolist()] + data.values.tolist()
    # open_gspread(rows_to_insert, GSPREAD_NAME_2, 2736)
    # data.to_csv(os.path.join(FOLDER_PATH,'CHDM.csv'),index=False)
    # df4 = data[data['SCAC'] != 'NaN'][['PO','Item','Lot #','Container#','SCAC']]
    # df4.to_csv(os.path.join(FOLDER_PATH, 'PO_INV_SCAC.csv'), index=False)
    print()


