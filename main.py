import pandas as pd
import numpy as np
from pathlib import Path
import re

waste_sd = {
    "General Waste" : {
    "0.12" : 12.6,
    "0.24" : 25.2,
    "0.66" : 69.3,
    "1.1" : 115.5,
    "1.5" : 157.5,
    "3" : 315.0,
    "4.5" : 472.5
},
"Dry Waste" : {
    "0.12" : 8.4,
    "0.24" : 16.8,
    "0.66" : 46.2,
    "1.1" : 77,
    "1.5" : 105,
    "3" : 210,
    "4.5" : 315
},
"Comingle" : {
    "0.12" : 7.2,
    "0.24" : 14.4,
    "0.66" : 39.6,
    "1.1" : 66.0,
    },
 "Paper/Cardboard":{
     "0.12" : 6,
    "0.24" : 12,
    "0.66" : 33,
    "1.1" : 55,
    "1.5" : 75,
    "3" : 150,
    "4.5" : 225
 },
 "Paper":{
    "0.12" : 10.8,
    "0.24" : 21.6,
    "0.66" : 59.4
 },
"Glass" : {
    "0.12" : 24,
    "0.24" : 48,
    "0.66" : 132
},
"Organics" : {
    "0.12" : 24,
    "0.24" : 48
}
}

path = "../../ubuntuShareDrive/Datasets/Nabour_report_ds/2020_1.csv"

df = pd.read_csv(path, dtype={"Customer number": np.float64 ,"Schd Time Start": str, "PO": str, 'PostCode': str})

# df['Customer number'] =
# df = df.round({'Customer number' : 3})
df['Customer number'] = df['Customer number'].round(3)

others = df[df['Customer number'].isin([1887,3763,3268,3483,2317,4236,3476,4156,3966])]
df_4138 = df.loc[(df['Customer number'] > 4138.000) & (df['Customer number'] <= 4138.1)]
df_1021 = df.loc[(df['Customer number'] > 1021.000) & (df['Customer number'] <= 1021.1)]
df_2505 = df.loc[(df['Customer number'] > 2505.000) & (df['Customer number'] <= 2505.1)]

df_3139 = df.loc[(df['Customer number'] >= 3139.000) & (df['Customer number'] <= 3139.1)]

# 4138 group 
df_4138_group = df_4138.groupby('Customer number')
df_4138_group_keys = list(df_4138_group.indices.keys())


# 1021 group
df_1021_group = df_1021.groupby('Customer number')
df_1021_group_keys = list(df_1021_group.indices.keys())

# others Group
others_group = others.groupby('Customer number')
others_group_keys = list(others_group.indices.keys()) 

# 2505 Group
df_2505_group = df_2505.groupby('Customer number')
df_2505_group_keys = list(df_2505_group.indices.keys()) 

# =====================================================================
df_3139_group = df_3139.groupby('Customer number')
df_3139_group_keys = list(df_3139_group.indices.keys()) 
# =====================================================================


for customer_key in df_3139_group_keys:

    naber_df = df_3139_group.get_group(customer_key)

    # Pick address as File name
    # in case There is more than 1 address in that customer
    addresses = naber_df.groupby('Address 1').indices.keys()
    addresses = [re.sub('[^a-zA-Z0-9]+', '_', address_name) for address_name in addresses]
    address_name = addresses[0]


    # 4138.003, 4138.004, 4138.005, 4138.006, 4138.007, 4138.008

    # df['col_3'] = df.apply(lambda x: f(x.col_1, x.col_2), axis=1)
    def cal_weight(status, waste_type, qty_serviced, bin_volume, weight):
        
        if weight == 0:
            if status == 'C':
        #         waste type =>  Food Waste => Organics / Cardboard => Paper/Cardboard
                if waste_type == 'Food Waste':
                    return qty_serviced * waste_sd['Organics'][str(bin_volume)]
            
                elif waste_type == 'Cardboard' or waste_type == 'Paper/Cardboard':
                    if str(bin_volume) == '3.0':
                        return qty_serviced*waste_sd['Paper/Cardboard']['3']
                    else:
                        return qty_serviced*waste_sd['Paper/Cardboard'][str(bin_volume)]
                elif waste_type == 'Secure Bin':
                    return qty_serviced*waste_sd['Paper'][str(bin_volume)]
                    
                return qty_serviced*waste_sd[waste_type][str(bin_volume)]
            else:
                return np.nan
        else:
            return weight

    naber_df['Total weight picked up - kg (if available)'] = (naber_df.apply(
                                                                lambda df: cal_weight(
                                                                    df['Status'],
                                                                    df['Waste Type'],
                                                                    df['Qty Serviced'],
                                                                    df['Bin Volume'],
                                                                    df['Weight']
                                                                ), 
                                                                axis=1
                                                            ))

    
    def facility(waste_type):
        facility = {
            'General Waste' : 'Veolia Waste Centre Clyde',
            'Dry Waste' : 'Veolia Waste Centre Clyde',
            'Food Waste' : 'Eathpower Technologies',
            'Organics' : 'Eathpower Technologies',
            'Paper/Cardboard' : 'Grima Recycling',
            'Cardboard' : 'Grima Recycling',
            'Comingle' : 'Visy Recycling',
            'Secure Bin' : 'Grima Recycling'
        }
        return facility[waste_type]
    naber_df["Processing Facility sent to (Optional)"] = naber_df['Waste Type'].transform(facility)


    def standardize_waste_type(waste_type):
        if waste_type == 'Food Waste':
            return 'Organics'
        elif waste_type == 'Cardboard' or waste_type == 'Paper/Cardboard':
            return 'Paper & Cardboard'
        elif waste_type == 'Comingle':
            return 'Mixed recycling'
        return waste_type
        
    naber_df['Waste Type'] = (naber_df.apply(lambda df: standardize_waste_type(df['Waste Type']), axis=1))


    naber_df["Equipment"] = "Mobile bin"
    naber_df["Bin Rejected due to contamination (Optional)"] = np.nan

    naber_df['Dock (can omit if only 1)'] = np.nan
    naber_df['Version:3'] = np.nan
    
    # City	State	PostCode
    
    naber_df['Site Address (StreetNumber Street Name, Suburb, State, Postcode)'] = naber_df[['Address 1','City','State','PostCode']].agg(','.join, axis=1)
# ==========================================================================
    # Sort by Date

    # naber_df['Date'] = pd.to_datetime(naber_df['Date'], format='%d/%m/%y')

    naber_df['Date'] = pd.to_datetime(naber_df['Date'], format='%d/%m/%Y')
    
    naber_df.sort_values(["Date"],ascending=True, inplace=True)

    naber_df['Date'] = naber_df['Date'].dt.date
    

    # naber_df['Date'] = pd.DatetimeIndex(df['Date'])
    # df.set_index(df['Date'], inplace=False)
    def is_fut_bin(status, served_bins):
        if status == 'F' or status == 'V':
            return 0
        return served_bins

    
    naber_df['Units Collected'] = naber_df.apply(lambda df : is_fut_bin(df['Status'], df['Qty Serviced']),axis=1)

    # 'Qty Serviced' : 

    naber_df = (naber_df
    .rename(columns={
        'Date' : 'Pick-up Date (dd/mm/yyyy)',
        # 'Address 1': 'Site Address',
        'Bin Volume' : 'Size (m3)'
        })
    )

    # naber_df = (naber_df[
    #     ['Pick-up Date (dd/mm/yyyy)',
    #     'Site Address',
    #     'Waste Type',
    #     'Equipment',
    #     'Size (m3)',
    #     'Units Collected',
    #     'Total weight picked up - kg (if available)',
    #     'Processing Facility sent to (Optional)',
    #     'Bin Rejected due to contamination (Optional)',
        
    #     ]])

    fileExt = ".xlsx"
#  + fileExt
    
    naber_df.to_excel(Path(str(address_name)+"_"+str(customer_key)+fileExt),sheet_name=str(customer_key),columns=[
        'Pick-up Date (dd/mm/yyyy)',
        'Site Address (StreetNumber Street Name, Suburb, State, Postcode)',
        'Dock (can omit if only 1)',
        'Waste Type',
        'Equipment',
        'Size (m3)',
        'Units Collected',
        'Total weight picked up - kg (if available)',
        'Processing Facility sent to (Optional)',
        'Bin Rejected due to contamination (Optional)',
        'Version:3'
        ], index=False)


