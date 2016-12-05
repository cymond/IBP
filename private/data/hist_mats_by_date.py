
import pandas as pd
import time
import os

def find_mats(market, date_to):
    legacy_path = "/home/pete/pysystemtrade/sysdata/legacycsv/"
    marketdata_path = '/home/pete/Documents/Python Packages/sysIB/private/data/admin/'
    marketdata_file = marketdata_path + 'marketdata.csv'
    marketdata_df = pd.read_csv(marketdata_file, dtype={'CARRY': str, 'PRICE': str})
    #print(marketdata_df)
    row = marketdata_df.loc[marketdata_df['CAVER'] == market]

    legacy_carry_file = legacy_path + market + '_carrydata.csv'
    carry_df = pd.read_csv(legacy_carry_file, dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    carry_df = carry_df.set_index('DATETIME').copy()
    new_carry_df =  (carry_df.loc[:date_to]).copy()
    line_string = market + "," + new_carry_df.iloc[-1]['CARRY_CONTRACT'] + \
                   "," + new_carry_df.iloc[-1]['PRICE_CONTRACT']
    out_file_handle = open(out_filename, 'a')
    out_file_handle.write(line_string)  # python will convert \n to os.linesep
    out_file_handle.close()  # you can omit in most cases as the destructor will call it
    print(line_string)


dir_filename = "../data/admin/directories.csv"
df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
output_path = df.loc['SOURCE'][0]
legacy_path = df.loc['DESTINATION'][0]

today = time.strftime("%Y%m%d")
time_now = time.strftime("%Y%m%d %H:%M:%S")

date = '2016-09-12'
out_filename = output_path + 'tests/' + "historical_mats" + date + ".csv"

markets = ["V2X","GAS_US","VIX","CAC","GOLD", "US2",
           "US5","EDOLLAR","MXP","CORN","EUROSTX","PLAT",
           "LEANHOG","GBP","COPPER","CRUDE_W","BOBL","WHEAT",
           "JPY","NASDAQ","SOYBEAN","AUD","SP500","PALLAD","LIVECOW"]

for market in markets:
    find_mats (market, date)