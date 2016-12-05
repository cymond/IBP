
import pandas as pd
import numpy as np
import os


def sanity_check_market(market):
    price_file = legacy_path + market + '_price.csv'
    carry_file = legacy_path + market + '_carrydata.csv'
    price_df = pd.read_csv(price_file)
    price_df = price_df.set_index('DATETIME').copy()
    carry_df = pd.read_csv(carry_file)
    carry_df = carry_df.set_index('DATETIME').copy()
    if price_df.index.is_monotonic and price_df.index.is_unique :
        print("Price file OK for market ", market)
    else:
        print("No for price ", market)
    if carry_df.index.is_monotonic and carry_df.index.is_unique:
        print("Carry file OK for market ", market)
    else:
        print("No for Carry ", market)
        print(carry_df[carry_df.index.duplicated(keep=False)])

markets = ["V2X","GAS_US","VIX","CAC","GOLD",
           "US2", "US5","EDOLLAR","MXP","CORN","EUROSTX",
           "PLAT","LEANHOG","GBP","COPPER","CRUDE_W",
           "BOBL","WHEAT","JPY","NASDAQ","SOYBEAN","AUD",
           "SP500","PALLAD","LIVECOW","KR3", "KR10"]
print("Hello")
dir_filename = "../../data/admin/directories.csv"
if os.path.isfile(dir_filename):
    df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
    legacy_path = df.loc['DESTINATION'][0]
    print(legacy_path)
    for market in markets:
        print()
        sanity_check_market(market)
