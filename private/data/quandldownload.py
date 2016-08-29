import pandas as pd, Quandl, numpy as np, matplotlib.pyplot as plt
import urllib.request
import os

'''
def construct_futures_symbols(symbol, start_year = 2010, end_year=2015):
    """Constructs a list of futures contract codes for a particular symbol
       and timeframe"""

    futures = []
    months = 'GJMNQVZ'  # March, June, September and December delivery codes
    for y in range(start_year, end_year+1):
        for m in months:
            futures.append("%s%s%s" % (symbol, m, y))
    return futures

def download_contract_from_quandl(contract, auth_token, dl_dir):
    """Download an individual futures contract from Quandl and then
       store it to disk in 'dl_dir' directory. An auth_token is
       required!"""

    # Construct the API call from the contract and auth_token

    api_call_head = "CME/%s" % contract
    wti = Quandl.get(api_call_head, returns="pandas", authtoken=auth_token)
    filename = '%s/%s.csv' % (dl_dir, contract)
    print("writing file ...." + filename + " to disk")
    wti.to_csv(filename)

def download_historical_contracts(symbol, auth_token, dl_dir, start_year=2000, end_year = 2014):
    """Downloads all futures contracts for a specified symbol between a start_year
       and an end year."""

    contracts = construct_futures_symbols(symbol, start_year, end_year)
    for c in contracts:
        download_contract_from_quandl(c, auth_token, dl_dir)
'''

def get_month_from_code(code):

    return {
        'F':'01',
        'G':'02',
        'H':'03',
        'J':'04',
        'K':'05',
        'M':'06',
        'N':'07',
        'Q':'08',
        'U':'09',
        'V':'10',
        'X':'11',
        'Z':'12'
    }[code]

if __name__ == "__main__":
    dir_filename = "../data/admin/directories.csv"
    df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
    source_path = df.loc['SOURCE'][0]
    downloads_path = source_path + "downloads/quandl/"

    # Load quandl_marketdata
    market_data_filename = source_path + 'admin/' + 'quandl_marketdata.csv'

    market_data_df = pd.read_csv(market_data_filename,
                                 dtype={'QUANDL': str, 'MONTHS': str})
    new_df = market_data_df.set_index(['CAVER', 'YEAR']).copy()
    #print(new_df.index)
    new_df.sort_index(ascending=True, inplace=True)
    #print(new_df)
    auth_token = "eLJYnHCTAxwn3xXFMZhB"

    for row in new_df.itertuples():
        # Construct the string...
        symbol = row[1]
        year = row[0][1]
        exchange = row[4]
        months = row[5]

        print(row)
        for m in months :
            api_call_head = "%s/%s%s%s" % (exchange, symbol, m, year)
            #print(api_call_head)
            month = get_month_from_code(m)
            filename = source_path + "downloads/quandl/" + str(row[0][0]) + "/" + str(year) + month + ".csv"
            #print(filename)
            #if api_call_head == "EUREX/FVSQ2016":
            result = Quandl.get(api_call_head, returns="pandas", authtoken=auth_token)
            cols = list(result)
            if exchange == "EUREX":
                cols[3], cols[0] = cols[0], cols[3]
            else :
                if exchange == "CBOE":
                    cols[4], cols[0] = cols[0], cols[4]
                else:
                    if exchange == "lIFFE":
                        cols[3], cols[0] = cols[0], cols[3]
                    else:
                        cols[5], cols[0] = cols[0], cols[5]
            result = result.ix[:, cols]
            print("!!!!! Check if columns changed!!!!")
            print(result)
            result.to_csv(filename)


        #
    # Download the contracts into the directory
    #download_historical_contracts(symbol, auth_token, dl_dir, start_year, end_year)





