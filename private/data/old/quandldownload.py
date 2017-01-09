import pandas as pd, Quandl, numpy as np, matplotlib.pyplot as plt
import urllib.request
import os
import time

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

    today = time.strftime("%Y%m%d")
    time_now = time.strftime("%Y%m%d %H:%M:%S")


    dir_filename = "../data/admin/directories.csv"
    if os.path.isfile(dir_filename):
        df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
        source_path = df.loc['SOURCE'][0]
        error_filename = source_path + 'logs/' + today + "download_errors" + ".csv"

        downloads_path = source_path + "downloads/quandl/"
        market_data_filename = source_path + 'admin/' + 'quandl_marketdata.csv'
        if os.path.isfile(market_data_filename):
            market_data_df = pd.read_csv(market_data_filename, dtype={'QUANDL': str, 'MONTHS': str})
            new_df = market_data_df.set_index(['CAVER', 'YEAR']).copy()
            new_df.sort_index(ascending=True, inplace=True)
            auth_token = "eLJYnHCTAxwn3xXFMZhB"

            for row in new_df.itertuples():
                # Construct the string...6
                symbol = row[1]
                year = row[0][1]
                exchange = row[4]
                months = row[5]


                for m in months :
                    api_call_head = "%s/%s%s%s" % (exchange, symbol, m, year)
                    month = get_month_from_code(m)
                    result = Quandl.get(api_call_head, returns="pandas", authtoken=auth_token)
                    cols = list(result)
                    cols.insert(0, cols.pop(cols.index('Settle')))
                    result = result.ix[:, cols]
                    if str(row[0][0]) == 'MXP' or str(row[0][0]) == 'JPY':
                        result['Settle'] = result['Settle'].apply(lambda x: x / 1000000)
                    filename = source_path + "downloads/quandl/" + str(row[0][0]) + "/" + str(year) + month + ".csv"
                    print(result.iloc[-1].name, "::" + str(row[0][0]) + "/" + str(year) + month)
                    result.to_csv(filename)
        else:
            print("Error: File quandl_marketdata.csv does not exist")
            lineString = "Error: File quandl_marketdata.csv does not exist"
            errorFileHandle = open(error_filename, 'a')
            errorFileHandle.write(lineString)  # python will convert \n to os.linesep
            errorFileHandle.close()
    else:
        print("Error! The file **directories.csv** is not accessible!")



