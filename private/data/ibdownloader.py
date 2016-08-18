from wrapper_v2 import IBWrapper, IBclient
from swigibpy import Contract as IBcontract
import pandas as pd
import time

if __name__=="__main__":

    """
    ===================================================================================================
    Author: P. Kawuki
    Date: 20160730
    Description: Download market data given by marketdata.csv
    ===================================================================================================
    """
    callback = IBWrapper()
    client = IBclient(callback)

    dir_filename = "../data/admin/directories.csv"
    df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
    source_path = df.loc['SOURCE'][0]
    destination_path = df.loc['DESTINATION'][0]

    today = time.strftime("%Y%m%d")
    time_now = time.strftime("%Y%m%d %H:%M:%S")

    error_filename = source_path + 'logs/' + today + "download_errors" + ".txt"
    data_filename = source_path + 'admin/' + 'marketdata.csv'
    market_data_df = pd.read_csv(data_filename)
    print(market_data_df)
    counter = 100

    for row in market_data_df.itertuples():

        print()
        print("IB Symbol: ", row[2], "row[6]:", row[6], " row[7]: ", row[7])
        print()
        counter = counter + 1
        subPath = source_path + 'downloads/' + row[1] + '/'

        ibcontract = IBcontract()
        ibcontract.symbol = row[2]  # IB Symbol
        ibcontract.secType = row[3]  # Security Type
        ibcontract.currency = row[4]  # Currency
        ibcontract.exchange = row[5]  # Exchange
        if row[8] > 0:
            ibcontract.multiplier = str(row[8])

        collection = [row[6], row[7]]

        for mat in collection:
            print(str(mat))
            if mat > 0 :
                ibcontract.expiry = str(mat)  # CARRY
                print()
                print("retrieving.... " + row[2] + " / " + str(mat))
                ans = client.get_IB_historical_data(ibcontract, "1 M", "1 day", counter, "TRADES")
                fileName = subPath + str(mat)[0:6] + ".csv"
            else :
                ans = client.get_IB_historical_data(ibcontract, "1 M", "1 day", counter, "MIDPOINT")
                fileName = subPath + row[2] + row[4] + ".csv"
            if isinstance(ans, pd.DataFrame):
                ans.to_csv(fileName)
                print()
                print(ans)
                print("Saving to file: " + str(counter) + ": " + fileName)
                print()
            else:
                lineString = time_now + "," + row[1] + "," + row[2] + "," + str(mat) + "\n"
                print("============================================================")
                print("Symbol: " + row[2] + " / " + str(mat) + " Error!!!!!!")
                print("============================================================")
                errorFileHandle = open(error_filename, 'a')
                errorFileHandle.write(lineString)  # python will convert \n to os.linesep
                errorFileHandle.close()  # you can omit in most cases as the destructor will call it
            if row[5] == 'IDEALPRO':  # currency, only need to retrieve once!
                break

