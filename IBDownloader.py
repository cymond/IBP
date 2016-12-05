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
    path = '/home/pete/Documents/IBDocs/'
    today = time.strftime("%Y%m%d")
    time_now = time.strftime("%Y%m%d %H:%M:%S")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"

    dataFilename = path + 'marketdata.csv'
    mdf = pd.read_csv(dataFilename)
    #print(mdf)
    counter = 100

    for row in mdf.itertuples():
        #print()
        #print(row)
        #print()
        #print("row[6]:", row[6], " row[7]: ", row[7])
        print()
        counter = counter + 1
        subPath = path + row[1] + '/'

        ibcontract = IBcontract()
        ibcontract.symbol = row[2]  # IB Symbol
        ibcontract.secType = row[3] # Security Type
        ibcontract.currency = row[4] # Currency
        ibcontract.exchange = row[5] # Exchange
        if row[8] > 0:
            ibcontract.multiplier = str(row[8])

        collection = [row[6], row[7]]

        for mat in collection:

            ibcontract.expiry = str(mat)  # CARRY
            #print()
            #print("retrieving.... " + row[2] + " / " + str(mat) )
            ans = client.get_IB_historical_data(ibcontract, "3 M", "1 day", counter)
            if isinstance(ans, pd.DataFrame):
                fileName = subPath + str(mat)[0:6] + ".csv"
                ans.to_csv(fileName)
                print(row[1], '/',str(mat))
                print(ans.tail(1))
                print("Saving to file: " + str(counter) + ": " + fileName)
                print()
            else:
                lineString = time_now + "," + row[1] + "," + row[2] + "," + str(mat) + "\n"
                #print("============================================================")
                print(row[1] + " / " + str(mat) + " errored!!!!!!")
                print("============================================================")
                errorFileHandle = open(errorFile, 'a')
                errorFileHandle.write(lineString)  # python will convert \n to os.linesep
                errorFileHandle.close()  # you can omit in most cases as the destructor will call it




































        """
           marketData = [('KR3','3KTB', 'FUT','KRW','KSE','201609', '201612',''),
                         ('EUROSTX', 'ESTX50', 'FUT', 'EUR', 'DTB', '201609', '201612', '')
                         ]
           mdf = pd.DataFrame(marketData)

           """

        '''
        ibcontract.expiry = str(row[7])  # CARRY
        print("retrieving.... " + row[2] + " / " + str(row[7]))
        ans = client.get_IB_historical_data(ibcontract, "1 M", "1 day", counter)
        if isinstance(ans, pd.DataFrame):

            fileName = path + str(row[7]) + ".csv"
            ans.to_csv(fileName)
            print()
            print("File: " + str(counter) + ": " + fileName)
            print(ans)
        else:
            print("============================================================")
            print("Symbol: " + row[2] + " / " + str(row[7]) + "Errored!!!!!!")
            print("============================================================")

            # Diagnostics





    ibcontract = IBcontract()
    ibcontract.secType =    "FUT"
    ibcontract.expiry = "201609"
    ibcontract.currency =    "KRW"
    ibcontract.symbol =  "3KTB"
    ibcontract.exchange = "KSE"
    ans=client.get_IB_historical_data(ibcontract)
    print(ans)

    ,
                   ('EUROSTX','ESTX50','FUT','EUR','DTB','201609','201612','')

    '''
    """
        path = '/home/pete/Documents/IBDocs/' + row[1] + '/'
        #ans.to_csv(path + ibcontract.symbol + "".csv")
        ans.to_csv(path + row[6] + ".csv")


EXCHANGE RATES
    # GBP.USD  - OK
    ibcontract = IBcontract()
    ibcontract.secType = "CASH"
    ibcontract.currency = "USD"
    ibcontract.symbol = "GBP"
    ibcontract.exchange = "IDEALPRO"
    ibcontract.primaryExchange = "IDEALPRO"

    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry = "201607"
    ibcontract.currency = "EUR"
    ibcontract.symbol = "V2TX"
    ibcontract.exchange = "DTB"
    #ibcontract.multiplier = "10"

    #V2X - NONONONO!!!!!!!!!!!
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201607"
    ibcontract.currency="EUR"
    ibcontract.symbol="V2TX"
    ibcontract.exchange="DTB"

    #V2X - NONONONO!!!!!!!!!!!
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201609"
    ibcontract.currency="GBP"
    ibcontract.symbol="R"
    ibcontract.exchange="ICEEU

    #KR3 - OK
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201609"
    ibcontract.currency="KRW"
    ibcontract.symbol="3KTB"
    ibcontract.exchange="KSE"

    #CORN  - OK
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201612"
    ibcontract.currency="USD"
    ibcontract.symbol="ZC"
    ibcontract.exchange="ECBOT"

    #V2X - NONONONO
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201609"
    ibcontract.currency="GBP"
    ibcontract.symbol="R"
    ibcontract.exchange="ICEEU"
"""