import pandas as pd
import time
import os

def deleteMarket(market, date_to):
    # Read marketdata to check
    # 1. date from
    # 2. carry and
    # 3, price maturity
    # Access source files and add all lines with valid price data... carry can be left blank
    # Report if no new data... or for each row with blank carry...
    print("==========================================================================================")
    print("Starting Update for.... ", market)
    path = '/home/pete/Documents/Python Packages/sysIB/private/data/admin/'
    legacyPath = "/home/pete/pysystemtrade/sysdata/legacycsv_test/"
    today = time.strftime("%Y%m%d")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"

    dataFilename = path + 'marketdata_test.csv'
    mdf = pd.read_csv(dataFilename, dtype={'CARRY': str, 'PRICE': str})
    print(mdf)
    # check for carry and price maturities
    row = mdf.loc[mdf['CAVER'] == market]
    print(row.iloc[0]['CARRY'])

    carryMaturity = row.iloc[0]['CARRY']
    priceMaturity = row.iloc[0]['PRICE']
    print("Carry Maturity: ", carryMaturity)
    print("Price Maturity: ", priceMaturity)


    # Get current price and carry files and check dates...

    priceFile = legacyPath + market + '_price.csv'
    carryFile = legacyPath + market + '_carrydata.csv'

    priceDf = pd.read_csv(priceFile)
    carryDf = pd.read_csv(carryFile,dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    priceDf = priceDf.set_index('DATETIME').copy()
    carryDf = carryDf.set_index('DATETIME').copy()
    print(priceDf)
    print(carryDf)
    newPriceDf = priceDf.copy()
    newCarryDf = carryDf.copy()
    newPriceDf = (newPriceDf.loc[:date_to])
    newCarryDf = (newCarryDf.loc[:date_to])
    print("NEW..............................")
    print(newPriceDf)
    print(newCarryDf)
    newPriceDf.to_csv(priceFile)
    newCarryDf.to_csv(carryFile)

def deleteFXmarket(market, date_to):

    # Read fxdata to check
    # 1. For each rate check for new data
    # 2. Access source file and add corresponding rows.
    # Report if no new data... or any other erros
    path = '/home/pete/Documents/IBDocs/'
    legacyPath = "/home/pete/pysystemtrade/sysdata/legacycsv_test/"
    today = time.strftime("%Y%m%d")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"

    dataFilename = path + 'fxdata.csv'
    print("dataFilename: ", dataFilename)
    mdf = pd.read_csv(dataFilename)
    print(mdf)

    #
    row = mdf.loc[mdf['CAVER'] == market]
    print("row: ", row)
    # Get corresponding fx rate file...

    ratePair = row.iloc[0]['CAVER']
    print("ratePair: ", ratePair)

    fxRateFile = legacyPath + ratePair + 'fx.csv'

    print("fxRateFile: ", fxRateFile)
    fxRateDF = pd.read_csv(fxRateFile)

    fxRateDF = fxRateDF.set_index('DATETIME').copy()
    newfxRateDF = fxRateDF.copy()
    newfxRateDF = (newfxRateDF.loc[:date_to])

    print("NEW..............................")
    print(newfxRateDF)
    newfxRateDF.to_csv(fxRateFile)



# Get current price and carry files and check dates...
def print_market_last_date(instrument):
    legacyPath = "/home/pete/pysystemtrade/sysdata/legacycsv_test/"
    priceFile = legacyPath + instrument + '_price.csv'
    carryFile = legacyPath + instrument + '_carrydata.csv'

    priceDf = pd.read_csv(priceFile)
    carryDf = pd.read_csv(carryFile,dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    priceDf = priceDf.set_index('DATETIME').copy()
    carryDf = carryDf.set_index('DATETIME').copy()

    price_date = priceDf.iloc[-1:].index[0]
    carry_date = carryDf.iloc[-1:].index[0]
    print(instrument, ":", "Price Date: ", price_date, "Carry Date: ", carry_date )

markets = ["V2X","GAS_US","VIX","CAC","GOLD",
           "US2", "US5","EDOLLAR","MXP","CORN",
           "EUROSTX","PLAT","LEANHOG","GBP","COPPER",
           "CRUDE_W","BOBL","WHEAT","JPY","NASDAQ",
           "SOYBEAN","AUD","SP500","PALLAD","LIVECOW",
            "KR3", "KR10",]

#markets = ["BOBL","AUD"]
fxmarkets = ["GBPUSD", "KRWUSD", "EURUSD"]
'''
delete_date = '2016-09-15'

for market in markets:
    deleteMarket (market, delete_date)

for market in fxmarkets:
    deleteFXmarket (market, delete_date)
'''

print("==================================================================================")

for market in markets:
    print()
    print_market_last_date(market)