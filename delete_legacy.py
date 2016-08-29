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
    path = '/home/pete/Documents/IBDocs/'
    legacyPath = "/home/pete/pysystemtrade/sysdata/legacycsv/"
    today = time.strftime("%Y%m%d")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"

    dataFilename = path + 'marketdata.csv'
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
    legacyPath = "/home/pete/pysystemtrade/sysdata/legacycsv/"
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





markets = ["KR3","V2X","EDOLLAR","MXP","CORN","EUROSTX","GAS_US","PLAT","US2","LEANHOG","GBP","VIX","CAC","COPPER","CRUDE_W","BOBL","WHEAT","JPY","NASDAQ","US5","SOYBEAN","AUD","SP500","PALLAD","KR10","LIVECOW"]
fxmarkets = ["GBPUSD", "KRWUSD", "EURUSD"]

delete_date = '2016-08-07'

for market in markets:
    deleteMarket (market, delete_date)

for market in fxmarkets:
    deleteFXmarket (market, delete_date)
