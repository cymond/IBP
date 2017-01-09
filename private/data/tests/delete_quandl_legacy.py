import pandas as pd
import time
import os

def deleteMarket(market, date_to):

    print("==========================================================================================")
    print("Starting Update for.... ", market)
    path = '/home/pete/Documents/Python Packages/sysIB/private/data/admin/'
    legacyPath = "/home/pete/Repos/pysystemtrade/private/SystemR/data_test/"


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








#markets = [ "V2X", "EDOLLAR", "MXP", "CORN", "EUROSTX", "GAS_US", "PLAT", "US2", "LEANHOG", "GBP", "VIX", "CAC",
#           "COPPER", "CRUDE_W", "BOBL", "WHEAT", "JPY", "NASDAQ", "GOLD", "US5", "SOYBEAN", "AUD", "SP500", "PALLAD",
#           "LIVECOW"]

markets = [ "AUD", "BOBL"]

delete_date = '2016-10-06'

for market in markets:
    deleteMarket (market, delete_date)



'''

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
'''