import pandas as pd
import time
import os

def updateMarket(market):
    # Read marketdata to check
    # 1. date from
    # 2. carry and
    # 3, price maturity
    # Access source files and add all lines with valid price data... carry can be left blank
    # Report if no new data... or for each row with blank carry...
    print("==================================")
    #print(market, "... checking for updates ")
    path = '/home/pete/Documents/Python Packages/sysIB/private/data/downloads/quandl/'
    legacyPath = "/home/pete/Repos/pysystemtrade/private/SystemR/data/"
    today = time.strftime("%Y%m%d")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"
    dataFilename = '/home/pete/Documents/Python Packages/sysIB/private/data/admin/marketdata_stitch.csv'
    #dataFilename = '/home/pete/Documents/IBDocs/marketdata_stitch.csv'
    mdf = pd.read_csv(dataFilename, dtype={'CARRY': str, 'PRICE': str})

    # check for carry and price maturities
    row = mdf.loc[mdf['CAVER'] == market]
    # print(row.iloc[0]['CARRY'])

    carryMaturity = row.iloc[0]['CARRY']
    priceMaturity = row.iloc[0]['PRICE']
    carryMaturity = carryMaturity[0:6]  # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!
    priceMaturity = priceMaturity[0:6]  # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!

    # Get current price and carry files and check dates...

    priceFile = legacyPath + market + '_price.csv'
    legacyCarryFile = legacyPath + market + '_carrydata.csv'

    priceDf = pd.read_csv(priceFile)
    carryDf = pd.read_csv(legacyCarryFile,dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    priceDf = priceDf.set_index('DATETIME').copy()
    carryDf = carryDf.set_index('DATETIME').copy()
    lastPrice = priceDf.iloc[-1:].index[0]
    lastCarry = carryDf.iloc[-1:].index[0]

    print("date last Price: ", lastPrice)
    #print("date last Carry: ", lastCarry)

    #print("== Price file ==")
    # Read and check Price maturity download File and append any new lines to priceDF and overwrite existing file....

    fPriceFile = path + market + '/' + priceMaturity + '.csv'
    if os.path.isfile(fPriceFile):
        sourcePriceDF = pd.read_csv(fPriceFile, usecols=[0, 1])
        #Check if sourcePriceDF contains any newer rows...
        sourcePriceDF.columns = ['DATETIME','PRICE']
        newPriceDF = sourcePriceDF.set_index('DATETIME').copy()
        newPriceDF = newPriceDF.loc[lastPrice:][1:]
        if newPriceDF.empty:
            print(market, ": No newer rows...", lastPrice)
            return
        print(market, ": Price rows to add...")
        print(newPriceDF)
        print()

        priceDf = (priceDf).append(newPriceDF)
        priceDf.to_csv(priceFile)
        #print("== Carry File ==")
        fCarryFile = path + market + '/' + carryMaturity + '.csv'
        matchedPriceDF = sourcePriceDF.set_index('DATETIME').copy()
        matchedPriceDF = (matchedPriceDF.loc[lastCarry:][1:])
        if os.path.isfile(fCarryFile):
            sourceCarryDF = pd.read_csv(fCarryFile, usecols=[0, 1])
            # Check if sourceCarryDF contains newer rows...
            sourceCarryDF.columns = ['DATETIME', 'PRICE']
            newCarryDF = sourceCarryDF.set_index('DATETIME').copy()
            newCarryDF = (newCarryDF.loc[lastPrice:][1:])
            if newCarryDF.empty:
                #print("No carry data...")
                dfConcat = matchedPriceDF
                dfConcat["CARRY"] = ""
            else:
                dfConcat = pd.concat([matchedPriceDF, newCarryDF], axis=1)
            dfConcat.columns = ["PRICE", "CARRY"]
            dfConcat["CARRY_CONTRACT"] = carryMaturity
            dfConcat["PRICE_CONTRACT"] = priceMaturity
            #print(market, ": Carry rows to add...")
            #print(dfConcat)
            carryDf = (carryDf).append(dfConcat)
            carryDf.to_csv(legacyCarryFile)
        else:
            #print("No carry file...") # Blanks in carry column!
            dfConcat = newPriceDF
            dfConcat.columns = ["PRICE"]
            dfConcat["CARRY"] = ""
            dfConcat["CARRY_CONTRACT"] = carryMaturity
            dfConcat["PRICE_CONTRACT"] = priceMaturity
            print(market, ": Carry rows to add...")
            print(dfConcat)
            carryDf = (carryDf).append(dfConcat)
            carryDf.to_csv(legacyCarryFile)
    else:
        print("*********** Cannot open file ", fPriceFile)
        #Needs to be put in

markets = ["V2X","GAS_US","CAC","GOLD", "US2", "US5","EDOLLAR",
           "MXP","CORN","EUROSTX","PLAT","LEANHOG","GBP",
           "COPPER","CRUDE_W","BOBL","WHEAT","JPY","NASDAQ",
           "SOYBEAN","AUD","SP500","PALLAD","LIVECOW"]

for market in markets:
    updateMarket (market)

