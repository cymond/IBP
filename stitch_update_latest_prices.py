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
    print("==========================================================================================")
    print("Starting Update for.... ", market)
    path = '/home/pete/Documents/Python Packages/sysIB/private/data/downloads/quandl/'
    legacyPath = "/home/pete/pysystemtrade/sysdata/legacycsv/"
    today = time.strftime("%Y%m%d")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"

    dataFilename = '/home/pete/Documents/IBDocs/marketdata_stitch.csv'
    mdf = pd.read_csv(dataFilename, dtype={'CARRY': str, 'PRICE': str})

    # check for carry and price maturities
    row = mdf.loc[mdf['CAVER'] == market]
    # print(row.iloc[0]['CARRY'])

    carryMaturity = row.iloc[0]['CARRY']
    priceMaturity = row.iloc[0]['PRICE']
    carryMaturity = carryMaturity[0:6]  # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!
    priceMaturity = priceMaturity[0:6]  # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!
    print("Carry Maturity: ", carryMaturity)
    print("Price Maturity: ", priceMaturity)

    # Get current price and carry files and check dates...

    priceFile = legacyPath + market + '_price.csv'
    carryFile = legacyPath + market + '_carrydata.csv'

    priceDf = pd.read_csv(priceFile)
    carryDf = pd.read_csv(carryFile,dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    priceDf = priceDf.set_index('DATETIME').copy()
    carryDf = carryDf.set_index('DATETIME').copy()
    lastPrice = priceDf.iloc[-1:].index[0]
    lastCarry = carryDf.iloc[-1:].index[0]

    print("lastPrice: ", lastPrice)
    print("lastCarry: ", lastCarry)

    print("== Updating Price Series =======================================")
    # Read and check Price maturity download File and append any new lines to priceDF and overwrite existing file....

    fPriceFile = path + market + '/' + priceMaturity + '.csv'
    if os.path.isfile(fPriceFile):
        sourcePriceDF = pd.read_csv(fPriceFile, usecols=[0, 1])
        #Check if sourcePriceDF contains any newer rows...
        sourcePriceDF.columns = ['DATETIME','PRICE']
        newPriceDF = sourcePriceDF.set_index('DATETIME').copy()
        newPriceDF = newPriceDF.loc[lastPrice:][1:]
        if newPriceDF.empty:
            print(market, ": Data is up to date...", lastPrice)
            return
        print(newPriceDF)
        print("****************************************")

        priceDf = (priceDf).append(newPriceDF)
        print(priceDf) # updated dataFrame
        print("Writing to file: ", priceFile)
        priceDf.to_csv(priceFile)
        print("== Now updating Carry File =======================================")
        # Create dataframe to write back to carry file
        # Check for Carry maturity
        fCarryFile = path + market + '/' + carryMaturity + '.csv'
        if os.path.isfile(fCarryFile):
            sourceCarryDF = pd.read_csv(fCarryFile, usecols=[0, 1])
            # Check if sourceCarryDF contains newer rows...
            sourceCarryDF.columns = ['DATETIME', 'PRICE']
            newCarryDF = sourceCarryDF.set_index('DATETIME').copy()
            newCarryDF = (newCarryDF.loc[lastPrice:][1:])
            if newCarryDF.empty:
                print("No carry data...")
            else:
                print(newCarryDF)
                print("****************************************")
                dfConcat = pd.concat([newPriceDF, newCarryDF], axis=1)
                dfConcat.columns = ["PRICE", "CARRY"]
                dfConcat["CARRY_CONTRACT"] = carryMaturity
                dfConcat["PRICE_CONTRACT"] = priceMaturity
                #dfConcat.index.names = ["DATETIME"]
                print(dfConcat)
                print(carryDf)
                carryDf = (carryDf).append(dfConcat)
                print(carryDf)
                carryDf.to_csv(carryFile)
        else:
            print("No carry file...") # Blanks in carry column!
            dfConcat = newPriceDF
            dfConcat.columns = ["PRICE"]
            dfConcat["CARRY"] = ""
            dfConcat["CARRY_CONTRACT"] = carryMaturity
            dfConcat["PRICE_CONTRACT"] = priceMaturity
            print(dfConcat)
            print(carryDf)
            carryDf = (carryDf).append(dfConcat)
            print(carryDf)
            carryDf.to_csv(carryFile)
    else:
        print("Cannot open file ", fPriceFile)
        #Needs to be put in

markets = ["V2X","GAS_US","VIX","CAC","GOLD", "US2", "US5","EDOLLAR","MXP","CORN","EUROSTX","PLAT","LEANHOG","GBP","COPPER","CRUDE_W","BOBL","WHEAT","JPY","NASDAQ","SOYBEAN","AUD","SP500","PALLAD","LIVECOW"]



for market in markets:
    updateMarket (market)

