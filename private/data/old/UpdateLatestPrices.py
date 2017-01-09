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
    path = '/home/pete/Documents/IBDocs/'
    legacyPath = "/home/pete/Repos/pysystemtrade/private/SystemR/data/"
    today = time.strftime("%Y%m%d")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"

    dataFilename = path + 'marketdata.csv'
    mdf = pd.read_csv(dataFilename, dtype={'CARRY': str, 'PRICE': str})

    # check for carry and price maturities
    row = mdf.loc[mdf['CAVER'] == market]
    #print(row.iloc[0]['CARRY'])

    carryMaturity = row.iloc[0]['CARRY']
    priceMaturity = row.iloc[0]['PRICE']
    carryMaturity = carryMaturity[0:6]  # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!
    priceMaturity = priceMaturity[0:6]  # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!
    print("Carry Maturity: ", carryMaturity)
    print("Price Maturity: ", priceMaturity)


    # Get current price and carry files and check dates...

    priceFile = legacyPath + market + '_price.csv'
    legacyCarryFile = legacyPath + market + '_carrydata.csv'

    priceDf = pd.read_csv(priceFile)
    legacyCarryDf = pd.read_csv(legacyCarryFile,dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    priceDf = priceDf.set_index('DATETIME').copy()
    legacyCarryDf = legacyCarryDf.set_index('DATETIME').copy()
    lastPrice = priceDf.iloc[-1:].index[0]
    lastCarry = legacyCarryDf.iloc[-1:].index[0]

    print("lastPrice: ", lastPrice)
    print("lastCarry: ", lastCarry)


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
        #print(newPriceDF)
        #print("****************************************")

        priceDf = (priceDf).append(newPriceDF)
        #print(priceDf) # updated dataFrame
        print("== Updating Price Series =======================================")
        print("Latest date appended: ", newPriceDF.iloc[-1:].index.tolist()[0])
        priceDf.to_csv(priceFile)
        #print("== Now updating Carry File =======================================")
        # Create dataframe to write back to carry file
        # Check for Carry maturity
        fCarryFile = path + market + '/' + carryMaturity + '.csv'
        #print("Carry File: ", fCarryFile)
        matchedPriceDF = sourcePriceDF.set_index('DATETIME').copy()
        matchedPriceDF = (matchedPriceDF.loc[lastCarry:][1:])
        if os.path.isfile(fCarryFile):
            sourceCarryDF = pd.read_csv(fCarryFile, usecols=[0, 1])
            # Check if sourceCarryDF contains newer rows...
            sourceCarryDF.columns = ['DATETIME', 'PRICE']
            newCarryDF = sourceCarryDF.set_index('DATETIME').copy()
            newCarryDF = (newCarryDF.loc[lastCarry:][1:])

            if newCarryDF.empty:
                print("No carry data...")
                dfConcat = matchedPriceDF
                dfConcat["CARRY"] = ""
            else:
         #       print(newCarryDF)
         #       print("****************************************")
                dfConcat = pd.concat([matchedPriceDF, newCarryDF], axis=1)
            dfConcat.columns = ["PRICE", "CARRY"]
            dfConcat["CARRY_CONTRACT"] = carryMaturity
            dfConcat["PRICE_CONTRACT"] = priceMaturity
            #dfConcat.index.names = ["DATETIME"]
        #    print(dfConcat)
        #    print(legacyCarryDf)
            legacyCarryDf = (legacyCarryDf).append(dfConcat)
        #    print(legacyCarryDf)
            legacyCarryDf.to_csv(legacyCarryFile)
        else:
            print("No carry file...") # Blanks in carry column!
            dfConcat = matchedPriceDF
            dfConcat.columns = ["PRICE"]
            dfConcat["CARRY"] = ""
            dfConcat["CARRY_CONTRACT"] = carryMaturity
            dfConcat["PRICE_CONTRACT"] = priceMaturity
         #   print(dfConcat)
         #   print(legacyCarryDf)
            legacyCarryDf = (legacyCarryDf).append(dfConcat)
         #   print(legacyCarryDf)
            legacyCarryDf.to_csv(legacyCarryFile)
    else:
        print("Cannot open file ", fPriceFile)
        #Needs to be put in

def updateFX(market):

    # Read fxdata to check
    # 1. For each rate check for new data
    # 2. Access source file and add corresponding rows.
    # Report if no new data... or any other erros
    path = '/home/pete/Documents/IBDocs/'
    legacyPath = "/home/pete/Repos/pysystemtrade/private/SystemR/data/"
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
    lastFXRate = fxRateDF.iloc[-1:].index[0]
    print("lastFXRate: ", lastFXRate)

    # Now append any new lines...

    # Read and check Price maturity download File and append any new lines to priceDF and overwrite existing file....

    fFXFile = path + ratePair + '/' + ratePair + '.csv'
    print("== Checking if there is new updates for: ", fFXFile)
    if os.path.isfile(fFXFile):
        sourceFXDF = pd.read_csv(fFXFile, usecols=[0, 1])
        # Doew sourceFXDF contains newer rows?
        sourceFXDF.columns = ['DATETIME', 'FX']
        newFXDF = sourceFXDF.set_index('DATETIME').copy()
        newFXDF = newFXDF.loc[lastFXRate:][1:]
        if newFXDF.empty:
            print(market, ": Data is up to date...", lastFXRate)
            return
        print(newFXDF)
        print("****************************************")

        fxRateDF = (fxRateDF).append(newFXDF)
        print(fxRateDF)  # updated dataFrame
        fxRateDF.to_csv(fxRateFile)
        print("== Now updating FX Rate ", market," =======================================")
    else:
        print("Cannot open file ", fFXFile)







markets = ["AUD", "BOBL", "CAC", "COPPER", "CORN", "CRUDE_W", "EDOLLAR", "EUROSTX",
           "GAS_US", "GBP", "GOLD", "JPY", "KR3", "KR10", "LEANHOG", "LIVECOW", "MXP",
           "NASDAQ", "PALLAD", "PLAT", "SOYBEAN", "SP500", "US2", "US5", "V2X", "VIX", "WHEAT"]
fxmarkets = ["GBPUSD", "KRWUSD", "EURUSD"]
#markets = ["AUD"]


for market in markets:
    updateMarket (market)

for market in fxmarkets:
    updateFX(market)
