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
    legacyPath = "/home/pete/pysystemtrade/sysdata/legacycsv/"
    today = time.strftime("%Y%m%d")
    errorFile = path + 'Logs/' + today + "download_errors" + ".txt"

    dataFilename = path + 'marketdata.csv'
    mdf = pd.read_csv(dataFilename, dtype={'CARRY': str, 'PRICE': str})

    # check for carry and price maturities
    row = mdf.loc[mdf['CAVER'] == market]
    #print(row.iloc[0]['CARRY'])

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

def updateFX(market):

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







markets = ["KR3","V2X","EDOLLAR","MXP","CORN","EUROSTX","GAS_US","PLAT","US2","LEANHOG","GBP","VIX","CAC","COPPER","CRUDE_W","BOBL","WHEAT","JPY","NASDAQ","GOLD","US5","SOYBEAN","AUD","SP500","PALLAD","KR10","LIVECOW"]
fxmarkets = ["GBPUSD", "KRWUSD", "EURUSD"]
#markets = ["AUD"]


for market in markets:
    updateMarket (market)

for market in fxmarkets:
    updateFX(market)
