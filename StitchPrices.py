import pandas as pd
import os
import time

'''
    ===================================================================================================
    Author: P. Kawuki
    Date: 20160730
    Description: Stich the data files using date and new rows in specified files to replace old ones in existing
                 PRICE AND CARRY series and to perform the adjustment to existing PRICE series
    ===================================================================================================
'''
source_path = '/home/pete/Documents/IBDocs/'
destination_path = "/home/pete/pysystemtrade/sysdata/legacycsv/"

today = time.strftime("%Y%m%d")
errorFile = source_path + 'Logs/' + today + "download_errors" + ".txt"


def get_stitch_row(symbol):
    # Read the file containg the stitch dates and handles for new maturities
    stitchFile = path + "stitchdata.csv"
    if os.path.isfile(stitchFile):
        stichDF = pd.read_csv(stitchFile)
        row = stichDF.loc[stichDF['CARVER'] == symbol]
        return row
    else:
        print("E: The file ", stitchFile, " cannot be opened")


def get_stitched_Price( stitchdate, newCarryContract, newPriceContract):

    newpricefile = newPricePath + str(newPriceContract) + '.csv'
    oldpricefile = destination_path + market + '_price.csv'

    newPriceDF = pd.read_csv(newpricefile,  usecols=[0, 1], index_col= 0, dayfirst=True)
    newPriceDF.rename(columns={'close': 'PRICE'}, inplace=True)
    newPriceDF.index.name = 'DATETIME'

    oldPriceDF = pd.read_csv(oldpricefile)
    oldPriceDF = oldPriceDF.set_index('DATETIME').copy()


    #Perform the PRICE stitching
    old_value = newPriceDF.loc[stitchdate]['PRICE']
    new_value = oldPriceDF.loc[stitchdate]['PRICE']
    delta = new_value - old_value
    print("Delta: ", delta)
    print(oldPriceDF)
    oldPriceDF['PRICE'] = oldPriceDF['PRICE'].apply(lambda x: x + delta)
    #TEST: Panama stitch oldPriceDF with newPriceDF and write the file to a test place for testing
    oldPriceDF = oldPriceDF[:stitchdate][:-1]
    print(oldPriceDF.tail(5))
    print(newPriceDF.head(5))
    stitchedPriceDF = oldPriceDF.append(newPriceDF[stitchdate:]).copy()
    print(stitchedPriceDF)
    print()
    stitchedPriceDF.to_csv(oldpricefile)

    # Now perform the CARRY stitching ase well...
    # At least 3 columns (PRICE,CARRY,PRICE_CONTRACT)  MUST be updated to match the PRICE series
    # First we must test if there is a new CARRY file. Some contracts do not trade until let in the cycle and
    # IB wont download a corresponding file. Blanks need to be filled in the CARRY columna until then

    oldcarryfile = destination_path + market + '_carrydata.csv'
    oldCarryDF = pd.read_csv(oldcarryfile, dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    oldCarryDF = oldCarryDF.set_index('DATETIME').copy()
    oldCarryDF = oldCarryDF[:stitchdate][:-1]
    newcarryfile = newPricePath + str(newCarryContract) + '.csv'
    if os.path.isfile(newcarryfile):
        newCarryDF = pd.read_csv(newcarryfile, usecols=[0, 1], index_col=0, dayfirst=True)
        newCarryDF.rename(columns={'close': 'PRICE'}, inplace=True)
        newCarryDF.index.name = 'DATETIME'
        dfConcat = pd.concat([newPriceDF[stitchdate:], newCarryDF[stitchdate:]], axis=1)
    else:
        dfConcat = newPriceDF[stitchdate:]
        dfConcat.is_copy = False
        dfConcat["CARRY"] = ""
    dfConcat.columns = ["PRICE", "CARRY"]
    dfConcat["CARRY_CONTRACT"] = newCarryContract
    dfConcat["PRICE_CONTRACT"] = newPriceContract
    carryDf = (oldCarryDF).append(dfConcat)
    print(carryDf)
    carryDf.to_csv(oldcarryfile)

    return


market = 'GOLD'
stitchRow = get_stitch_row(market)
newPricePath = path + market + '/'

stitchDate = stitchRow.iloc[0]['DATETIME']
newCarryContract = stitchRow.iloc[0]['CARRY_CONTRACT']
newPriceContract = stitchRow.iloc[0]['PRICE_CONTRACT']
print("stitch date :", stitchDate, "New Carry Contract: ", newCarryContract, "New Price Contract: ", newPriceContract)


stitchedPrice = get_stitched_Price(stitchDate, newCarryContract, newPriceContract)



#Notes
'''
Structure of stitchdata
CAVER, DATETIME, FRO_PRICE, FRO_CARRY, TO_PRICE, TO_CARRY, DELTA, DONE


Checks before stitching
1. DATETIME date should be before today's date
2. DATETIME date should exist in both FRO_PRICE and TO_PRICE time series

Code should be able to handle stitching whole a Panama stitched price series as long as stitch dates and
ALL the required raw maturities are avaiable.

This stitching starts from the earliest not DONE date to the latest


'''