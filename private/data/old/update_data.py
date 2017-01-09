import pandas as pd
import time
import os

def updateFuturesMarket(market):
    # Read marketdata to check
    # 1. date from
    # 2. carry and
    # 3, price maturity
    # Access source files and add all lines with valid price data... carry can be left blank
    # Report if no new data... or for each row with blank carry...



    print("==========================================================================================")
    print("Begin processing for.... ", market)


    # check for carry and price maturities
    row = market_data_df.loc[market_data_df['CAVER'] == market]
    #print(row.iloc[0]['CARRY'])

    #!!!! Test
    carryMaturity = row.iloc[0]['CARRY']
    priceMaturity = row.iloc[0]['PRICE']
    carryMaturity = carryMaturity[0:6] # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!
    priceMaturity = priceMaturity[0:6] # market_data file requires VIX maturity in 8 digit form but file uses 6 digits!
    print("Carry Maturity: ", carryMaturity)
    print("Price Maturity: ", priceMaturity)
    #!!!!


    # From the current PRICE and CARRY files get the last dates updated...

    legacy_price_file = legacy_path + market + '_price.csv'
    legacy_carry_file = legacy_path + market + '_carrydata.csv'
    legacy_price_df = pd.read_csv(legacy_price_file)
    legacy_carry_df = pd.read_csv(legacy_carry_file,dtype={'CARRY_CONTRACT': str, 'PRICE_CONTRACT': str})
    legacy_price_df = legacy_price_df.set_index('DATETIME').copy()
    legacy_carry_df = legacy_carry_df.set_index('DATETIME').copy()
    latest_price_date = legacy_price_df.iloc[-1:].index[0]
    latest_carry_date = legacy_carry_df.iloc[-1:].index[0]
    if latest_price_date != latest_carry_date:
        lineString = time_now + "," + "E" + "," + market + "," + "Price and Carry legacy files different lengths!"
        print("============================================================")
        print("PRICE and CARRY files different dates!")
        print("============================================================")
        errorFileHandle = open(error_filename, 'a')
        errorFileHandle.write(lineString)  # python will convert \n to os.linesep
        errorFileHandle.close()

        # !!!! Test
    print("latest price date: ", latest_price_date)
    print("latest carry date: ", latest_carry_date)
    # !!!!

    print("== Processing Price Series =======================================")
    # If downloaded PRICE  has any new data rows, append them to price_df and then overwrite existing file....

    downloaded_price_file = newdata_path + 'downloads/quandl/' + market + '/' + priceMaturity + '.csv'
    if not(os.path.isfile(downloaded_price_file)):  # If the File does not exist or can't be opened, report an error!!!!
        # MUST be reported error... processing can proceed with next market

        print("Cannot open file ", downloaded_price_file)
        # Record in the error file

        lineString = time_now + "," + "E" + "," + market + "," + "Cannot open file: " + "," + downloaded_price_file
        print("============================================================")
        print(downloaded_price_file + "File opening error!!!")
        print("============================================================")
        errorFileHandle = open(error_filename, 'a')
        errorFileHandle.write(lineString)  # python will convert \n to os.linesep
        errorFileHandle.close()  # you can omit in most cases as the destructor will call it

    else:
        downloaded_price_df = pd.read_csv(downloaded_price_file, usecols=[0, 1])
        #Check if downloads from IB contains any newer PRICE rows...
        downloaded_price_df.columns = ['DATETIME','PRICE']
        latest_prices_df = downloaded_price_df.set_index('DATETIME').copy()
        latest_prices_df = latest_prices_df.loc[latest_price_date:][1:]
        if latest_prices_df.empty:
            print(market, ": Data is up to date...", latest_price_date)
            lineString = time_now + "," + "W" + "," + market + "," + "Data is up to date!"
            errorFileHandle = open(error_filename, 'a')
            errorFileHandle.write(lineString)  # python will convert \n to os.linesep
            errorFileHandle.close()  # you can omit in most cases as the destructor will call it
            return

        print("Now adding the following PRICE row/s....****************************************")
        print(latest_prices_df)
        row_count = len(latest_prices_df)

        legacy_price_df = (legacy_price_df).append(latest_prices_df)
        print(legacy_price_df) # updated dataFrame
        print("Writing " + str(row_count) + " rows to file: ", legacy_price_file)
        legacy_price_df.to_csv(legacy_price_file)
        print()

        print("== Now processing the CARRY File =======================================")
        # Create dataframe to write back to carry file
        # Check for Carry maturity
        latest_prices_df = downloaded_price_df.set_index('DATETIME').copy()
        latest_prices_df = latest_prices_df.loc[latest_carry_date:][1:]  # from date of last CARRRY row!
        downloaded_carry_file = newdata_path + 'downloads/quandl/' + market + '/' + carryMaturity + '.csv'
        if not(os.path.isfile(downloaded_carry_file)):
            print("No CARRY file...")
            # However, we still need to create new CARRY FILE rows with PRICE and blanks in CARRY column!
            # Write a warning to error file!
            lineString = time_now + "," + "W" + "," + "Cannot open file: " + "," + downloaded_carry_file
            print("============================================================")
            print(downloaded_carry_file + " file opening error!!!")
            print("============================================================")
            errorFileHandle = open(error_filename, 'a')
            errorFileHandle.write(lineString)  # python will convert \n to os.linesep
            errorFileHandle.close()  # you can omit in most cases as the destructor will call it
            dfConcat = latest_prices_df
            row_count = len(latest_prices_df)
            dfConcat.columns = ["PRICE"]
            dfConcat["CARRY"] = ""
            dfConcat["CARRY_CONTRACT"] = carryMaturity
            dfConcat["PRICE_CONTRACT"] = priceMaturity
            print(dfConcat)
            legacy_carry_df = (legacy_carry_df).append(dfConcat)
            print(legacy_carry_df)
            print("Writing " + str(row_count) + " rows to file: ", legacy_carry_file)
            legacy_carry_df.to_csv(legacy_carry_file)
        else: # A carry file exists
            downloaded_carry_df = pd.read_csv(downloaded_carry_file, usecols=[0, 1])
            # Check if sourceCarryDF contains newer rows...
            downloaded_carry_df.columns = ['DATETIME', 'PRICE']
            latest_carry_df = downloaded_carry_df.set_index('DATETIME').copy()
            latest_carry_df = (latest_carry_df.loc[latest_carry_date:][1:])
            if latest_carry_df.empty:
                print("No new carry data...")
                # However, we still need to create new CARRY FILE rows with PRICE and blanks in CARRY column!
                # Write a warning to error file!
                lineString = time_now + "," + "W" + "," + "No new carry data: " + "," + downloaded_carry_file
                print("============================================================")
                print(downloaded_carry_file + " No new carry data!!!")
                print("============================================================")
                errorFileHandle = open(error_filename, 'a')
                errorFileHandle.write(lineString)  # python will convert \n to os.linesep
                errorFileHandle.close()  # you can omit in most cases as the destructor will call it
                dfConcat = latest_prices_df
                dfConcat.columns = ["PRICE"]
                dfConcat["CARRY"] = ""
            else:
                print("Now adding the following CARRY row/s ****************************************")
                print(latest_carry_df)
                dfConcat = pd.concat([latest_prices_df, latest_carry_df], axis=1)
            dfConcat.columns = ["PRICE", "CARRY"]
            dfConcat["CARRY_CONTRACT"] = carryMaturity
            dfConcat["PRICE_CONTRACT"] = priceMaturity
            #dfConcat.index.names = ["DATETIME"]
            print(dfConcat)
            legacy_carry_df = (legacy_carry_df).append(dfConcat)
            print(legacy_carry_df)
            print("Writing to file: ", legacy_carry_file)
            legacy_carry_df.to_csv(legacy_carry_file)
            print("== End processing for ", market, " =======================================")
            print()


def updateFXRate(market):

    # Read fxdata to check
    # 1. For each rate check for new data
    # 2. Access source file and add corresponding rows.
    # Report if no new data... or any other erros




    print("==========================================================================================")
    print("Begin processing for.... ", market)

    row = market_data_df.loc[market_data_df['CAVER'] == market]
    #print("row: ", row)
    # Get corresponding fx rate file...

    ratePair = row.iloc[0]['CAVER']
    print("ratePair: ", ratePair)

    fx_rate_file = legacy_path + ratePair + 'fx.csv'

    print("fxRateFile: ", fx_rate_file)
    fx_rate_df = pd.read_csv(fx_rate_file)
    fx_rate_df = fx_rate_df.set_index('DATETIME').copy()
    latest_fx_rate_date = fx_rate_df.iloc[-1:].index[0]
    print("latest_fx_rate_date: ", latest_fx_rate_date)

    # Now append any new lines...

    # Read and check Price maturity download File and append any new lines to priceDF and overwrite existing file....

    downloaded_fx_file = newdata_path + 'downloads/quandl/' + ratePair + '/' + ratePair + '.csv'
    print("== Checking if there is new updates for: ", downloaded_fx_file)
    if not(os.path.isfile(downloaded_fx_file)):
        print("Cannot open file ", downloaded_fx_file)
        lineString = time_now + "," + "W" + "," + "Cannot open fx rate file: " + "," + downloaded_fx_file
        print("============================================================")
        print(downloaded_fx_file + " file opening error!!!")
        print("============================================================")
        errorFileHandle = open(error_filename, 'a')
        errorFileHandle.write(lineString)  # python will convert \n to os.linesep
        errorFileHandle.close()  # you can omit in most cases as the destructor will call it
    else:
        downloaded_fx_df = pd.read_csv(downloaded_fx_file, usecols=[0, 1])
        # Doew sourceFXDF contains newer rows?
        downloaded_fx_df.columns = ['DATETIME', 'FX']
        latest_fx_df = downloaded_fx_df.set_index('DATETIME').copy()
        latest_fx_df = latest_fx_df.loc[latest_fx_rate_date:][1:]
        if latest_fx_df.empty:
            print(market, ": Data is up to date...", latest_fx_rate_date)
            lineString = time_now + "," + "W" + "," + "No new fx data in: " + "," + downloaded_fx_file
            print("============================================================")
            print(downloaded_fx_file + " No new fx data!!!")
            print("============================================================")
            errorFileHandle = open(error_filename, 'a')
            errorFileHandle.write(lineString)  # python will convert \n to os.linesep
            errorFileHandle.close()  # you can omit in most cases as the destructor will call it
            return

        print("Now adding the following fx rate row/s ****************************************")
        print(latest_fx_df)

        fx_rate_df = (fx_rate_df).append(latest_fx_df)
        print(fx_rate_df)  # updated dataFrame
        print("Writing to file: ", fx_rate_file)
        fx_rate_df.to_csv(fx_rate_file)
        print("== End processing for ", market," =======================================")
        print()



if __name__=="__main__":

    """
        ===================================================================================================
        Author: P. Kawuki
        Date: 20160730
        Description: Update legacycsv files with downloaded prices. Prices are assumed
                   : to be stored in downloads subdirectory
        ===================================================================================================
    """

    dir_filename = "../data/admin/directories.csv"
    df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
    newdata_path = df.loc['SOURCE'][0]
    legacy_path = df.loc['DESTINATION'][0]

    today = time.strftime("%Y%m%d")
    time_now = time.strftime("%Y%m%d %H:%M:%S")

    error_filename = newdata_path + 'logs/' + today + "download_errors" + ".txt"
    data_filename = newdata_path + 'admin/' + 'marketdata.csv'

    market_data_df = pd.read_csv(data_filename, dtype={'CARRY': str, 'PRICE': str})

    for row in market_data_df.itertuples():

        print(row[3])
        if row[3] == 'FUT':
            updateFuturesMarket(row[1])
        if row[3] == 'CASH':
            updateFXRate(row[1])























'''
    markets = ["KR3","V2X","EDOLLAR","MXP","CORN","EUROSTX","GAS_US","PLAT","US2","LEANHOG","GBP","VIX","CAC","COPPER","CRUDE_W","BOBL","WHEAT","JPY","NASDAQ","GOLD","US5","SOYBEAN","AUD","SP500","PALLAD","KR10","LIVECOW"]
    fxmarkets = ["GBPUSD", "KRWUSD", "EURUSD"]

    for market in markets:
        updateMarket (market)

    for market in fxmarkets:
        updateFX(market)
'''
