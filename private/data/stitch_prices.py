import pandas as pd
import time
import os


if __name__=="__main__":

    """
        ===================================================================================================
        Author: P. Kawuki
        Date: 20160823
        Description: Stich the data files using date and new rows in specified files to replace old ones in existing
                    PRICE AND CARRY series and to perform the adjustment to existing PRICE series
        ===================================================================================================
    """

    dir_filename = "../data/admin/directories.csv"
    df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
    source_path = df.loc['SOURCE'][0]
    downloads_path = source_path + "downloads/quandl/"
    destination_path = df.loc['DESTINATION'][0]

    time_now = time.strftime("%Y%m%d %H:%M:%S")
    fileday = time.strftime("%Y%m%d")
    today = time.strftime("%Y-%m-%d")
    print("Date today: ", today)
    error_filename = source_path + 'logs/' + fileday + "download_errors" + ".txt"
    stich_filename = source_path + 'admin/' + 'stitchdata.csv'

    stitch_data_df = pd.read_csv(stich_filename, dtype={'FROM_PRICE': str, 'FROM_CARRY': str, 'TO_PRICE': str, 'TO_CARRY': str})
    new_df = stitch_data_df.set_index(['CARVER','DATETIME']).copy()
    print(new_df.index)
    new_df.sort_index(ascending=True, inplace=True)
    print(new_df)
    print(new_df.index.values)
    for row in new_df.itertuples():

        datetime = row[0][1]
        print("row[6] ", row[6])

        if datetime < today and row[6] == 0:
            # If the date for the splice is OK and the value in column 'DONE' is 0, then  that splice date is OK!
            current_price_file = destination_path + row[0][0] +  '_price.csv'
            if os.path.isfile(current_price_file):
                #print(current_price_file, "exists")
                price_df = pd.read_csv(current_price_file, index_col='DATETIME')
                #print(price_df.tail(3))
                download_price_file = downloads_path + row[0][0] + '/' + row[2] + '.csv'
                if os.path.isfile(download_price_file):
                    print(download_price_file, "exists")
                    dl_price_df = pd.read_csv(download_price_file, usecols=[0, 'Settle'], index_col=0, dayfirst=True)
                    dl_price_df.rename(columns={'Settle': 'PRICE'}, inplace=True)
                    dl_price_df.index.name = 'DATETIME'
                    print(dl_price_df.tail(20))
                    # Check if stitch date exists in both existing and downloaded price series
                    if datetime in price_df.index.values and datetime in dl_price_df.index.values:
                        old_value = price_df.loc[datetime]['PRICE']
                        new_value = dl_price_df.loc[datetime]['PRICE']
                        delta = new_value - old_value

                        print("old Price ", old_value)
                        print("new Price ", new_value)
                        print()
                        print("***** The stitch date is: ", datetime)
                        print("***** The Delta is: ", delta)

                        new_df.set_value((row[0][0], row[0][1]),'DELTA',delta)
                        new_df.set_value((row[0][0], row[0][1]), 'DONE', 1)

                        print(new_df)
                        print("Before")
                        print(price_df.head(5))
                        print(price_df.tail(5))

                        price_df['PRICE'] = price_df['PRICE'].apply(lambda x: x + delta)
                        # TEST: Panama stitch oldPriceDF with newPriceDF and write the file to a test place for testing
                        price_df = price_df[:datetime][:-1]

                        print("After")
                        print(price_df.head(5))
                        print(price_df.tail(5))

                        #Now stitch old "adjusted" price series with new price maturity
                        stitched_price_df = price_df.append(dl_price_df[datetime:]).copy()
                        print(stitched_price_df.tail(20))
                        stitched_price_df.to_csv(current_price_file)

                    else:
                        print(datetime, "does not exist in both... ")
                        print(download_price_file, "and ")
                        print(current_price_file)
                else:
                    print(download_price_file, "does not exist")
            else:
                print(current_price_file, "does not exist")

            # Now perform the CARRY stitching ase well...
            # At least 3 columns (PRICE,CARRY,PRICE_CONTRACT)  MUST be updated to match the PRICE series
            # First we must test if there is a new CARRY file. Some contracts do not trade until let in the cycle and
            # Blanks need to be filled in the CARRY columna until then

            current_carry_file = destination_path + row[0][0] + '_carrydata.csv'
            if os.path.isfile(current_carry_file):
                carry_df = pd.read_csv(current_carry_file, index_col='DATETIME',dtype={'PRICE_CONTRACT': str, 'CARRY_CONTRACT': str})
                carry_df = carry_df[:datetime][:-1]

                download_carry_file = downloads_path + row[0][0] + '/' + row[4] + '.csv'
                if os.path.isfile(download_carry_file):
                    dl_carry_df = pd.read_csv(download_carry_file, usecols=[0, 'Settle'], index_col=0, dayfirst=True)
                    dl_carry_df.rename(columns={'Settle': 'PRICE'}, inplace=True)
                    dl_carry_df.index.name = 'DATETIME'
                    dfConcat = pd.concat([dl_price_df[datetime:], dl_carry_df[datetime:]], axis=1)
                else:
                    dfConcat = dl_price_df[datetime:]
                    dfConcat.is_copy = False
                    dfConcat["CARRY"] = ""
                dfConcat.columns = ["PRICE", "CARRY"]
                dfConcat["CARRY_CONTRACT"] = row[4]
                dfConcat["PRICE_CONTRACT"] = row[2]
                carry_df = (carry_df).append(dfConcat)
                print(carry_df)
                carry_df.to_csv(current_carry_file)

            else:
                # This is a FATAL error. The CARRY file should exist!
                print(current_carry_file, "does not exist")

        else:
            lineString = time_now + "," + row[0][0] + datetime + " cannot be added!"
            print("============================================================")
            print(row[0][0] + datetime + " cannot be added!")
            print("============================================================")
            errorFileHandle = open(error_filename, 'a')
            errorFileHandle.write(lineString)  # python will convert \n to os.linesep
            errorFileHandle.close()

    # Now save the value of new_df back to the market_data file!
    new_df.to_csv(stich_filename)

