
import pandas as pd
import numpy as np
import os

def diff_pd(df1, df2):
    """Identify differences between two pandas DataFrames"""
    assert (df1.columns == df2.columns).all(), \
        "DataFrame column names are different"
    if df1.equals(df2):
        return None
    else:
        # need to account for np.nan != np.nan returning True
        diff_mask = (df1 != df2) & ~(df1.isnull() & df2.isnull())
        ne_stacked = diff_mask.stack()
        changed = ne_stacked[ne_stacked]
        changed.index.names = ['id', 'col']
        difference_locations = np.where(diff_mask)
        changed_from = df1.values[difference_locations]
        changed_to = df2.values[difference_locations]
        return pd.DataFrame({'from': changed_from, 'to': changed_to},
                            index=changed.index)



path_latest = '/home/pete/Documents/IBDocs/'
path_saved = '/home/pete/Documents/IBDocs_20160818/'
dataFilename = path_latest + 'marketdata.csv'
market_df = pd.read_csv(dataFilename)

for row in market_df.itertuples():
    print()
    print("=========================================================================")
    print(row[1])
    print("=========================================================================")
    file_latest = path_latest + row[1] + '/' + str(row[7])[0:6] + ".csv"
    file_saved = path_saved + row[1] + '/' + str(row[7])[0:6] + ".csv"

    if not (os.path.isfile(file_latest)):  # If the File does not exist or can't be opened, report an error!!!!
        # MUST be reported error... processing can proceed with next market

        print("Cannot open file ", file_latest)
        # Record in the error file
        continue

    else:
        latest_df = pd.read_csv(file_latest, usecols=[0, 1])
        # Check if downloads from IB contains any newer PRICE rows...
        latest_df.columns = ['DATETIME', 'PRICE']
        latest_prices_df = latest_df.set_index('DATETIME').copy()

    if not (os.path.isfile(file_saved)):  # If the File does not exist or can't be opened, report an error!!!!
        # MUST be reported error... processing can proceed with next market

        print("Cannot open file ", file_saved)
        # Record in the error file
        continue

    else:
        saved_df = pd.read_csv(file_saved, usecols=[0, 1])
        # Check if downloads from IB contains any newer PRICE rows...
        saved_df.columns = ['DATETIME', 'PRICE']
        saved_prices_df = saved_df.set_index('DA'
                                             'TETIME').copy()
    # Now check if both dataframes are identical
    print("saved prices df")
    print(saved_prices_df)
    print("latest prices df")
    print(latest_prices_df)
    diffs_pd = diff_pd(latest_prices_df, saved_prices_df)
    print(diffs_pd)
