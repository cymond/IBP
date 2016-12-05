from datetime import *
from dateutil.relativedelta import *
import os
import logging


def set_logging():
    logger = logging.getLogger('Downloads')
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even DEBUG messages
    file_handler = logging.FileHandler('update.log')
    file_handler.setLevel(logging.DEBUG)

    # create a console handler to show errors
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create formatter and add it to handlers
    # formatter = logging.Formatter('%(message)s')
    console_formatter = logging.Formatter('{asctime} {name} {levelname:8s} {message}', datefmt='%Y%m%d %I:%M:%S%p',
                                          style='{')
    file_formatter = logging.Formatter('{asctime},{name},{levelname:8s},{message}', datefmt='%Y%m%d %I:%M:%S%p',
                                       style='{')
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def main():


    from datetime import timedelta
    from wrapper_v5 import IBWrapper, IBclient
    import pandas as pd
    import numpy as np
    from swigibpy import Contract as IBcontract

    path = '/home/pete/Repos/pysystemtrade/private/SystemR/'
    positions_file = path + 'positions/system.csv'
    positions_df = pd.read_csv(positions_file, index_col=0, dtype={'MATURITY': str, 'PRICE': str})

    dir_filename = "../../data/admin/directories.csv"
    if not os.path.isfile(dir_filename):
        logger.error("The file, {}, does not exist".format('directories.csv'))
    else:
        # Get daily system positions
        dir_df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
        source_path = dir_df.loc['SOURCE'][0] # /home/pete/Documents/Python Packages/sysIB/private/data/
        market_data_filename = source_path + 'admin/' + 'new_marketdata_test.csv'
        market_data_df = pd.read_csv(market_data_filename,  dtype={'CARRY': str, 'PRICE': str})
        # For each market we only need most recent (by DATETIME) row where DONE == 1
        #live_market_df = market_data_df[market_data_df.DONE == 1]
        temp_df = market_data_df[market_data_df.DONE == 1].sort_values(by=['CARVER','DATEFROM'], ascending=[1,1])
        curr_markets_df = temp_df.groupby('CARVER').tail(1)
        curr_markets_df = curr_markets_df.set_index(['CARVER'])
        curr_markets_df = curr_markets_df[['IB', 'PRICE']]
        curr_markets_df = curr_markets_df[pd.notnull(curr_markets_df['PRICE'])]
        system_positions = pd.concat([curr_markets_df, positions_df], axis=1)[['IB', 'MATURITY', 'QUANTITY']]
        system_positions = system_positions.set_index(['IB'])

        # CONNECT to Interactive Brokers and get current portfolio positions
        callback = IBWrapper()
        client = IBclient(callback)
        (account_value, portfolio_data) = client.get_IB_account_data()
        port_df1 = pd.DataFrame(portfolio_data)
        port_df1 = port_df1.ix[:, 0:2]
        port_df1.columns = ['IB', 'EXPIRY', 'POSITION']
        port_df1['EXPIRY'].replace('', np.nan, inplace=True)
        portfolio_positions = port_df1[pd.notnull(port_df1['EXPIRY'])].copy()
        portfolio_positions['EXPIRY'] = portfolio_positions.EXPIRY.str[:6]
        portfolio_positions = portfolio_positions.set_index(['IB'])

        # Merge system positions and portfolio positions into one df
        merged_positions = system_positions.join(portfolio_positions)
        merged_positions['POSITION'] = merged_positions['POSITION'].fillna(0)
        merged_positions['POSITION'] = merged_positions['POSITION'].astype(int)

        actual_positions = merged_positions[(merged_positions.QUANTITY != 0) | (merged_positions.POSITION != 0)]
        print(actual_positions)
        print()
        logger.debug("")
        for tuple in actual_positions.itertuples():

            market = tuple.Index
            if market == 'NG':

                # month early expiry so shift expiry month forwad
                date = datetime(int(tuple.EXPIRY[:4]), int(tuple.EXPIRY[4:6]),1)
                foward_date = date + relativedelta(months=1)
                expiry = "{:%Y%m}".format(foward_date)
            else:
                expiry = tuple.EXPIRY

            if tuple.MATURITY == expiry and tuple.POSITION == tuple.QUANTITY:
                logger.info("OK        : {:<10}: Position is OK".format(market))
        logger.debug("")
        for tuple in actual_positions.itertuples():
            market = tuple.Index
            if market == 'NG':
                # month early expiry so shift expiry month forwad
                date = datetime(int(tuple.EXPIRY[:4]), int(tuple.EXPIRY[4:6]), 1)
                foward_date = date + relativedelta(months=1)
                expiry = "{:%Y%m}".format(foward_date)
            else:
                expiry = tuple.EXPIRY
            if tuple.MATURITY != expiry and pd.notnull(expiry):
                print(market)
                print(tuple.MATURITY)
                print(expiry)
                logger.error("ROLL      : {:<10}: from {} to {}".format(market, expiry, tuple.MATURITY))
            if tuple.QUANTITY > tuple.POSITION:
                units = tuple.QUANTITY - tuple.POSITION
                logger.error("BUY       : {:<10}: Buy {} contracts".format(market, units))
            if tuple.QUANTITY < tuple.POSITION:
                units = tuple.POSITION - tuple.QUANTITY
                logger.error("SELL      : {:<10}: Sell {} contracts".format(market, units))

        #print("\n account info")
        #print(account_value)



if __name__=="__main__":

    """
    This simple example places an order, checks to see if it is active, and receives fill(s)

    Note: If you are running this on the 'edemo' account it will probably give you back garbage

    Though the mechanics should still work

    This is because you see the orders that everyone demoing the account is trading!!!
    """

    logger = set_logging()
    try:
        main()
    except Exception as e:
        logger.exception(e)


