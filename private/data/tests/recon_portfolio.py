from datetime import *
from dateutil.relativedelta import *
import os
import logging
from time import sleep


def set_logging():
    logger = logging.getLogger('Downloads')
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even DEBUG messages
    file_handler = logging.FileHandler('../data/logs/orders/orders.log')
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
    from wrapper_combine import IBWrapper, IBclient
    import pandas as pd
    import numpy as np
    from swigibpy import Contract as IBcontract

    path = '/home/pete/Repos/pysystemtrade/private/SystemR/'
    positions_file = path + 'positions/system.csv'
    positions_df = pd.read_csv(positions_file, index_col=0, dtype={'MATURITY': str, 'PRICE': str})
    print(positions_df)

    dir_filename = "../data/admin/directories.csv"
    if not os.path.isfile(dir_filename):
        logger.error("The file, {}, does not exist".format(dir_filename))
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
        curr_markets_df = curr_markets_df[['IB', 'PRICE', 'IB_EXCHANGE', 'CURRENCY', 'MULTIPLIER']]

        curr_markets_df = curr_markets_df[pd.notnull(curr_markets_df['PRICE'])]
        print(curr_markets_df)
        system_positions = pd.concat([curr_markets_df, positions_df], axis=1)[['IB', 'IB_EXCHANGE', 'CURRENCY', 'MULTIPLIER', 'MATURITY', 'QUANTITY']]
        system_positions = system_positions.set_index(['IB'])
        print(system_positions)

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
        portfolio_positions = portfolio_positions[portfolio_positions['POSITION'] != 0]
        portfolio_positions = portfolio_positions.set_index(['IB'])

        # Merge system positions and portfolio positions into one df
        merged_positions = system_positions.join(portfolio_positions)
        merged_positions['POSITION'] = merged_positions['POSITION'].fillna(0)
        merged_positions['POSITION'] = merged_positions['POSITION'].astype(int)

        actual_positions = merged_positions[(merged_positions.QUANTITY != 0) | (merged_positions.POSITION != 0)]
        print(actual_positions)
        print()
        sleep(1)
        logger.debug("")
        for tuple in actual_positions.itertuples():

            market = tuple.Index
            expiry = tuple.EXPIRY
            if ( market == 'NG' or market == 'CL' ) and expiry == expiry:  # expiry == expiry is true if expiry is not NAN
                # month early expiry so shift expiry month forwad
                date = datetime(int(tuple.EXPIRY[:4]), int(tuple.EXPIRY[4:6]),1)
                foward_date = date + relativedelta(months=1)
                expiry = "{:%Y%m}".format(foward_date)

            if tuple.MATURITY == expiry and tuple.POSITION == tuple.QUANTITY:
                logger.info("OK        : {:<6}: {:<6}: {:<3}: Position is OK".format(market, expiry, tuple.POSITION))
        logger.debug("")
        order_list = []
        for tuple in actual_positions.itertuples():
            #print(tuple)
            market = tuple.Index
            # For some markets, expiry is a month earlier than Contract Month -
            # So we have to shift EXPIRY one month forwad for these markets...(NG, CL, ...?)
            expiry = tuple.EXPIRY
            if ( market == 'NG' or market == 'CL') and expiry == expiry :
                date = datetime(int(tuple.EXPIRY[:4]), int(tuple.EXPIRY[4:6]), 1)
                foward_date = date + relativedelta(months=1)
                expiry = "{:%Y%m}".format(foward_date)
            if tuple.MATURITY != expiry and pd.notnull(expiry):
                logger.error("ROLL      : {:<10}: from {} to {}".format(market, expiry, tuple.MATURITY))
            elif tuple.QUANTITY > tuple.POSITION:
                units = tuple.QUANTITY - tuple.POSITION
                logger.error("BUY       : {:<6}{:<6}: Buy {} contracts".format(market, tuple.MATURITY, units))
                order_list.append({'IB': tuple.Index,'IB_EXCHANGE': tuple.IB_EXCHANGE, 'CURRENCY': tuple.CURRENCY,
                                      'MULTIPLIER': tuple.MULTIPLIER, 'MATURITY': tuple.MATURITY, 'QUANTITY': units})
            elif tuple.QUANTITY < tuple.POSITION:
                units = tuple.POSITION - tuple.QUANTITY
                logger.error("SELL      : {:<6}{:<6}: Sell {} contracts".format(market, tuple.MATURITY, units))
                order_list.append({'IB': tuple.Index, 'IB_EXCHANGE': tuple.IB_EXCHANGE, 'CURRENCY': tuple.CURRENCY,
                                   'MULTIPLIER': tuple.MULTIPLIER, 'MATURITY': tuple.MATURITY, 'QUANTITY': units * -1})
        order_df = pd.DataFrame(order_list)
        print(order_df)
        # Cycle through the orders and for each market at a designated time (RT?), for a duration, t,(default: 5min)
        # place a Limit Order (Bid price for BUY, Ask price for SELL). If no fill after duration, move across spread
        # for a duration, d. If not filled, cross the spread (BUY at Ask or SELL at Bid)
        #
        # Add prices to order_df
        tickerid = 10000
        seconds = 20
        mark = []
        for row in order_df.itertuples():
            tickerid = tickerid + 1
            ibcontract = IBcontract()
            ibcontract.secType = "FUT"
            ibcontract.expiry = row.MATURITY
            ibcontract.symbol = row.IB
            ibcontract.exchange = row.IB_EXCHANGE
            ibcontract.currency = row.CURRENCY
            if row.MULTIPLIER > 0:
                ibcontract.multiplier = str(row.MULTIPLIER)
            ans = client.get_IB_market_data(ibcontract)
            print(row.IB, ans)

    '''
    from wrapper_combine import IBWrapper, IBclient
    from swigibpy import Contract as IBcontract
    callback = IBWrapper()
    client = IBclient(callback)

    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry = "201612"
    ibcontract.symbol = "GE"
    ibcontract.exchange = "GLOBEX"
    ans = client.get_IB_market_data(ibcontract)
    print(ans)
    '''

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

        '''

                for row in order_df.itertuples():
                    ibcontract = IBcontract()
                    ibcontract.secType = "FUT"
                    ibcontract.expiry = row.MATURITY
                    ibcontract.symbol = row.IB
                    ibcontract.exchange = row.IB_EXCHANGE
                    ibcontract.currency = row.CURRENCY
                    if row.MULTIPLIER > 0:
                        ibcontract.multiplier = str(row.MULTIPLIER)
                    ans = client.get_IB_market_data(ibcontract)
                    print(row.IB, ans)

                '''


