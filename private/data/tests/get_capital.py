from wrapper_v5 import IBWrapper, IBclient
from swigibpy import Contract as IBcontract
import pandas as pd

import time
 
if __name__=="__main__":

    """
    This simple example places an order, checks to see if it is active, and receives fill(s)
    
    Note: If you are running this on the 'edemo' account it will probably give you back garbage
    
    Though the mechanics should still work
    
    This is because you see the orders that everyone demoing the account is trading!!!

    """
    callback = IBWrapper()
    client=IBclient(callback)

    (account_value, portfolio_data)=client.get_IB_account_data()
    account_value_df = pd.DataFrame(account_value, columns=['KEY', 'VALUE', 'CURRENCY', 'ACCOUNT'])
    account_value_df.set_index('KEY', inplace=True)
    account = account_value_df.loc['NetLiquidation']['VALUE']
    print(account)
