from swigibpy import EWrapper
import time
import numpy as np
import datetime
from swigibpy import EPosixClientSocket, ExecutionFilter

from swigibpy import Order as IBOrder
from IButils import bs_resolve, action_ib_fill

MAX_WAIT_SECONDS=30
MEANINGLESS_NUMBER=1830


def return_IB_connection_info():
    """
    Returns the tuple host, port, clientID required by eConnect
   
    """
   
    host=""
   
    port=7496
    clientid=999
   
    return (host, port, clientid)

class IBWrapper(EWrapper):
    """

    Callback object passed to TWS, these functions will be called directly by the TWS or Gateway.

    """

    ## We need these but don't use them




    def init_error(self):
        setattr(self, "flag_iserror", False)
        setattr(self, "error_msg", "")

    def error(self, id, errorCode, errorString):
        """
        error handling, simple for now
       
        Here are some typical IB errors
        INFO: 2107, 2106
        WARNING 326 - can't connect as already connected
        CRITICAL: 502, 504 can't connect to TWS.
            200 no security definition found
            162 no trades

        """
        ## Any errors not on this list we just treat as information
        ERRORS_TO_TRIGGER=[201, 103, 502, 504, 509, 200, 162, 420, 2105, 1100, 478, 201, 399]
       
        if errorCode in ERRORS_TO_TRIGGER:
            errormsg="IB error id %d errorcode %d string %s" %(id, errorCode, errorString)
            print (errormsg)
            setattr(self, "flag_iserror", True)
            setattr(self, "error_msg", True)
           
        ## Wrapper functions don't have to return anything
       
    """
    Following methods will be called, but we don't use them
    """

    def nextValidId(self, id):
        pass
       
    def managedAccounts(self, openOrderEnd):
        pass

    def orderStatus(self, reqid, status, filled, remaining, avgFillPrice, permId,
            parentId, lastFilledPrice, clientId, whyHeld):
        pass

    def commissionReport(self, blah):
        pass

    def updateAccountTime(self, timeStamp):
        pass 
    
    ## contract details

    def init_contractdetails(self, reqId):
        if "data_contractdetails" not in dir(self):
            dict_contractdetails=dict()
        else:
            dict_contractdetails=self.data_contractdetails
        
        dict_contractdetails[reqId]={}
        setattr(self, "flag_finished_contractdetails", False)
        setattr(self, "data_contractdetails", dict_contractdetails)
        

    def contractDetails(self, reqId, contractDetails):
        """
        Return contract details
        
        If you submit more than one request watch out to match up with reqId
        """
        
        contract_details=self.data_contractdetails[reqId]

        contract_details["contractMonth"]=contractDetails.contractMonth
        contract_details["liquidHours"]=contractDetails.liquidHours
        contract_details["longName"]=contractDetails.longName
        contract_details["minTick"]=contractDetails.minTick
        contract_details["tradingHours"]=contractDetails.tradingHours
        contract_details["timeZoneId"]=contractDetails.timeZoneId
        contract_details["underConId"]=contractDetails.underConId
        contract_details["evRule"]=contractDetails.evRule
        contract_details["evMultiplier"]=contractDetails.evMultiplier

        contract2 = contractDetails.summary

        contract_details["expiry"]=contract2.expiry

        contract_details["exchange"]=contract2.exchange
        contract_details["symbol"]=contract2.symbol
        contract_details["secType"]=contract2.secType
        contract_details["currency"]=contract2.currency

    def contractDetailsEnd(self, reqId):
        """
        Finished getting contract details
        """
        
        setattr(self, "flag_finished_contractdetails", True)




    ## portfolio

    def init_portfolio_data(self):
        if "data_portfoliodata" not in dir(self):
            setattr(self, "data_portfoliodata", [])
        if "data_accountvalue" not in dir(self):
            setattr(self, "data_accountvalue", [])
            
        
        setattr(self, "flag_finished_portfolio", False)
        

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        """
        Add a row to the portfolio structure
        """

        portfolio_structure=self.data_portfoliodata
                
        portfolio_structure.append((contract.symbol, contract.expiry, position, marketPrice, marketValue, averageCost, 
                                    unrealizedPNL, realizedPNL, accountName, contract.currency))

    ## account value
    
    def updateAccountValue(self, key, value, currency, accountName):
        """
        Populates account value dictionary
        """
        account_value=self.data_accountvalue
        
        account_value.append((key, value, currency, accountName))
        

    def accountDownloadEnd(self, accountName):
        """
        Finished can look at portfolio_structure and account_value
        """
        setattr(self, "flag_finished_portfolio", True)

    def init_tickdata(self, TickerId):
        if "data_tickdata" not in dir(self):
            tickdict = dict()
        else:
            tickdict = self.data_tickdata

        tickdict[TickerId] = [np.nan] * 4
        setattr(self, "data_tickdata", tickdict)

    def tickString(self, TickerId, field, value):
        marketdata = self.data_tickdata[TickerId]

        ## update string ticks

        tickType = field

        if int(tickType) == 0:
            ## bid size
            marketdata[0] = int(value)
        elif int(tickType) == 3:
            ## ask size
            marketdata[1] = int(value)

        elif int(tickType) == 1:
            ## bid
            marketdata[0][2] = float(value)
        elif int(tickType) == 2:
            ## ask
            marketdata[0][3] = float(value)

    def tickGeneric(self, TickerId, tickType, value):
        marketdata = self.data_tickdata[TickerId]

        ## update generic ticks

        if int(tickType) == 0:
            ## bid size
            marketdata[0] = int(value)
        elif int(tickType) == 3:
            ## ask size
            marketdata[1] = int(value)

        elif int(tickType) == 1:
            ## bid
            marketdata[2] = float(value)
        elif int(tickType) == 2:
            ## ask
            marketdata[3] = float(value)

    def tickSize(self, TickerId, tickType, size):

        ## update ticks of the form new size

        marketdata = self.data_tickdata[TickerId]

        if int(tickType) == 0:
            ## bid
            marketdata[0] = int(size)
        elif int(tickType) == 3:
            ## ask
            marketdata[1] = int(size)

    def tickPrice(self, TickerId, tickType, price, canAutoExecute):
        ## update ticks of the form new price

        marketdata = self.data_tickdata[TickerId]

        if int(tickType) == 1:
            ## bid
            marketdata[2] = float(price)
        elif int(tickType) == 2:
            ## ask
            marketdata[3] = float(price)

    def updateMktDepth(self, id, position, operation, side, price, size):
        """
        Only here for completeness - not required. Market depth is only available if you subscribe to L2 data.
        Since I don't I haven't managed to test this.

        Here is the client side call for interest

        tws.reqMktDepth(999, ibcontract, 9)

        """
        pass

    def tickSnapshotEnd(self, tickerId):

        print("No longer want to get %d" % tickerId)
    
class IBclient(object):
    """
    Client object
    
    Used to interface with TWS for outside world, does all handling of streaming waiting etc
    
    Create like this
    callback = IBWrapper()
    client=IBclient(callback)

    We then use various methods to get prices etc

    """
    def __init__(self, callback, accountid="DU15237"):
        """
        Create like this
        callback = IBWrapper()
        client=IBclient(callback)
        """
        
        tws = EPosixClientSocket(callback)
        (host, port, clientid)=return_IB_connection_info()
        tws.eConnect(host, port, clientid)

        self.tws=tws
        self.accountid=accountid
        self.cb=callback

    def get_contract_details(self, ibcontract, reqId=MEANINGLESS_NUMBER):
    
        """
        Returns a dictionary of contract_details
        
        
        """
        
        self.cb.init_contractdetails(reqId)
        self.cb.init_error()
    
        self.tws.reqContractDetails(
            reqId,                                         # reqId,
            ibcontract,                                   # contract,
        )
    

        finished=False
        iserror=False
        
        start_time=time.time()
        while not finished and not iserror:
            finished=self.cb.flag_finished_contractdetails
            iserror=self.cb.flag_iserror
            
            if (time.time() - start_time) > MAX_WAIT_SECONDS:
                finished=True
                iserror=True
            pass
    
        contract_details=self.cb.data_contractdetails[reqId]
        if iserror or contract_details=={}:
            print (self.cb.error_msg)
            print ("Problem getting details")
            return None
    
        return contract_details


    

    def get_IB_account_data(self):

        self.cb.init_portfolio_data()
        self.cb.init_error()
        
        ## Turn on the streaming of accounting information
        self.tws.reqAccountUpdates(True, self.accountid)
        
        start_time=time.time()
        finished=False
        iserror=False

        while not finished and not iserror:
            finished=self.cb.flag_finished_portfolio
            iserror=self.cb.flag_iserror

            if (time.time() - start_time) > MAX_WAIT_SECONDS:
                finished=True
                print ("Didn't get an end for account update, might be missing stuff")
            pass

        ## Turn off the streaming
        ## Note portfolio_structure will also be updated

        self.tws.reqAccountUpdates(False, self.accountid)

        portfolio_data=self.cb.data_portfoliodata
        account_value=self.cb.data_accountvalue

        
        
        if iserror:
            print (self.cb.error_msg)
            print ("Problem getting details")
            return None

        return (account_value, portfolio_data)

    def get_IB_market_data(self, ibcontract, seconds=2, tickerid=MEANINGLESS_NUMBER):
        """
        Returns granular market data

        Returns a tuple (bid price, bid size, ask price, ask size)

        """

        ## initialise the tuple
        self.cb.init_tickdata(tickerid)
        self.cb.init_error()

        # Request a market data stream
        self.tws.reqMktData(
            tickerid,
            ibcontract,
            "",
            False)

        start_time = time.time()

        finished = False
        iserror = False

        while not finished and not iserror:
            iserror = self.cb.flag_iserror
            if (time.time() - start_time) > seconds:
                #print("We timed out....")
                finished = True
            pass
        self.tws.cancelMktData(tickerid)

        marketdata = self.cb.data_tickdata[tickerid]
        ## marketdata should now contain some interesting information
        ## Note in this implementation we overwrite the contents with each tick; we could keep them


        if iserror:
            print("Error: " + str(self.cb.error_msg))
            print("Failed to get any prices with marketdata")

        return marketdata