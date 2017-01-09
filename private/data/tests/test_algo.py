from wrapper_v2 import IBWrapper, IBclient
from swigibpy import Contract as IBcontract
 
if __name__=="__main__":

    """
    This simple example returns historical data 
    """

    callback = IBWrapper()
    client=IBclient(callback)

    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry = "201609"
    ibcontract.currency = "KRW"
    ibcontract.symbol = "3KTB"
    ibcontract.exchange = "KSE"





    ans=client.get_IB_historical_data(ibcontract)
    print(ans)

    path = '/home/pete/Documents/IBDocs/'
    fileName = path + ibcontract.symbol + ".csv"
    print(fileName)
    #ans.to_csv(path + ibcontract.symbol + ".csv")
    ans.to_csv(fileName)

"""
EXCHANGE RATES
    # GBP.USD  - OK
    ibcontract = IBcontract()
    ibcontract.secType = "CASH"
    ibcontract.currency = "USD"
    ibcontract.symbol = "GBP"
    ibcontract.exchange = "IDEALPRO"
    ibcontract.primaryExchange = "IDEALPRO"

    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry = "201607"
    ibcontract.currency = "EUR"
    ibcontract.symbol = "V2TX"
    ibcontract.exchange = "DTB"
    #ibcontract.multiplier = "10"

    #V2X - NONONONO!!!!!!!!!!!
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201607"
    ibcontract.currency="EUR"
    ibcontract.symbol="V2TX"
    ibcontract.exchange="DTB"

    #V2X - NONONONO!!!!!!!!!!!
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201609"
    ibcontract.currency="GBP"
    ibcontract.symbol="R"
    ibcontract.exchange="ICEEU

    #KR3 - OK
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201609"
    ibcontract.currency="KRW"
    ibcontract.symbol="3KTB"
    ibcontract.exchange="KSE"

    #CORN  - OK
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201612"
    ibcontract.currency="USD"
    ibcontract.symbol="ZC"
    ibcontract.exchange="ECBOT"

    #V2X - NONONONO
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.expiry="201609"
    ibcontract.currency="GBP"
    ibcontract.symbol="R"
    ibcontract.exchange="ICEEU"
"""