from dbnew import setup_blank_tables
from dbconnect import get_db_filename, connection
from dbtablestatic import staticdata
from dbtablets import tsdata
import os
import pandas as pd
from successrights import g_access_key

dbname="mydb"
dbfilename=get_db_filename(dbname)

try:
    os.remove(dbfilename)
except:
    pass

setup_blank_tables(dbfilename, ["CREATE TABLE timeseries (datetime text, code text, price float)", 
                          "CREATE TABLE static (code text, fullname text)"])


setup_blank_tables(dbfilename, ["CREATE TABLE marketdata (carver text, datefrom text, delta float," +
                                " done text, quandl text,ib text, sectype text, currency text, " +
                                " qunadl_exchange text, ib_exchange text, multiplier integer, " +
                                " price text, carry text)"])

st_table=staticdata(dbname)
st_table.add("FTSE", "FTSE 100 index")
assert st_table.read("FTSE")=="FTSE 100 index"
st_table.modify("FTSE", "FTSE all share")
assert st_table.read("FTSE")=="FTSE all share"
#st_table.delete("FTSE")
#assert st_table.read("FTSE") is None


dt_table=tsdata(dbname)

somprices=pd.TimeSeries(range(100), pd.date_range('1/1/2014', periods=100))

dt_table.add("FTSE", somprices)

assert dt_table.read("FTSE").values[-1]==99.0

## Remove the file so example is clean next time
#os.remove(dbfilename)

print("Yes No problems")