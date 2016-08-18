import time
import datetime
from pytz import timezone

exchange = 'KE'

if exchange == 'KSE':
    yesterday = datetime.datetime.now(timezone('Asia/Tokyo')) - datetime.timedelta(1)
    b4midnight = yesterday.replace(hour=23, minute=59, second=59)
    print(b4midnight.strftime("%Y%m%d %H:%M:%S %Z"))
else:
    yesterday = datetime.datetime.now(timezone('GMT')) - datetime.timedelta(1)
    b4midnight = yesterday.replace(hour=23, minute=59, second=59)
    print(b4midnight.strftime("%Y%m%d %H:%M:%S %Z"))





#dt2 = dt(timezone('Asia/Tokyo'))
#print(dt2)
'''
yesterday = datetime.date.today() - datetime.timedelta(1)
print(time.strftime("%Y%m%d %H:%M:%S"))
print(today.strftime("%Y%m%d %H:%M:%S %Z"))
print(yesterday.timezone('UTC').strftime("%Y%m%d %H:%M:%S %Z"))
dt = datetime.datetime.combine(yesterday,t)
print(dt)
print(dt.strftime("%Y%m%d %H:%M:%S %Z"))
'''