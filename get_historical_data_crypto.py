# We import the necessary modules for the correct execution of the code
import bitfinex
import time
import datetime
import pandas as pd
from tqdm import tqdm

# Define query parameters
pair = 'XRPUSD' # Currency pair of interest
bin_size = '1m' # This will return minute data
limit = 1000    # We want the maximum of 1000 data points 
api_key = 'Lm4QXQhVHUH24UeY2Ym4ih5NnpYYk3TtG8V4hb9jJiC' #your apikey of bitfinex
api_secret = 'Bs7bjJUUUl4byEMUFdApx7tbzSd2pSFEA7YE2mcOpQe' # your secret key of bitfinex

# In this function I prepare the query through the api, taking into account the batch of requests and also the number of steps
def fetch_data(start, stop, symbol, interval, tick_limit, step):
    # Create api instance
    api_v2 = bitfinex.bitfinex_v2.api_v2(api_key=api_key, api_secret=api_secret)
    data = []
    start = start - step
    while start < stop:
        start = start + step
        end = start + step
        res = api_v2.candles(symbol=symbol, interval=interval,
                             limit=tick_limit, start=start,
                             end=end)
        print(str(start)+' >>> '+ str(end)+' ---- '+str(stop))
        # print(res if res else "Respuesta sin datos... ")
        if res:
            data.extend(res)
            time.sleep(5)
        else:
            print("Respuesta sin datos...")
            time.sleep(5)

    return data

# Set step size
time_step = 90000000
# Define the start date 
t_start = datetime.datetime(2018, 1, 1, 0, 0)
t_start = time.mktime(t_start.timetuple()) * 1000
# Define the end date
t_stop = datetime.datetime(2019, 11, 30, 0, 0)
t_stop = time.mktime(t_stop.timetuple()) * 1000
pair_data = fetch_data(start=t_start, stop=t_stop, symbol=pair,
                       interval=bin_size, tick_limit=limit, 
                       step=time_step)

if pair_data:

    # Create pandas data frame and clean/format data
    names = ['time', 'open', 'close', 'high', 'low', 'volume']
    df = pd.DataFrame(pair_data, columns=names)
    df.drop_duplicates(inplace=True)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df.set_index('time', inplace=True)
    df.sort_index(inplace=True)
    df.to_csv('./recolect/data_1.csv')