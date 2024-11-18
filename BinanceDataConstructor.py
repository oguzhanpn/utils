import pandas as pd
import numpy as np
import datetime as dt
import requests
import json
import multiprocessing as mp
from multiprocessing.pool import ThreadPool

class BinanceDataConstructor():

    def __init__(self,  api_row_limit=500):
        self.endpoint_url = "https://api.binance.com"
        self.api_row_limit = api_row_limit

    def get_symbols_data(self, symbol_list, start_datetime, end_datetime):

        kline_data_dict = {}
        threads_dic = {}
        proc_count = max(mp.cpu_count() - 1, 1)
        pool = ThreadPool(processes=proc_count)

        # Start pooling:
        for symbol in symbol_list:
            async_result = pool.apply_async(self.get_klines_data,
                                            args=(symbol, "1m", start_datetime, end_datetime))
            threads_dic[symbol] = async_result

        # Get results:
        for symbol in symbol_list:
            returned_df = threads_dic[symbol].get()
            kline_data_dict[symbol] = returned_df

        return kline_data_dict

    def get_klines_data(self, symbol, interval, start_datetime, end_datetime):
        
        #This method calls methods and returns the whole required data (consider it like __main__ function)
    
        #Create required parameters:
        interval_td = self.interval_to_timedelta(interval) # Convert interval to timedelta object
        self.start_datetime = start_datetime
        self.start_timestamp = str(int(start_datetime.timestamp()*1000))
        self.end_datetime = end_datetime
        self.end_timestamp = str(int(end_datetime.timestamp()*1000))
        
        #Call the methods 
        temp_start_ends = self.create_start_end_dates(interval_td)
        merged_data = self.send_requests_and_merge_data(temp_start_ends, symbol, interval)
        return merged_data
        
    def get_partial_data(self, _temp_start_timestamp, _temp_end_timestamp, symbol, interval):
        
        # This method gets a part of data using Binance API
        
        klines_extension  = "/api/v3/klines"
        parameters = {'symbol': symbol, 'interval': interval, 'startTime': _temp_start_timestamp, 'endTime': _temp_end_timestamp}
        url = self.endpoint_url + klines_extension

        # Get the data from API and filter unnecessary columns:
        api_answer = json.loads(requests.get(url, params = parameters).text)
        df = pd.DataFrame(api_answer).iloc[:, 0:6]

        # Format the columns and index:
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df['datetime'] = pd.to_datetime(df.timestamp, unit = 'ms', utc=True)
        df = df.set_index('datetime')

        return df
    
    def create_start_end_dates(self, interval_td):
        
        # This method creates start and end dates considering the 500 rows API limit.
        
        # Calculate the timestamp difference for interval:
        _temp_dt =  self.start_datetime + interval_td
        interval_timestamp = int(_temp_dt.timestamp()*1000) - int(self.start_timestamp)
        
        #Get the starting and ending timestamps:
        starting_timestamps = np.array(range(int(self.start_timestamp), int(self.end_timestamp), interval_timestamp*self.api_row_limit))
        ending_timestamps   = starting_timestamps + interval_timestamp*(self.api_row_limit - 1)
        
        #Create a df with starts and ends:
        temp_start_ends = pd.DataFrame({"temp_starts": starting_timestamps, "temp_ends" : ending_timestamps})
        
        #Fix the last end timestamp:
        temp_start_ends.iloc[-1,-1] = self.end_timestamp
        
        return temp_start_ends
        
    def send_requests_and_merge_data(self, temp_start_ends, symbol, interval):
        
        #Send the requests: (For better performance use Threads TO BE DEVELOPED...)
        #partial_klines = temp_start_ends.apply(lambda x: self.get_partial_data(x.temp_starts, x.temp_ends, symbol, interval), axis=1)

        data_dict = {}
        threads_dic = {}
        proc_count = max(mp.cpu_count() - 1, 1)
        pool = ThreadPool(processes=proc_count)

        # Start pooling:
        for i in temp_start_ends.index:
            temp_start = temp_start_ends.loc[i, 'temp_starts']
            temp_end = temp_start_ends.loc[i, 'temp_ends']

            async_result = pool.apply_async(self.get_partial_data,
                                            args=(temp_start, temp_end, symbol, interval))
            threads_dic[i] = async_result

        # Get results:
        for i in temp_start_ends.index:
            returned_df = threads_dic[i].get()
            data_dict[i] = returned_df

        # Merge the data:
        merged_data = pd.concat(list(data_dict.values()), axis=0)
        return merged_data
    
    def interval_to_timedelta(self, interval):
        
        # This method converts string interval object to a timedelta object.
        
        intervalLetter = interval[-1].lower()
        intervalNumber = int(interval[:-1])
        
        if (intervalLetter == "s"):
            interval_td = dt.timedelta(seconds = intervalNumber)
        elif (intervalLetter == "m"):
            interval_td = dt.timedelta(minutes = intervalNumber)
        elif (intervalLetter == "h"):
            interval_td = dt.timedelta(hours = intervalNumber)
        elif (intervalLetter == "d"):
            interval_td = dt.timedelta(days = intervalNumber)
        
        return interval_td
        
