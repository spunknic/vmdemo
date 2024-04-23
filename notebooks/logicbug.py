#logic bug fix
import pandas as pd
import pandas as pd
from datetime import datetime, timedelta
import pytz
import statistics

from dateutil import parser

def parse_date_string(date_str):
    date_str = date_str[:-9]
    try:
        # Parse the date string
        parsed_date = parser.parse(date_str, fuzzy=True)
        return parsed_date
    except ValueError:
        # Handle parsing errors gracefully
        print("Error: Unable to parse the date string.")
        return None

def convert_to_quebec_time(original_time):
    # Convert the string to a datetime object
    dt = datetime.strptime(original_time, "%Y-%m-%dT%H:%M:%SZ")
    
    # Define the timezone for Quebec, considering daylight saving time
    quebec_timezone = pytz.timezone('America/Montreal')
    
    # Convert to Quebec time
    dt_quebec = dt.replace(tzinfo=pytz.utc).astimezone(quebec_timezone)
    
    # Add one additional hour to the Quebec time
    dt_quebec += timedelta(hours=1)
    
    # Return the Quebec time with the added hour as a string
    return dt_quebec.strftime("%Y-%m-%d %H:%M:%S %Z%z")

class ProcessData():
    def __init__(self,df):
        df.columns = ['ID', 'Timestamp', 'Count', 'RSSI_min', 'RSSI_max', 'RSSI_avr', 'RSSI_med', 'Longitude', 'Latitude', 'Flag']
        n_df              = df.drop(columns=['RSSI_min', 'RSSI_med', 'Flag'])#, 'RSSI_max'
        #format ID
        n_df['ID']          = n_df['ID'].apply(lambda x: x.replace('[', ''))
        #format count
        n_df['Count'] = pd.to_numeric(n_df['Count'])
        #format RSSI_avr
        n_df['RSSI_avr'] = pd.to_numeric(n_df['RSSI_avr'])
        #format Longitude
        n_df['Longitude'] = n_df['Longitude'].apply(lambda x: x.strip().strip('"'))
        n_df['Longitude'] = pd.to_numeric(n_df['Longitude'])
        #format Latitude
        n_df['Latitude'] = n_df['Latitude'].apply(lambda x: x.strip().strip('"'))
        n_df['Latitude'] = pd.to_numeric(n_df['Latitude'])
        
        #Others
        n_df = n_df.drop(n_df[(n_df['Longitude'] == 10) & (n_df['Latitude'] == 10)].index)
        #n_df['Timestamp'] = self.n_df['Timestamp'].apply(lambda x: convert_to_quebec_time(x))
        
        self.n_df = n_df
    
        self.times       = self.n_df['Timestamp'].unique()
        self.ids         = list(set(self.n_df['ID']))
        self.observation = pd.DataFrame()
    
    def one_tag_list(self):
        return self.ids
    def checkdf(self):
        return self.n_df
    
    def one_tag_raw(self,tagid):
        if tagid=='':
            return self.n_df
        else:
            return self.n_df[self.n_df['ID']==tagid]
    
    def all_tag_raw(self):
        return self.n_df
    
    def one_tag_last(self,tagid):
        my_results = []
        if tagid =='':
            for id in self.ids:
                id_df     = self.n_df[self.n_df['ID']==id]
                my_last   = id_df.tail(1)
                my_results.append(my_last)
            if my_results:
                return pd.concat(my_results, ignore_index=True)
            else:
                return None
        else:
            my_reg = self.n_df[self.n_df['ID']==tagid]
            my_last = my_reg.tail(1)
            return my_last
    
    def one_tag_last_count(self,tagid,Cth):
        my_results = []

        if tagid =='':
            for id in self.ids:
                id_df     = self.n_df[self.n_df['ID']==id]
                id_df_c   = id_df[id_df['Count']>=Cth]
                my_last   = id_df_c.tail(1)
                my_results.append(my_last)
            
            if my_results:
                return pd.concat(my_results, ignore_index=True)
            else:
                return None
        else:
            my_reg   = self.n_df[self.n_df['ID']==tagid]
            id_df_c  = my_reg[my_reg['Count']>=Cth]
            my_last  = id_df_c.tail(1)
            return my_last
    
    def one_tag_last_rssi(self,tagid,rssith):
        my_results = []
        if tagid =='':
            for id in self.ids:
                id_df     = self.n_df[self.n_df['ID']==id]
                id_df_c   = id_df[id_df['RSSI_avr']>=rssith]
                my_last   = id_df_c.tail(1)
                my_results.append(my_last)
            
            if my_results:
                return pd.concat(my_results, ignore_index=True)
            else:
                return None
        else:
            my_reg   = self.n_df[self.n_df['ID']==tagid]
            id_df_c  = my_reg[my_reg['RSSI_avr']>=rssith]
            my_last  = id_df_c.tail(1)
            return my_last
        
        
    def one_tag_last_count_rssi(self,tagid,rssith,Cth):
        my_results = []
        if tagid =='':
            for id in self.ids:
                id_df     = self.n_df[self.n_df['ID']==id]
                id_df_c   = id_df[(id_df['RSSI_avr']>=rssith) & (id_df['Count']>=Cth)]
                my_last   = id_df_c.tail(1)
                my_results.append(my_last)
            
            if my_results:
                return pd.concat(my_results, ignore_index=True)
            else:
                return None
        else:
            my_reg   = self.n_df[self.n_df['ID']==tagid]
            id_df_c  = my_reg[(my_reg['RSSI_avr']>=rssith) & (my_reg['Count']>=Cth)] 
            my_last  = id_df_c.tail(1)
            return my_last
        
    def gil_algo(self,RSSIlimit,ClampSize,CountLimit,Ratiotheshold,option):
        result = []
        for id in self.ids:
            id_df     = self.n_df[self.n_df['ID']==id]
            for index, row in id_df.iterrows():
                if option =='max':
                    facteurRssi = row['RSSI_max']+RSSIlimit
                else:
                    facteurRssi = row['RSSI_avr']+RSSIlimit
                Rssi0_20 = statistics.median(sorted([0,facteurRssi,ClampSize]))  
                my_hand     = ((row['Count']/CountLimit)*(Rssi0_20/ClampSize))*100
                if my_hand >=Ratiotheshold:
                    result.append(row) 
        res= pd.DataFrame(result)
        print(res.shape)
        
        return res
    
    def gil_algo_time_window(self,RSSIlimit,ClampSize,CountLimit,Ratiotheshold,timewindow):
        result      = []
        result_gil  = self.gil_algo(RSSIlimit,ClampSize,CountLimit,Ratiotheshold)
        ids         = list(set(result_gil['ID']))
        
        for id in ids:
            id_df       = result_gil[result_gil['ID']==id].sort_values('Timestamp')
            truewindow_=check_consecutive_rows(id_df,timewindow)
            if len(truewindow_)!=0:
                result.append(id_df[id_df['Timestamp'] == truewindow_[-1]].tail(1))
                #result.append(id_df.tail(1))
        concatenated_df = pd.concat(result)
        
        return concatenated_df

def check_consecutive_rows(df,n,my_way=2):
    if my_way==1:
        if len(df) < n:
            return False
        #One way
        timestamps = df['Timestamp'].apply(lambda x: parse_date_string(x))
        
        time_diffs = [(timestamps.iloc[i] - timestamps.iloc[i - 1]).total_seconds() for i in range(1, len(timestamps))]
        #print(time_diffs)
        
        consecutive_count = 1
        for diff in time_diffs:
            if diff == 1:
                consecutive_count += 1
                if consecutive_count <= n:
                    return True
            else:
                consecutive_count = 1
        
        return False
    else:
        #second way:
        to_return = []
        my_times = df['Timestamp'].unique()
        for t in my_times:
            c_df = df[df['Timestamp']==t]
            if len(c_df) < n:
                pass
            elif len(c_df)>=n:
                to_return.append(t)
        #print(to_return)
        return to_return
                