#logic bug fix
import pandas as pd
class ProcessData():
    def __init__(self,df):
        df.columns = ['ID', 'Timestamp', 'Count', 'RSSI_min', 'RSSI_max', 'RSSI_avr', 'RSSI_med', 'Longitude', 'Latitude', 'Flag']
        n_df              = df.drop(columns=['RSSI_min', 'RSSI_max', 'RSSI_med', 'Flag'])
        #format ID
        n_df['ID']          = n_df['ID'].apply(lambda x: x.replace('[', ''))
        #format count
        n_df['Count'] = pd.to_numeric(n_df['Count'])
        #format RSSI_avr
        n_df['RSSI_avr'] = pd.to_numeric(n_df['RSSI_avr'])
        #format Longitude
        n_df['Longitude'] = n_df['Longitude'].apply(lambda x: float(x.strip().strip('"')))
        n_df['Longitude'] = n_df['Longitude'].apply(lambda x: float(x))
        #format Latitude
        n_df['Latitude'] = n_df['Latitude'].apply(lambda x: float(x.strip().strip('"')))
        n_df['Longitude'] = n_df['Longitude'].apply(lambda x: float(x))
        #format Flag 
        n_df = df.drop(columns=['RSSI_min', 'RSSI_max', 'RSSI_med', 'Flag'])
        #Others
        #n_df = n_df.drop(n_df[(n_df['Longitude'] == 10) & (n_df['Latitude'] == 10)].index)
        
        return n_df