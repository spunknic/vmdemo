import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timedelta
import pytz

class ProcessData:
    def __init__(self, input_txt_path):
        input_path = Path(input_txt_path)
        with open(input_path, 'r') as file:
            data = [json.loads(line) for line in file]
        self.df = pd.DataFrame(data, columns=['ID', 'Timestamp', 'Count', 'RSSI_min', 'RSSI_max', 'RSSI_avr', 'RSSI_med', 'Longitude', 'Latitude', 'Flag'])
        self.n_df = self.df[['ID', 'Timestamp', 'Count', 'RSSI_avr', 'Longitude', 'Latitude']].copy()
        self.n_df = self.n_df[(self.n_df['Longitude'] != 10) & (self.n_df['Latitude'] != 10)]
        self.n_df['Timestamp'] = self.n_df['Timestamp'].apply(self.convert_to_quebec_time)

    def convert_to_quebec_time(self, original_time):
        dt = datetime.strptime(original_time, "%Y-%m-%dT%H:%M:%SZ")
        quebec_timezone = pytz.timezone('America/Montreal')
        dt_quebec = dt.replace(tzinfo=pytz.utc).astimezone(quebec_timezone) + timedelta(hours=1)
        return dt_quebec.strftime("%Y-%m-%d %H:%M:%S %Z%z")

    def trust(self, row, Cmax=50, Emax=-50, wC=5, wE=1, th=0.5):
        C = row['Count']   
        E = row['RSSI_avr']
        T = (wC * (C / Cmax) + wE * (1 - (E / Emax))) / (wC + wE)
        return T >= th

    def pipeline(self, n=3, min_trust=0.5, inhand_t=2, option='filter', count_att=5, rssi_att=1):
        push_coords = []
        for i in range(0, len(self.n_df['Timestamp']), n):
            state = pd.DataFrame(columns=['ID', 'state', 'time', 'coords'])
            my_time = self.n_df.iloc[i:i+n]['Timestamp']
            observation = self.n_df[self.n_df['Timestamp'].isin(my_time)] 

            if self.trust(observation.iloc[observation['Count'].idxmax()], th=min_trust, wC=count_att, wE=rssi_att):
                for _, row in observation.iterrows():
                    if self.trust(row, th=min_trust, wC=count_att, wE=rssi_att):
                        state = state.append({'ID': row['ID'], 'Count': row['Count'], 'RSSI_avr': row['RSSI_avr'], 'state': 'near', 'time': row['Timestamp'], 'coords': (row['Latitude'], row['Longitude'])}, ignore_index=True)

                if not state.empty:
                    if (state['ID'].nunique() == 1) and (state[state['state'] == 'near'].count()[-1] >= inhand_t):
                        state['state'] = 'in hand'
                        candidate = state.tail(1)
                        push_coords.append([candidate['coords'].iloc[-1][0], candidate['coords'].iloc[-1][1], candidate['Count'].iloc[-1], candidate['ID'].iloc[-1], candidate['state'].iloc[-1]])

                    elif (state['ID'].nunique() > 1) and (state[state['state'] == 'near'].count()[-1] >= 2):
                        state.loc[state.groupby('state')['state'].transform('count') >= 2, 'state'] = 'truck'
                        candidate = state.tail(1)
                        push_coords.append([candidate['coords'].iloc[-1][0], candidate['coords'].iloc[-1][1], candidate['Count'].iloc[-1], candidate['ID'].iloc[-1], candidate['state'].iloc[-1]])

        return push_coords

if __name__ == "__main__":
    pro = ProcessData('C:/Users/juan.david/projects/garda/data/logs_Daniel.txt')      
    my_result = pro.pipeline(min_trust=0.5, inhand_t=2, n=2, option='filter', count_att=5, rssi_att=1)
    print(my_result)
