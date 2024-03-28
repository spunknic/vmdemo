import pandas as pd
import json
from datetime import datetime, timedelta
import pytz

class ProcessData:
    def __init__(self, input_txt_path):
        self.df = pd.read_csv(input_txt_path, names=['ID', 'Timestamp', 'Count', 'RSSI_min', 'RSSI_max', 'RSSI_avr', 'RSSI_med', 'Longitude', 'Latitude', 'Flag'])
        self.df = self.df[(self.df['Longitude'] != 10) & (self.df['Latitude'] != 10)]
        self.df['Timestamp'] = self.df['Timestamp'].apply(self.convert_to_quebec_time)
        self.times = self.df['Timestamp'].unique()
        self.observation = pd.DataFrame()

    def convert_to_quebec_time(self, original_time):
        original_time = original_time.strip()  # Eliminar espacios adicionales alrededor de la cadena de tiempo
        dt = datetime.strptime(original_time, "\"%Y-%m-%dT%H:%M:%SZ\"")  # Agregar comillas dobles al patrón de formato
        quebec_timezone = pytz.timezone('America/Montreal')
        dt_quebec = dt.replace(tzinfo=pytz.utc).astimezone(quebec_timezone) + timedelta(hours=1)
        return dt_quebec.strftime("%Y-%m-%d %H:%M:%S %Z%z")


    def trust(self, my_row, Cmax=50, Emax=100, wC=0.5, wE=0.5, th=0.5):
        C = self.norm_count(my_row['Count'])
        E = self.norm_RRSI(my_row['RSSI_avr'])
        T_ = (wC * (C / Cmax) + wE * (E / Emax)) / (wC + wE)
        return T_ >= th

    def norm_RRSI(self, E):
        if E <= -60:
            return 0
        elif -60 < E < -40:
            return abs(100 + E)
        else:
            return 100

    def norm_count(self, C):
        return 50 if C > 25 else 0

    def pipeline(self, n=3, min_trust=0.5, inhand_t=2, option='filter', count_att=5, rssi_att=1):
        order = 'Do nothing'
        push = 0
        push_coords = []
        my_output = []

        if option == 'all':
            for index, row in self.df.iterrows():
                if self.trust(row, th=min_trust, wC=count_att, wE=rssi_att):
                    my_output.append([row['Latitude'], row['Longitude'], row['Count'], row['ID'], 'no filter'])

        elif option == 'filter':
            for i in range(0, len(self.times), n):
                self.state = pd.DataFrame({'ID': [], 'state': [], 'time': [], 'coords': []})
                my_time = self.times[i:i + n]
                self.observation = self.df[self.df['Timestamp'].isin(my_time)]

                if self.trust(self.observation.loc[self.observation['Count'].idxmax()], th=min_trust, wC=count_att, wE=rssi_att):
                    for index, row in self.observation.iterrows():
                        if self.trust(row, th=min_trust, wC=count_att, wE=rssi_att):
                            element = pd.DataFrame({'ID': [row['ID']], 'Count': row['Count'], 'RSSI_avr': row['RSSI_avr'], 'state': ['near'], 'time': [row['Timestamp']], 'coords': [(row['Latitude'], row['Longitude'])]})

                            self.state = pd.concat([self.state, element], ignore_index=True)

                    if not self.state.empty:
                        if (self.state['ID'].nunique() == 1) and (self.state[self.state['state'] == 'near'].count()[-1] >= inhand_t):
                            order = 'Push to Jam'
                            push += 1
                            self.state['state'] = 'in hand'
                            candidate = self.state.tail(1)
                            push_coords.append([candidate['coords'].iloc[-1][0], candidate['coords'].iloc[-1][1], candidate['Count'].iloc[-1], candidate['ID'].iloc[-1], candidate['state'].iloc[-1]])
                            self.state = pd.DataFrame()

                        if ((self.state['ID'].nunique() > 1)) and (self.state[self.state['state'] == 'near'].count()[-1] >= 2):
                            self.state['near_count'] = self.state.groupby('state')['state'].transform('count')
                            self.state.loc[self.state['near_count'] >= 2, 'state'] = 'truck'
                            self.state.drop(columns=['near_count'], inplace=True)
                            candidate = self.state.tail(1)

                            if self.trust(self.state.loc[self.state['Count'].idxmax()]):
                                idx_max = self.state['Count'].idxmax()
                                self.state.at[idx_max, 'state'] = self.state.at[idx_max, 'state'].replace('truck', 'in hand')

                            push_coords.append([candidate['coords'].iloc[-1][0], candidate['coords'].iloc[-1][1], candidate['Count'].iloc[-1], candidate['ID'].iloc[-1], candidate['state'].iloc[-1]])
                            order = 'Do nothing'
                            self.state = pd.DataFrame()

                        my_output.append(push_coords)
                        push_coords = []

        return [item for sublist in my_output for item in sublist]

if __name__ == "__main__":
    pro = ProcessData('C:/Users/juan.david/projects/garda/data/logs_Daniel.txt')      
    my_result = pro.pipeline(min_trust=0.7, inhand_t=2, n=2, option='filter', count_att=8, rssi_att=3)
    print(my_result)