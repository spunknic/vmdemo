import ujson as json
from time import mktime, localtime

class DataFrame:
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns

    def __getitem__(self, key):
        return [row[key] for row in self.data]

    def __iter__(self):
        return iter(self.data)

    def iterrows(self):
        for row in self.data:
            yield row
    def drop(self, columns):
        for row_index in range(len(self.data)):
            for col in columns:
                if col in self.data[row_index]:
                    del self.data[row_index][col]
    def __len__(self):
        return len(self.data)

    def __bool__(self):
        return bool(self.data)

# Replace time conversion and timezone functions
def convert_to_quebec_time(original_time):
    dt = localtime(mktime(original_time))
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d} {:s}".format(dt[0], dt[1], dt[2], dt[3], dt[4], dt[5], "UTC+0100")

# Replace trust function
def trust(my_row, Cmax=50, Emax=-40, wC=5, wE=1, th=0.5):
    C = my_row['Count']
    E = my_row['RSSI_avr']
    T_ = (wC * (C / Cmax) + wE * (1 - (E / Emax))) / (wC + wE)
    print(T_, th)
    return T_ >= th

# Replace file manipulation with MicroPython functions
class ProcessData:
    def __init__(self, input_txt_path):
        with open(input_txt_path, 'r') as file:
            data = [json.loads(line) for line in file]
        self.df = DataFrame(data, columns=['ID', 'Timestamp', 'Count', 'RSSI_min', 'RSSI_max', 'RSSI_avr', 'RSSI_med', 'Longitude', 'Latitude', 'Flag'])
        self.n_df = self.df
        self.n_df.drop(['RSSI_min', 'RSSI_max', 'RSSI_med', 'Flag'])

    def pipeline(self, n=3, min_trust=0.5, inhand_t=2, option='filter', count_att=5, rssi_att=1):
        order = 'Do nothing'
        print('Trust   ', min_trust)
        push = 0
        push_coords = []
        my_output = []
        if option == 'all':
            for row in self.n_df:
                if trust(row, th=min_trust, wC=count_att, wE=rssi_att):
                    my_output.append([row['Latitude'], row['Longitude'], row['Count'], row['ID'], 'no filter'])
            return my_output
        elif option == 'filter':
            for i in range(len(self.n_df) // n):
                self.state = DataFrame([], columns=['ID', 'state', 'time', 'coords'])  # reset state
                my_time = self.n_df['Timestamp'][i:i+n]

                # Get Observation
                self.observation = [row for row in self.n_df if row['Timestamp'] in my_time]

                if trust(self.observation[self.observation['Count'].index(max(self.observation['Count']))], th=min_trust, wC=count_att, wE=rssi_att):
                    for row in self.observation:
                        if trust(row, th=min_trust, wC=count_att, wE=rssi_att):
                            element = {'ID': row['ID'], 'Count': row['Count'], 'RSSI_avr': row['RSSI_avr'],
                                       'state': 'near', 'time': row['Timestamp'], 'coords': (row['Latitude'], row['Longitude'])}
                            self.state.data.append(element)

                    if self.state:
                        if len(set(self.state['ID'])) == 1 and self.state['state'].count('near')[-1] >= inhand_t:
                            order = 'Push to Jam'
                            push += 1
                            self.state['state'] = 'in hand'
                            candidate = self.state.data[-1]
                            push_coords.append([candidate['coords'][0], candidate['coords'][1], candidate['Count'], candidate['ID'], candidate['state']])
                            self.state.data = []

                        if len(set(self.state['ID'])) > 1 and self.state['state'].count('near')[-1] >= 2:
                            self.state['near_count'] = [self.state['state'].count(state) for state in self.state['state']]
                            self.state.data = [row if count < 2 else 'truck' for row, count in zip(self.state.data, self.state['near_count'])]
                            self.state.drop(['near_count'])
                            candidate = self.state.data[-1]
                            push_coords.append([candidate['coords'][0], candidate['coords'][1], candidate['Count'], candidate['ID'], candidate['state']])
                            order = 'Do nothing'
                            self.state.data = []
                my_output.append(push_coords)
                push_coords = []
            return [item for sublist in my_output for item in sublist]

if __name__ == "__main__":
    pro = ProcessData('C:/Users/juan.david/projects/garda/data/logs_Daniel.txt')
    my_result = pro.pipeline(min_trust=0.5, inhand_t=2, n=2, option='filter', count_att=5, rssi_att=1)
    print(my_result)
