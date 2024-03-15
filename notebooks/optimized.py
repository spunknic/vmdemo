import json
from datetime import datetime

class ProcessData():
    def __init__(self, input_txt_path, start_in=3):
        self.data = []
        self.times = []
        self.observation = []

        with open(input_txt_path, 'r') as file:
            for line in file:
                # Convertir la lista en un diccionario
                json_line = json.loads(line)
                data_dict = self.convert_to_dict(json_line)
                self.data.append(data_dict)

        for item in self.data:
            if 'Timestamp' in item and 'RSSI_1' in item:
                timestamp = datetime.strptime(item['Timestamp'], '%Y-%m-%dT%H:%M:%SZ')
                item['Timestamp'] = timestamp
                item['Latitude'] = float(item['Latitude'])
                item['Longitude'] = float(item['Longitude'])
                self.times.append(timestamp)
        self.times = list(set(self.times))

    def convert_to_dict(self, data_list):
        keys = ['ID', 'Timestamp', 'Flag', 'RSSI_1', 'RSSI_2', 'RSSI_3', 'RSSI_4', 'Longitude', 'Latitude', 'Status']
        #data_list = line.strip().split(',')
        data_dict = {keys[i]: data_list[i] for i in range(len(keys))}
        return {k: v for k, v in data_dict.items() if k in ['ID', 'Timestamp', 'RSSI_1', 'Latitude', 'Longitude']}

    def pipeline(self, n=3):
        order = 'Do nothing'
        push = 0
        for i in range(len(self.times) // n):
            self.observation = []
            my_time = self.times[i:i+n]

            for item in self.data:
                if 'Timestamp' in item and 'RSSI_1' in item:
                    if item['Timestamp'] in my_time and int(item['RSSI_1']) >= -60:
                        self.observation.append(item)

            if any('RSSI_1' in item and int(item['RSSI_1']) >= -60 for item in self.observation):
                state = []
                for item in self.observation:
                    if 'RSSI_1' in item and int(item['RSSI_1']) >= -60:
                        state.append({'ID': item['ID'], 'state': 'near', 'time': item['Timestamp']})

                if state:
                    if len(set(item['ID'] for item in state)) == 1 and sum(1 for item in state if item['state'] == 'near') >= 2:
                        order = 'Push to Jam'
                        push += 1
                        print(order, ' => ', state[-1]['ID'], str(state[-1]['time']))
                        order = 'Do nothing'
                    elif sum(1 for item in state if item['state'] == 'near') > 2:
                        state = [{'ID': item['ID'], 'state': 'truck', 'time': item['time']} for item in state]
                        order = 'Do nothing'
                        self.observation = []
        print('Push to Jam: ', push)


if __name__ == "__main__":
    pro = ProcessData('C:/Users/juan.david/projects/garda/data/logs_8668.txt')
    pro.pipeline()
