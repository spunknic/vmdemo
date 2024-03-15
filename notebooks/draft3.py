#
import pandas as pd
import json
import copy
from pathlib import Path as PATH

from itertools import islice

from datetime import datetime, timedelta
import pytz

#IF RSSI <= -60 AND Count > 25 THEN Near
#IF Near 3 times in a rows AND OnlyOne IS NEAR THEN tag.status = InHand
#IF Plusieur tag.status IS InHand 
#IF Away AND last is InHand THEN Report last to JL
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
def trust(C,E,Cmax=50,Emax=-40,wC=1,wE=0.5):
    T_=(wC*(C/Cmax)+wE*(1-(E/Emax)))/(wC+wE)
    return T_
    #Confianza=w1​+w2​w1​⋅Cmax​C​+w2​⋅(1−Emax​E​)​
class ProcessData():
    def __init__(self,input_txt_path):
        input_path = PATH(input_txt_path)
        data  = list()
        with open(input_path, 'r') as file:
            for line in file:
                json_line = json.loads(line)
                data.append(json_line)
        self.df                = pd.DataFrame(data, columns=['ID', 'Timestamp', 'Count', 'RSSI_1', 'RSSI_2', 'RSSI_3', 'RSSI_4', 'Longitude', 'Latitude', 'Status'])
        self.n_df              = self.df.drop(columns = ['RSSI_2','RSSI_3','RSSI_4','Status','Count'])
        #self.n_df['Timestamp'] = pd.to_datetime(self.n_df['Timestamp'], format='%Y-%m-%dT%H:%M:%SZ')
        self.n_df['Timestamp'] = self.n_df['Timestamp'].apply(lambda x: convert_to_quebec_time(x))
        self.n_df['Latitude']  = pd.to_numeric(self.n_df['Latitude'])
        self.n_df['Longitude'] = pd.to_numeric(self.n_df['Longitude'])
        
        self.times       = self.n_df['Timestamp'].unique()
        self.observation = pd.DataFrame()
  
    def pipleine(self,n=3,min_rssi=-60,inhand_t=2,option='filter'):
        order='Do nothing'
        push = 0
        push_coords = []
        if option =='all':
            for i in range(len(self.n_df['Timestamp'])):
                self.observation = self.n_df.iloc[i]
                
                if self.observation['RSSI_1']>=min_rssi:
                    push_coords.append([self.observation['Latitude'],self.observation['Longitude'],self.observation['RSSI_1'],self.observation['ID'],'near'])
            return push_coords
        elif option =='filter':
            for i in range(len(self.times)//n):
                self.state = pd.DataFrame({'ID': [],'state': [],'time': [],'coords':[]}) # reset state
                my_time = self.times[i:i+n]
                #Get Observation
                self.observation = self.n_df[self.n_df['Timestamp'].isin(my_time)] 
                if self.observation['RSSI_1'].max()>=min_rssi:  
                    for index, row in self.observation.iterrows():#Take window matrix  
                        
                        if row['RSSI_1']>=min_rssi: 
                            element    = pd.DataFrame({'ID': [row['ID']],'RSSI_1':row['RSSI_1'], 'state': ['near'], 'time': [row['Timestamp']],'coords':[(row['Latitude'],row['Longitude'])]})
                            self.state = pd.concat([self.state, element], ignore_index=True)
                            
                    if not self.state.empty:
                        if (self.state['ID'].nunique()==1) and (self.state[self.state['state']=='near'].count()[-1]>=inhand_t):#check if same ID has same 
                            order = 'Push to Jam'
                            push+=1
                            self.state['state']='in hand'
                            push_coords.append([self.state['coords'][0][0],self.state['coords'][0][1],self.state['RSSI_1'].tail(1).iloc[0],self.state['ID'],self.state['state'][0]])
                            
                            self.state = pd.DataFrame({'ID': [],'state': [],'time': [],'coords':[]}) # reset state
                            order = 'Do nothing'
                        if self.state[self.state['state']=='near'].count()[-1]>2:
                            self.state['near_count'] = self.state.groupby('state')['state'].transform('count')
                            self.state.loc[self.state['near_count'] > 2, 'state'] = 'truck'
                            self.state.drop(columns=['near_count'], inplace=True)
                            push_coords.append([self.state['coords'][0][0],self.state['coords'][0][1],self.state['RSSI_1'].tail(1).iloc[0],self.state['ID'],self.state['state'][0]])
                            order = 'Do nothing'
                            self.state = pd.DataFrame({'ID': [],'state': [],'time': [],'coords':[]}) 

            return push_coords

if __name__ == "__main__":
    pro = ProcessData('C:/Users/juan.david/projects/garda/data/logs_9013.txt')      
    pro.pipleine(min_rssi=-60,inhand_t=1,n=1,option='filter')             
                