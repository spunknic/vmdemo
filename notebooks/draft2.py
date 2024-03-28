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

def norm_RRSI(E):
    if E<=-60:
        E= 0
    elif E>-60 and E<-40:
        E=abs(100+E)
    else:
        E=100
    return E

def norm_count(C):
        return 50 if C > 25 else 0

def trust(my_row,Cmax=50,Emax=100,wC=0.5,wE=0.5,th=0.5):#-40,-60   
    C = norm_count(my_row['Count']) 
    E = norm_RRSI(my_row['RSSI_avr'])
    #T_= (wC*(C/Cmax)+wE*(1-(E/Emax)))/(wC+wE)
    T_= (wC*(C/Cmax)+wE*(E/Emax))/(wC+wE)
    
    if T_>=th:
        return True
    else: 
        return False
    
def trust_old(my_row):
    C = my_row['Count'] 
    E = my_row['RSSI_avr']
    if E>=-60:RE=True
    else:     RE=False

    if C>=25: RC=True
    else: RC=False

    if RE and RC:return True
    else: return False
        
class ProcessData():
    def __init__(self,input_txt_path):
        input_path = PATH(input_txt_path)
        data  = list()
        with open(input_path, 'r') as file:
            for line in file:
                json_line = json.loads(line)
                data.append(json_line)
        self.df                = pd.DataFrame(data, columns=['ID', 'Timestamp', 'Count', 'RSSI_min', 'RSSI_max', 'RSSI_avr', 'RSSI_med', 'Longitude', 'Latitude', 'Flag'])
        self.n_df              = self.df.drop(columns = ['RSSI_min','RSSI_max','RSSI_med','Flag'])
        self.n_df              = self.n_df.drop(self.n_df[(self.n_df['Longitude'] == 10) & (self.n_df['Latitude'] == 10)].index)
        #self.n_df['Timestamp'] = pd.to_datetime(self.n_df['Timestamp'], format='%Y-%m-%dT%H:%M:%SZ')
        self.n_df['Timestamp'] = self.n_df['Timestamp'].apply(lambda x: convert_to_quebec_time(x))
        self.n_df['Latitude']  = pd.to_numeric(self.n_df['Latitude'])
        self.n_df['Longitude'] = pd.to_numeric(self.n_df['Longitude'])
        
        self.times       = self.n_df['Timestamp'].unique()
        self.observation = pd.DataFrame()
  
    def pipeline(self,n=3,min_trust=0.5,inhand_t=2,option='filter',count_att=5,rssi_att=1):
        push_coords = []
        my_output   = []
        if option =='all':
            for i in range(len(self.n_df['Timestamp'])):
                self.observation = self.n_df.iloc[i]
                if trust(self.observation,th=min_trust,wC=count_att,wE=rssi_att):
                    my_output.append([self.observation['Latitude'],self.observation['Longitude'],self.observation['Count'],self.observation['ID'],'no filter'])
            
            return my_output
        elif option =='filter':
            for i in range(len(self.times)//n):
                self.state = pd.DataFrame({'ID': [],'state': [],'time': [],'coords':[]}) # reset state
                my_time = self.times[i:i+n]
                
                #Get Observation
                self.observation = self.n_df[self.n_df['Timestamp'].isin(my_time)] 
                
                if trust(self.observation.loc[self.observation['Count'].idxmax()],th=min_trust,wC=count_att,wE=rssi_att):
                    
                    for index, row in self.observation.iterrows():#Take window matrix  
                        
                        if trust(row,th=min_trust,wC=count_att,wE=rssi_att): 
                            element    = pd.DataFrame({'ID': [row['ID']],'Count':row['Count'],'RSSI_avr':row['RSSI_avr'], 'state': ['near'], 'time': [row['Timestamp']],'coords':[(row['Latitude'],row['Longitude'])]})
                            self.state = pd.concat([self.state, element], ignore_index=True)
                            
                    if not self.state.empty:
                        if (self.state['ID'].nunique()==1) and (self.state[self.state['state']=='near'].count()[-1]>=inhand_t):#check if same ID has same 
                            self.state['state']='in hand'
                            candidate =self.state.tail(1)
                            
                            push_coords.append([candidate['coords'].iloc[-1][0],candidate['coords'].iloc[-1][1],candidate['Count'].iloc[-1],candidate['ID'].iloc[-1],candidate['state'].iloc[-1]])
                            self.state = self.state.drop(self.state.index) #Reset df

                        if ((self.state['ID'].nunique()>1)) and (self.state[self.state['state']=='near'].count()[-1]>=2):
                            self.state['near_count'] = self.state.groupby('state')['state'].transform('count')
                            self.state.loc[self.state['near_count'] >= 2, 'state'] = 'truck'
                            self.state.drop(columns=['near_count'], inplace=True)
                            candidate =self.state.tail(1)

                            if trust(self.state.loc[self.state['Count'].idxmax()]):
                                idx_max = self.state['Count'].idxmax()
                                self.state.at[idx_max, 'state'] = self.state.at[idx_max, 'state'].replace('truck', 'in hand')

                            push_coords.append([candidate['coords'].iloc[-1][0],candidate['coords'].iloc[-1][1],candidate['Count'].iloc[-1],candidate['ID'].iloc[-1],candidate['state'].iloc[-1]])
                            self.state = self.state.drop(self.state.index) #Reset df
                        else:
                            self.state = self.state.drop(self.state.index) #Reset df

                    my_output.append(push_coords)
                    push_coords = []
            
            return [item for sublist in my_output for item in sublist]

if __name__ == "__main__":
    pro = ProcessData('C:/Users/juan.david/projects/garda/data/logs_Daniel.txt')      
    my_result = pro.pipeline(min_trust=0.7,inhand_t=2,n=2,option='filter',count_att=8,rssi_att=3)
    print(my_result)             
                