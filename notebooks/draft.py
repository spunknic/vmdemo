import pandas as pd
import json
import numpy as np
import random 
from tqdm import tqdm

import matplotlib.pyplot as plt

def generate_random_hex_color():
    """
    Generates a random hexadecimal color code.

    Returns:
        str: A string representing the hexadecimal color code.
    """
    # Generate three random numbers between 0 and 255
    r = random.randint(0, 255)
    g = random.randint(0, 255)
    b = random.randint(0, 255)
    
    # Format the numbers into a hexadecimal color code
    return f'#{r:02x}{g:02x}{b:02x}'

# Example usage
random_color = generate_random_hex_color()

data = []
my_stats={}

with open("C:/Users/juan.david/projects/garda/data/logs (Gilo_deploy1).txt", 'r') as file:
    for line in file:
        json_line = json.loads(line)
        
        data.append(json_line)

n_df  = pd.DataFrame(data, columns=['ID', 'Timestamp', 'Flag', 'RSSI_1', 'RSSI_2', 'RSSI_3', 'RSSI_4', 'Longitude', 'Latitude', 'Status'])
df = n_df[(n_df['Latitude'] != 10) & (n_df['Longitude'] != 10)].reset_index(drop=True)
ids = set(df.ID)

for id in ids:
    ids_filter = df[df.ID == id]
    #print(ids_filter['Timestamp'].duplicated().any())

e_id = ''
my_index = 0
fig, ax = plt.subplots()
for t in tqdm(df["Timestamp"]):
    events = df[df['Timestamp']==t]
    for index, e in events.iterrows():
        if e['ID'] != e_id:
            my_color=generate_random_hex_color()
            e_id = e['ID']
        x=e['Latitude']
        y=e['Longitude']
        if x!=10 or y!=10:
            ax.scatter(x,y,color=my_color)
            ax.annotate(str(index),xy=(x,y)) 
    plt.savefig("C:/Users/juan.david/projects/garda/data/"+"_"+str(my_index)+'.png')
    my_index+=1 

    