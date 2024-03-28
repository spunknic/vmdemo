import streamlit as st
import pandas as pd
from io import StringIO

uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:

    dataframe = pd.read_csv(uploaded_file)
    df        = dataframe.copy()
    df.columns = ['ID', 'Timestamp', 'Count', 'RSSI_min', 'RSSI_max', 'RSSI_avr', 'RSSI_med', 'Longitude', 'Latitude', 'Flag']
    n_df              = df.drop(columns=['RSSI_min', 'RSSI_max', 'RSSI_med', 'Flag'])
    #format ID
    n_df['ID']          = n_df['ID'].apply(lambda x: x[1:])
    #format count
    n_df['Count'] = pd.to_numeric(n_df['Count'])
    #format RSSI_avr
    n_df['RSSI_avr'] = pd.to_numeric(n_df['RSSI_avr'])
    #format Longitude
    n_df['Longitude'] = n_df['Longitude'].astype(float)
    #format Latitude
    n_df['Latitude'] = n_df['Latitude'].astype(float)
    #format Flag 
    n_df = df.drop(columns=['RSSI_min', 'RSSI_max', 'RSSI_med', 'Flag'])
    #Others
    n_df = n_df.drop(n_df[(n_df['Longitude'] == 10) & (n_df['Latitude'] == 10)].index)
    st.write(n_df)