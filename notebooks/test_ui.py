import streamlit as st
import pandas as pd
from io import StringIO

uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:

    dataframe = pd.read_csv(uploaded_file)
    
    df = dataframe
    n_df = df.drop(columns=['RSSI_min', 'RSSI_max', 'RSSI_med', 'Flag'])
    n_df = n_df.drop(n_df[(n_df['Longitude'] == 10) & (n_df['Latitude'] == 10)].index)
    n_df['Latitude'] = pd.to_numeric(n_df['Latitude'])
    n_df['Longitude'] = pd.to_numeric(n_df['Longitude'])
    st.write(n_df)