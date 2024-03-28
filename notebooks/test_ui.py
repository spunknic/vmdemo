import streamlit as st
import pandas as pd
from io import StringIO
from logicbug import ProcessData

uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:

    dataframe = pd.read_csv(uploaded_file)
    df        = dataframe.copy()
    prodat = ProcessData(df)
    st.write(prodat.first())
    
    
    
