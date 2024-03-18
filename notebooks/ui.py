import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static

from draft2 import ProcessData
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent 

def plot_gps_coordinates(my_df):
    
    print(len(my_df))
    map_center = [float(my_df[-1][0]),float(my_df[-1][1])]
    m = folium.Map(location=map_center, zoom_start=16)     
    for df in my_df:
        
        lat    = float(df[0])
        lon    = float(df[1])
        count  = df[2]
        id     = df[3]
        state  = df[4]        
        popup_text = f"ID: {id} <br> Count: {count} <br>State:  {state}"
        if state == 'near':
            # Marker color green for 'near' state
            folium.Marker([lat,lon], popup=popup_text, icon=folium.Icon(color='blue')).add_to(m)
        elif state == 'in hand':
            folium.Marker([lat,lon], popup=popup_text, icon=folium.Icon(color='green')).add_to(m)
        elif state == 'truck':
            # Marker color red for other states
            folium.Marker([lat,lon], popup=popup_text, icon=folium.Icon(color='red')).add_to(m)
        else :
            # Marker color red for other states
            folium.Marker([lat,lon], popup=popup_text, icon=folium.Icon(color='gray')).add_to(m)

        # Render the map in Streamlit
    folium_static(m)


def process_file(file, option, min_trust, time_window, inhand_t,count_att,rssi_att):
    pro = ProcessData(input_txt_path= ROOT / Path('data') / Path(file.name))
    push_coords = pro.pipeline(option=option, min_trust=min_trust, inhand_t=inhand_t, n=time_window,count_att=count_att,rssi_att=rssi_att)
        # Display user input)
    
    st.write("File processed successfully!")
    return push_coords

def main():
    st.title("File Processing App")
    
    # File uploader
    file = st.file_uploader("Upload file", type=['csv', 'txt'])
    st.markdown(r"$$Trust = \frac{w_1 \cdot \frac{C}{C_{max}} + w_2 \cdot (\frac{E}{E_{max}})}{w_1 + w_2}$$", unsafe_allow_html=True)
    count_att = st.slider("Count Attention (w1)", min_value=1, max_value=10, value=5)
    rssi_att  = st.slider("RSSI Attention  (w2)", min_value=1, max_value=10, value=1)
    min_trust = st.slider("Minimum Trust", min_value=0.001, max_value=1.0, value=0.5)
    
    inhand_t = st.number_input("InHand Threshold", value=3)
    time_window = st.number_input("Time Window", value=5)
    option = st.radio("Option", options=['all', 'filter'])
    # Button to process the file
    if st.button("Process File") and file:
        # Widgets for user input
        push_coords = process_file(file, option, min_trust, time_window, inhand_t,count_att,rssi_att)
        plot_gps_coordinates(push_coords)
        st.write("Console:", push_coords)

if __name__ == "__main__":
    main()
