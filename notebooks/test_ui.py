import streamlit as st
import pandas as pd
from logicbug import ProcessData


import streamlit as st
import folium
from streamlit_folium import folium_static
import random 
import math


if 'selected_tag' not in st.session_state:
    st.session_state['selected_tag'] = None

if 'previous_selection' not in st.session_state:
  st.session_state['previous_selection'] = None

tolerance = 0.00001

def has_same_location(row, markers):
  """
  Checks if at least one element in markers[row['ID']] has the same longitude and latitude as row['Longitude'] and row['Latitude'].

  Args:
      row (dict): A row from the DataFrame.
      markers (dict): A dictionary where keys are IDs and values are lists of marker information (likely containing 'Longitude' and 'Latitude' keys).

  Returns:
      bool: True if at least one element in markers[row['ID']] has the same location, False otherwise.
  """

  # Check if the ID exists in the markers dictionary
  if row['ID'] in markers:
    # Loop through markers for this ID
    for marker in markers[row['ID']]:
      # Check if longitude and latitude match
      if marker['Longitude'] == row['Longitude'] and marker['Latitude'] == row['Latitude']:
        return True  # Match found, return True
  return False  # No match found, return False

def format_marker(row,markers):
    colors = [
    'red',
    'blue',
    'gray',
    'darkred',
    'lightred',
    'orange',
    'beige',
    'green',
    'darkgreen',
    'lightgreen',
    'darkblue',
    'lightblue',
    'purple',
    'darkpurple',
    'pink',
    'cadetblue',
    'lightgray',
    'black'
            ]
    #color format
    if row['ID'] not in markers.keys():
        my_color          = random.choice(colors)
        markers[row['ID']]= {'color':my_color,'Longitude':row['Longitude'],'Latitude':row['Latitude']}
        row['Color']      = my_color
    else:
        row['Color']      = markers[row['ID']]['color']
    
    #if has_same_location(row,markers):
     
    if len(markers)>1:
        for marker in markers:
            check1 = math.isclose(row['Longitude'], markers[marker]['Longitude'], abs_tol=tolerance)
            check2 = math.isclose(row['Latitude'], markers[marker]['Latitude'], abs_tol=tolerance)
            #if (row['Longitude'] == markers[marker]['Longitude']) and (row['Latitude'] == markers[marker]['Latitude']): 
            if check1 and check2:
                row['Longitude'] = row['Longitude']+0.00005 
                row['Latitude']  = row['Latitude'] +0.00005
                row['note']      = 'Point modified to avoid overlapping'
    
    return row,markers

def plot_gps_coordinates(my_df):
    
    map_center = [my_df['Latitude'].iloc[-1],my_df['Longitude'].iloc[-1]]    
    m          = folium.Map(location=map_center, zoom_start=16)
    markers    = dict()
    to_animate = list()
    console    = list()
    for index, row in my_df.iterrows():
        row['note']    = ''
        my_row,markers = format_marker(row,markers)
        count          = my_row['Count']
        id             = my_row['ID'] 
        rssi           = my_row['RSSI_avr'] 
        color          = my_row['Color']
        note           = my_row['note']
        popup_text     = f"ID: {id} <br> Count: {count} <br> RSSI: {rssi} <br> note: {note}"
        console.append(popup_text)
        folium.Marker([my_row['Latitude'],my_row['Longitude']], popup=popup_text, icon=folium.Icon(color=color,icon_size=(20,20))).add_to(m)
        
        #m.save(str(index)+"_.html")
        #to_animate.append(str(index)+"_.html")

    folium_static(m)
    st.write(str([r for r in console]))
    #m.save("random_locations_map.png")
    #m.save("random_locations_map.html  ")

def adapt_interface(option):

    if option =='Last Position Count':
        st.session_state['count_th'] = st.slider("Count threshold", min_value=1, max_value=50, value=25)
    elif option =='Last Position RSSI':
        st.session_state['rssi_th'] = st.slider("RSSI_avr threshold", min_value=-100, max_value=0, value=-60)
    elif option == 'Last Position Count and RSSI':
        st.session_state['count_th'] = st.slider("Count threshold", min_value=1, max_value=50, value=25)
        st.session_state['rssi_th'] = st.slider("RSSI_avr threshold", min_value=-100, max_value=0, value=-60)
    elif option == 'Gil Algo': 
        st.session_state['RSSI limit']    = st.slider("RSSI limit", min_value=1, max_value=100, value=70)
        st.session_state['Clamp Size']    = st.slider("Clamp Size", min_value=1, max_value=100, value=20)
        st.session_state['Count Limit']   = st.slider("Count Limit", min_value=1, max_value=50, value=50)
        st.session_state['Ratio theshold']= st.slider("Ratio theshold", min_value=1, max_value=50, value=30)
    elif option == 'Gil Algo + Time Window': 
        st.session_state['RSSI limit']    = st.slider("RSSI limit", min_value=1, max_value=100, value=70)
        st.session_state['Clamp Size']    = st.slider("Clamp Size", min_value=1, max_value=100, value=20)
        st.session_state['Count Limit']   = st.slider("Count Limit", min_value=1, max_value=50, value=50)
        st.session_state['Ratio theshold']= st.slider("Ratio theshold", min_value=1, max_value=50, value=30)
        st.session_state['In hand Th']= st.slider("In hand theshold", min_value=1, max_value=5, value=2)

def main():
    st.title("File Processing App")
    # File uploader
    file   = st.file_uploader("Upload file", type=['txt'])
    if file is not None:
        dataframe = pd.read_csv(file)
        df        = dataframe.copy()
        option = st.radio("Option", options=['Raw data', 'Last Position','Last Position Count','Last Position RSSI','Last Position Count and RSSI','Gil Algo','Gil Algo + Time Window'])
        adapt_interface(option)

        st.session_state['selected_tag'] = None

        if file:
            st.session_state['file'] = file
            #Unitary Option
            prodata      = ProcessData(df)
            checkdf=prodata.checkdf()
            st.write(checkdf['Longitude'])
            print(type(checkdf['Longitude'].iloc[0]))
            print(checkdf['Longitude'].iloc[0])
            
            my_ids       = prodata.one_tag_list()
            my_ids.append('All') 
            selected_tag = st.selectbox('Select tag from list', (my_ids))

            if st.button("Go"):
                st.text(selected_tag)
                st.text(option)
                if option =='Raw data':
                    if selected_tag != st.session_state['previous_selection']:

                        if selected_tag != 'All':
                            plot_gps_coordinates(prodata.one_tag_raw(selected_tag))
                        else:
                            plot_gps_coordinates(prodata.one_tag_raw(''))

                elif option =='Last Position':
                    if selected_tag != st.session_state['previous_selection']:

                        if selected_tag != 'All':
                            plot_gps_coordinates(prodata.one_tag_last(selected_tag))
                        else:
                            plot_gps_coordinates(prodata.one_tag_last(''))

                elif option == 'Last Position RSSI':
                    
                    if selected_tag != st.session_state['previous_selection']:

                        if selected_tag != 'All':
                            plot_gps_coordinates(prodata.one_tag_last_rssi(selected_tag,st.session_state['rssi_th']))
                        else:
                            plot_gps_coordinates(prodata.one_tag_last_rssi('',st.session_state['rssi_th']))  

                elif option == 'Last Position Count':
                    
                    if selected_tag != st.session_state['previous_selection']:

                        if selected_tag != 'All':
                            plot_gps_coordinates(prodata.one_tag_last_count(selected_tag,st.session_state['count_th']))
                        else:
                            plot_gps_coordinates(prodata.one_tag_last_count('',st.session_state['count_th'])) 

                elif option =='Last Position Count and RSSI':
                    if selected_tag != st.session_state['previous_selection']:

                        if selected_tag != 'All':
                            plot_gps_coordinates(prodata.one_tag_last_count_rssi(selected_tag,st.session_state['rssi_th'],st.session_state['count_th']))
                        else:
                            plot_gps_coordinates(prodata.one_tag_last_count_rssi('',st.session_state['rssi_th'],st.session_state['count_th'])) 

                elif option == 'Gil Algo':
                    plot_gps_coordinates(prodata.gil_algo(st.session_state['RSSI limit'],st.session_state['Clamp Size'],st.session_state['Count Limit'],st.session_state['Ratio theshold']))
                
                elif option == 'Gil Algo + Time Window':
                    plot_gps_coordinates(prodata.gil_algo_time_window(st.session_state['RSSI limit'],st.session_state['Clamp Size'],st.session_state['Count Limit'],st.session_state['Ratio theshold'],st.session_state['In hand Th']))
                        
        else:
            pass

if __name__ == "__main__":
    main()

    
    
    
