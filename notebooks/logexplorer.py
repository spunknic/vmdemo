import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Function to parse the file content
def parse_file(uploaded_file):
    data = []
    in_hand_records = []
    for line in uploaded_file.readlines():
        line = line.decode('utf-8').strip()
        if line.startswith("tag in hand"):
            scan_id = len(data) - 1  # The decision cycle should correspond to the last processed scan
            tag_in_hand = line.split(": ")[1]
            in_hand_records.append([scan_id, tag_in_hand])
        else:
            parts = line.split(',')
            if len(parts) == 5:
                scan_id, tag, rssi1, rssi2, score = parts
                data.append([int(scan_id), tag, int(rssi1), int(rssi2), float(score)])
    return pd.DataFrame(data, columns=['scan_id', 'tag', 'rssi1', 'rssi2', 'score']), pd.DataFrame(in_hand_records, columns=['scan_id', 'tag_in_hand'])

# Function to get the top 10 tags by average score
def get_top_10_tags(df):
    top_tags = df.groupby('tag')['score'].mean().nlargest(10).index
    return df[df['tag'].isin(top_tags)]

# Function to plot the score evolution
def plot_score_evolution(df):
    fig, ax = plt.subplots()
    
    # Plot each tag's score evolution
    for tag in df['tag'].unique():
        tag_data = df[df['tag'] == tag]
        ax.plot(tag_data['scan_id'], tag_data['score'], label=tag)
    
    # Labels and legend
    ax.set_xlabel('Scan ID')
    ax.set_ylabel('Score')
    ax.set_title('Score Evolution for Top 10 Tags')
    ax.legend(loc='best', bbox_to_anchor=(1.05, 1), title='Tags')
    
    st.pyplot(fig)

# Main Streamlit application
st.title("Bluetooth Scan Log Analyzer")

uploaded_file = st.file_uploader("Upload a log file", type="txt")

if uploaded_file is not None:
    # Parse the uploaded file
    df, in_hand_df = parse_file(uploaded_file)
    
    # Identify top 10 tags
    top_10_df = get_top_10_tags(df)
    
    # Display parsed data and top 10 tags' score evolution
    st.write("Top 10 Tags' Score Evolution")
    plot_score_evolution(top_10_df)
    
    # Display the "in hand" decision records in a separate table
    st.write("Tag in Hand Decisions")
    st.dataframe(in_hand_df)

    # Display the parsed DataFrame for reference
    st.write("Parsed Data:")
    st.dataframe(df)
