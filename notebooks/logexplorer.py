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

# Function to plot the score evolution with consistent colors
def plot_score_evolution(df, colors):
    fig, ax = plt.subplots()
    
    # Plot each tag's score evolution with assigned colors
    for tag in df['tag'].unique():
        tag_data = df[df['tag'] == tag]
        ax.plot(tag_data['scan_id'], tag_data['score'], label=tag, color=colors[tag])
    
    # Labels and legend
    ax.set_xlabel('Scan ID')
    ax.set_ylabel('Score')
    ax.set_title('Score Evolution for Top 10 Tags')
    ax.legend(loc='best', bbox_to_anchor=(1.05, 1), title='Tags')
    
    st.pyplot(fig)

# Function to plot the sum of scores in batches of 'x' scans with consistent colors
def plot_batch_sum_scores(df, batch_size, colors):
    df['batch'] = (df['scan_id'] // batch_size) * batch_size  # Group scans into batches
    batch_sum = df.groupby(['batch', 'tag'])['score'].sum().reset_index()

    fig, ax = plt.subplots()

    # Plot each tag's batch score sum evolution with assigned colors
    for tag in batch_sum['tag'].unique():
        tag_data = batch_sum[batch_sum['tag'] == tag]
        ax.plot(tag_data['batch'], tag_data['score'], label=tag, color=colors[tag])
    
    # Labels and legend
    ax.set_xlabel(f'Scan ID (Grouped by {batch_size})')
    ax.set_ylabel('Sum of Scores')
    ax.set_title(f'Sum of Scores in Batches of {batch_size} Scans')
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
    
    # Create a unique color map for the tags
    unique_tags = top_10_df['tag'].unique()
    colors = {tag: plt.cm.get_cmap('tab10')(i) for i, tag in enumerate(unique_tags)}
    
    # Add a number input for batch size
    batch_size = st.number_input("Enter batch size (default 5):", min_value=1, value=5, step=1)

    # Display both plots at the top
    st.write("Score Evolution and Batch Sum Scores")
    plot_score_evolution(top_10_df, colors)
    plot_batch_sum_scores(top_10_df, batch_size, colors)
    
    # Display the "in hand" decision records in a separate table
    st.write("Tag in Hand Decisions")
    st.dataframe(in_hand_df)

    # Display the parsed DataFrame for reference
    st.write("Parsed Data:")
    st.dataframe(df)
