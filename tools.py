import matplotlib.pyplot as plt
import datetime

def plot_time_differences(filename):
    # Open and read the file
    with open(filename, 'r') as file:
        timestamps = file.readlines()
    
    # Convert strings to datetime objects
    timestamps = [datetime.datetime.strptime(ts.strip(), '%d-%m-%Y %H:%M:%S') for ts in timestamps]
    
    # Calculate differences between consecutive timestamps
    time_differences = [int((timestamps[i] - timestamps[i-1]).total_seconds()) for i in range(1, len(timestamps))]
    
    # Plot histogram
    plt.figure(figsize=(10, 6))
    plt.hist(time_differences, bins=50, color='blue', alpha=0.7)
    plt.title('Histogram of Time Differences Between Consecutive Records')
    plt.xlabel('Time difference in seconds')
    plt.ylabel('Frequency')
    plt.show()

# To use the function, call it with the path to your text file
plot_time_differences('C:/Users/juan.david/Documents/checl_log_jam_histogram.txt')
