import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV data into a DataFrame
df = pd.read_csv("requestsPerSecond.csv")

# Convert timestamp to datetime
df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')

# Calculate relative time
start_time = df['Timestamp'].min()
df['RelativeTime'] = (df['Timestamp'] - start_time).dt.total_seconds()

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(df['RelativeTime'], df['Requests'], marker='o', linestyle='-')
plt.title('Requests per Second')
plt.xlabel('Relative Time (seconds)')
plt.ylabel('Number of Requests')
plt.tight_layout()
plt.show()
