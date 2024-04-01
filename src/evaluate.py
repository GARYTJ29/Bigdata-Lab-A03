import json
from dvclive import Live
import pandas as pd
from sklearn.metrics import r2_score
import os

# Load processed and ground truth CSV files
processed_data = pd.read_csv('data/processed/aggregated_monthly_data.csv')
ground_truth_data = pd.read_csv('data/prepared/ground_truth.csv')

# Extract columns containing "Monthly" from both DataFrames
processed_columns = [col for col in processed_data.columns if 'Monthly' in col]
ground_truth_columns = [col for col in ground_truth_data.columns if 'Monthly' in col]

# Initialize a dictionary to store R2 scores
r2_scores = {}

# Initialize dvclive
with Live() as live:
    # Calculate R2 score for each column and log metrics
    for col in processed_columns:
        if col in ground_truth_columns:
            processed_values = processed_data[col]
            ground_truth_values = ground_truth_data[col]
            # Filter out NaN values
            notnan_values = ~processed_values.isnull() & ~ground_truth_values.isnull()
            if notnan_values.any():
                r2 = r2_score(ground_truth_values[notnan_values], processed_values[notnan_values])
                live.log_metric(col, r2)
                # Store R2 score in the dictionary
                r2_scores[col] = r2

# Save R2 scores to a JSON file
output_folder='data/eval'
os.makedirs(output_folder, exist_ok=True)
output_file = 'data/eval/r2_scores.json'
with open(output_file, 'w') as f:
    json.dump(r2_scores, f)

