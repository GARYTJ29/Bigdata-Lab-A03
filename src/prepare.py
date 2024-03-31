import os
import pandas as pd
import yaml

def extract_monthly_aggregates(csv_folder):
    # Create an empty DataFrame to store aggregated data
    aggregated_data = pd.DataFrame()

    # Iterate over each CSV file in the folder
    for file_name in os.listdir(csv_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(csv_folder, file_name)

            # Read CSV file
            df = pd.read_csv(file_path)

            # Extract month and year from the DATE column
            df['DATE'] = pd.to_datetime(df['DATE'])
            df['MONTH_YEAR'] = df['DATE'].dt.strftime('%m-%Y')

            # Group by station and month_year, compute mean of monthly columns
            monthly_aggregates = df.groupby(['STATION', 'MONTH_YEAR']).mean(numeric_only=True)

            # Append to aggregated data
            aggregated_data = pd.concat([aggregated_data,monthly_aggregates])

    return aggregated_data

def check_monthly_columns(data):
    # Get the monthly column names
    monthly_columns = [col for col in data.columns if 'Monthly' in col]

    # Check if any of the monthly columns contain non-empty values (including zero)
    non_empty_columns = [col for col in monthly_columns if data[col].notna().any()]

    return non_empty_columns

if __name__ == "__main__":
    # Set default values for n_locs and year
    year = 2007

    # Specify the folder containing CSV files
    with open('params.yaml', 'r') as params_file:
        params = yaml.safe_load(params_file)
    csv_folder = f"data/csv/{params.get('year',year)}/"

    # Step 1: Extract monthly aggregates
    aggregated_data = extract_monthly_aggregates(csv_folder)

    # Step 2: Save the aggregated data to a CSV file
    output_file = 'data/prepared/ground_truth.csv'
    output_folder='data/prepared'
    os.makedirs(output_folder, exist_ok=True)
    aggregated_data.to_csv(output_file)

    # Step 3: Check for non-empty monthly columns
    non_empty_columns = check_monthly_columns(aggregated_data)

    # Step 4: Save the array of non-empty monthly column names to a file
    with open('data/prepared/non_empty_monthly_columns.txt', 'w') as file:
        file.write('\n'.join(non_empty_columns))
