
import os
import pandas as pd
import yaml

def load_non_empty_columns(file_path):
    with open(file_path, 'r') as file:
        non_empty_columns = file.read().splitlines()
    return non_empty_columns

def calculate_monthly_aggregates(csv_folder, non_empty_columns):
    # Create an empty list to store DataFrames of monthly aggregates
    monthly_aggregates_list = []

    # Iterate over each CSV file in the folder
    for file_name in os.listdir(csv_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(csv_folder, file_name)

            # Read CSV file
            df = pd.read_csv(file_path)

            # Convert 'DATE' column to datetime
            df['DATE'] = pd.to_datetime(df['DATE'])

            # Extract month and year from the 'DATE' column
            df['MONTH_YEAR'] = df['DATE'].dt.strftime('%m-%Y')

            
            # Calculate monthly aggregates based on non-empty columns
            monthly_aggregates = {}
            if 'MonthlyAverageRH' in non_empty_columns:
                monthly_aggregates['MonthlyAverageRH'] = df.groupby('MONTH_YEAR')['HourlyRelativeHumidity'].mean().astype(str)
            if 'MonthlyDaysWithGT001Precip' in non_empty_columns:
                monthly_aggregates['MonthlyDaysWithGT001Precip'] = df[pd.to_numeric(df['DailyPrecipitation'], errors='coerce',downcast='float') > 1].groupby('MONTH_YEAR').size()
            if 'MonthlyDaysWithGT010Precip' in non_empty_columns:
                monthly_aggregates['MonthlyDaysWithGT010Precip'] = df[pd.to_numeric(df['DailyPrecipitation'], errors='coerce',downcast='float') > 10].groupby('MONTH_YEAR').size()
            if 'MonthlyDaysWithGT32Temp' in non_empty_columns:
                monthly_aggregates['MonthlyDaysWithGT32Temp'] = df[pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce',downcast='float') > 32].groupby('MONTH_YEAR').size()
            if 'MonthlyDaysWithGT90Temp' in non_empty_columns:
                monthly_aggregates['MonthlyDaysWithGT90Temp'] = df[pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce',downcast='float') > 90].groupby('MONTH_YEAR').size()
            if 'MonthlyDaysWithLT0Temp' in non_empty_columns:
                monthly_aggregates['MonthlyDaysWithLT0Temp'] = df[pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce',downcast='float') < 0].groupby('MONTH_YEAR').size()
            if 'MonthlyDaysWithLT32Temp' in non_empty_columns:
                monthly_aggregates['MonthlyDaysWithLT32Temp'] = df[pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce',downcast='float') < 32].groupby('MONTH_YEAR').size()
            if 'MonthlyDewpointTemperature' in non_empty_columns:
              monthly_aggregates['MonthlyDewpointTemperature'] = df.groupby('MONTH_YEAR')['HourlyDewPointTemperature'].mean()
            # if 'MonthlyMeanTemperature' in non_empty_columns:
            #     monthly_aggregates['MonthlyMeanTemperature'] = pd.to_numeric(df.groupby('MONTH_YEAR')['HourlyDryBulbTemperature'], errors='coerce',downcast='float').mean()
            # Add calculations for MonthlyMinSeaLevelPressureValue
            if 'MonthlyMinSeaLevelPressureValue' in non_empty_columns:
                # Convert 'HourlySeaLevelPressure' column to numeric, ignoring errors
                df['HourlySeaLevelPressure'] = pd.to_numeric(df['HourlySeaLevelPressure'], errors='coerce')
                # Calculate minimum value, ignoring NaN values
                monthly_aggregates['MonthlyMinSeaLevelPressureValue'] = df.groupby('MONTH_YEAR')['HourlySeaLevelPressure'].min()

            # Add calculations for MonthlyGreatestPrecip
            if 'MonthlyGreatestPrecip' in non_empty_columns:
                # Convert 'HourlyPrecipitation' column to numeric, ignoring errors
                df['DailyPrecipitation'] = pd.to_numeric(df['DailyPrecipitation'], errors='coerce')
                # Calculate maximum value, ignoring NaN values
                monthly_aggregates['MonthlyGreatestPrecip'] = df.groupby('MONTH_YEAR')['DailyPrecipitation'].max()
            # if 'MonthlyMinimumTemperature' in non_empty_columns:
            #     monthly_aggregates['MonthlyMinimumTemperature'] = pd.to_numeric(df.groupby('MONTH_YEAR')['HourlyDryBulbTemperature'], errors='coerce',downcast='float').min()
            # if 'MonthlySeaLevelPressure' in non_empty_columns:
            #     monthly_aggregates['MonthlySeaLevelPressure'] = pd.to_numeric(df.groupby('MONTH_YEAR')['HourlySeaLevelPressure'], errors='coerce',downcast='float').mean()
            # if 'MonthlyStationPressure' in non_empty_columns:
            #     monthly_aggregates['MonthlyStationPressure'] = pd.to_numeric(df.groupby('MONTH_YEAR')['HourlyStationPressure'], errors='coerce',downcast='float').mean()
            
            # Add more calculations as needed
            
            aggregated_monthly_data = df.groupby(['STATION', 'MONTH_YEAR']).mean(numeric_only=True)
            for column, values in monthly_aggregates.items():
                aggregated_monthly_data[column] = values.astype(str)
            # Append the monthly aggregates DataFrame to the list
            monthly_aggregates_list.append(aggregated_monthly_data)

    # Concatenate all DataFrames in the list along the column axis
    aggregated_monthly_data = pd.concat(monthly_aggregates_list)

    return aggregated_monthly_data



if __name__ == "__main__":
    # Specify the path to the file containing non-empty monthly columns
    file_path = 'data/prepared/non_empty_monthly_columns.txt'

    # Load the array of non-empty monthly columns
    non_empty_columns = load_non_empty_columns(file_path)

    # Print the array of non-empty monthly columns
    print(non_empty_columns)

    # Specify the folder containing CSV files
    year = 2007 #default
    with open('params.yaml', 'r') as params_file:
        params = yaml.safe_load(params_file)
    csv_folder = f"data/csv/{params.get('year',year)}/"

    # Step 1: Calculate monthly aggregates
    monthly_aggregates = calculate_monthly_aggregates(csv_folder,non_empty_columns)

    # Step 2: Save the aggregated monthly data to a CSV file
    output_folder='data/processed'
    os.makedirs(output_folder, exist_ok=True)
    output_file = 'data/processed/aggregated_monthly_data.csv'
    monthly_aggregates.to_csv(output_file)
