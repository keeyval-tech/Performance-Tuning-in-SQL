import pandas as pd

# Make sure to install pandas if you haven't already
# You can install it using PyCharm's built-in package manager, or by running `pip install pandas` in your terminal

# Path to the CSV file
file_path = '/Users/keval/Desktop/BU/Spring 24/ADBMS/Project/Modified_Motor_Vehicle_Collisions_-_Crashes.csv'

# Load the CSV file into a DataFrame
data = pd.read_csv(file_path)

# Display the first few rows of the DataFrame
print(data.head())

# Display the original column names
print("Original column names:")
print(data.columns)

# Define the new column names
new_column_names = {
    'CRASH DATE': 'CRASH_DATE',
    'CRASH TIME': 'CRASH_TIME',
    'BOROUGH': 'BOROUGH',
    'ZIP CODE': 'ZIP_CODE',
    'LATITUDE': 'LATITUDE',
    'LONGITUDE': 'LONGITUDE',
    'LOCATION': 'LOC',
    'ON STREET NAME': 'ON_STREET_NAME',
    'CROSS STREET NAME': 'CROSS_STREET_NAME',
    'OFF STREET NAME': 'OFF_STREET_NAME',
    'NUMBER OF PERSONS INJURED': 'NUMBER_OF_PERSONS_INJURED',
    'NUMBER OF PERSONS KILLED': 'NUMBER_OF_PERSONS_KILLED',
    'NUMBER OF PEDESTRIANS INJURED': 'NUMBER_OF_PEDESTRIANS_INJURED',
    'NUMBER OF PEDESTRIANS KILLED': 'NUMBER_OF_PEDESTRIANS_KILLED',
    'NUMBER OF CYCLIST INJURED': 'NUMBER_OF_CYCLIST_INJURED',
    'NUMBER OF CYCLIST KILLED': 'NUMBER_OF_CYCLIST_KILLED',
    'NUMBER OF MOTORIST INJURED': 'NUMBER_OF_MOTORIST_INJURED',
    'NUMBER OF MOTORIST KILLED': 'NUMBER_OF_MOTORIST_KILLED',
    'CONTRIBUTING FACTOR VEHICLE 1': 'CONTRIBUTING_FACTOR_VEHICLE_1',
    'CONTRIBUTING FACTOR VEHICLE 2': 'CONTRIBUTING_FACTOR_VEHICLE_2',
    'CONTRIBUTING FACTOR VEHICLE 3': 'CONTRIBUTING_FACTOR_VEHICLE_3',
    'CONTRIBUTING FACTOR VEHICLE 4': 'CONTRIBUTING_FACTOR_VEHICLE_4',
    'CONTRIBUTING FACTOR VEHICLE 5': 'CONTRIBUTING_FACTOR_VEHICLE_5',
    'COLLISION_ID': 'COLLISION_ID',
    'VEHICLE TYPE CODE 1': 'VEHICLE_TYPE_CODE_1',
    'VEHICLE TYPE CODE 2': 'VEHICLE_TYPE_CODE_2',
    'VEHICLE TYPE CODE 3': 'VEHICLE_TYPE_CODE_3',
    'VEHICLE TYPE CODE 4': 'VEHICLE_TYPE_CODE_4',
    'VEHICLE TYPE CODE 5': 'VEHICLE_TYPE_CODE_5',
}


# Rename the columns using the defined mappings
data = data.rename(columns=new_column_names)

# Display the new column names
print("\nNew column names:")
print(data.columns)

# Replace all NaN/NULL values in numeric columns with 0 and handle empty strings in numeric columns
numeric_cols = data.select_dtypes(include=['number'])  # Selects columns with numeric data type
for col in numeric_cols.columns:
    data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

# Replace 'AB' with 0 in 'ZIP Code' column, assuming it's a numeric column
data['ZIP_CODE'] = data['ZIP_CODE'].replace('AB', 0).fillna(0)

# Display the first few rows of the DataFrame to verify changes
print(data.head())

# Handle NULL and empty strings in text columns
text_cols = data.select_dtypes(include=['object'])  # Selects columns with text data type
data[text_cols.columns] = text_cols.fillna('AB').replace('', 'AB')

# Replace multiple spaces with a single space and strip leading/trailing spaces in 'OFF STREET NAME'
data['OFF_STREET_NAME'] = data['OFF_STREET_NAME'].str.replace(r'\s+', ' ', regex=True).str.strip()

# Display the cleaned-up 'OFF STREET NAME' column to verify changes
print(data['OFF_STREET_NAME'].head())

# Strip whitespace and replace 'AB' with '(0,0)' in the 'Location' column
data['LOC'] = data['LOC'].str.strip().replace('AB', '(0,0)')

# Display the 'Location' column to verify the changes
print(data['LOC'].head())

columns_to_drop = ['VEHICLE_TYPE_CODE_4', 'VEHICLE_TYPE_CODE_5', 'CONTRIBUTING_FACTOR_VEHICLE_4', 'CONTRIBUTING_FACTOR_VEHICLE_5']

# Drop the specified columns
data = data.drop(columns=columns_to_drop, axis=1)


# Specify the path where you want to save the file
output_path = 'Final_Motor_Vehicle_Collisions_-_Crashes.csv'

# Save the DataFrame to a CSV file
data.to_csv(output_path, index=False)  # Set index=False if you don't want to save the DataFrame index as a separate column

print(f"File has been saved to {output_path}")

print(data.dtypes)