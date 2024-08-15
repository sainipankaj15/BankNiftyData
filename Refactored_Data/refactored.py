import pandas as pd
import os 

def process_day_data(date, group):
    """
    Processes data for a specific day.
    
    Filters the data to include only times between '09:15' and '15:29',
    reindexes the DataFrame to include every minute within this range,
    fills missing values, and formats the data before returning it.
    
    Args:
    date (datetime.date): The date for which the data is processed.
    group (pd.DataFrame): DataFrame containing data for the given day.
    
    Returns:
    pd.DataFrame: Processed DataFrame for the specific day.
    """
    # Filter data to include only the times between '09:15' and '15:29'
    df_filtered = group.between_time('09:15', '15:29')
    
    # Create a full range of datetime indices for the given day, every minute from '09:15' to '15:29'
    full_range = pd.date_range(start=f'{date} 09:15', end=f'{date} 15:29', freq='1min')
    
    # Reindex the filtered DataFrame to include every minute in the full range
    df_reindexed = df_filtered.reindex(full_range)
    
    # Backfill missing values and forward fill remaining missing values
    df_filled = df_reindexed.bfill().ffill()
    
    # Add columns for 'Date' and 'Time'
    df_filled['Date'] = date.strftime('%Y%m%d')
    df_filled['Time'] = [t.strftime('%H:%M') for t in df_filled.index.time]
    
    # Format numerical columns to two decimal places
    df_filled['Open'] = df_filled['Open'].map('{:.2f}'.format)
    df_filled['High'] = df_filled['High'].map('{:.2f}'.format)
    df_filled['Low'] = df_filled['Low'].map('{:.2f}'.format)
    df_filled['Close'] = df_filled['Close'].map('{:.2f}'.format)
    
    # Rearrange columns and return the processed DataFrame
    df_filled = df_filled[['Instrument', 'Date', 'Time', 'Open', 'High', 'Low', 'Close']]
    return df_filled

def clean_and_save_data(input_path, output_path):
    """
    Cleans and saves the data from the input CSV to the output CSV.
    
    Reads the raw data, processes it to ensure there are no duplicate time indices,
    and applies daily processing to each day of data before saving the cleaned DataFrame.
    
    Args:
    input_path (str): Path to the input CSV file.
    output_path (str): Path to the output CSV file.
    """
    # Read the raw CSV data
    df = pd.read_csv(input_path)
    
    # Combine 'Date' and 'Time' columns into a single datetime index and set it as the index
    df['datetime'] = pd.to_datetime(df['Date'].astype(str) + df['Time'], format='%Y%m%d%H:%M')
    df.set_index('datetime', inplace=True)
    
    # Remove duplicate indices, keeping the first occurrence
    df = df[~df.index.duplicated(keep='first')]
    
    # Initialize an empty DataFrame to collect cleaned data
    df_cleaned = pd.DataFrame()
    
    # Process each day of data separately
    for date, group in df.groupby(df.index.date):
        df_cleaned = pd.concat([df_cleaned, process_day_data(date, group)])
    
    # Save the cleaned DataFrame to the output CSV file
    df_cleaned.to_csv(output_path, index=False)

if __name__ == "__main__":
    input_path = r"D:\Coding\Python\backend\BankNiftyData-main\BANK_NIFTY_data\BNF_2010.csv"
    output_path = r"D:\Coding\Python\backend\BankNiftyData-main\refactored-data\BNF-Refactored\bnf_2010_cleaned.csv"
    output_folder = r"refactored-data/BNF-Refactored"
    
    # Create output directory if it does not exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Clean and save the data
    clean_and_save_data(input_path, output_path)
