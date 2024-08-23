import pandas as pd
from datetime import datetime
import os 

def timeToIndex(time_input, start_time='09:15'):
    time_format = '%H:%M'
    try:
        input_time = datetime.strptime(time_input, time_format)
        start_time = datetime.strptime(start_time, time_format)
    except ValueError:
        raise ValueError(f"Invalid time format. Please use '{time_format}'.")
    time_difference = (input_time - start_time).total_seconds() / 60
    index = int(time_difference)
    if index < 0 or index >= 375:
        raise ValueError("Out of range time")
    return index

def calDayHigh(df, column_name, end_index):
    dayHigh = float('-inf')
    for i in range(end_index + 1):
        currHigh = df.loc[i, column_name]
        if currHigh > dayHigh:
            dayHigh = currHigh
    return dayHigh

def calDayLow(df, column_name, end_index):
    dayLow = float('inf')
    for i in range(end_index + 1):
        currLow = df.loc[i, column_name]
        if currLow < dayLow:
            dayLow = currLow
    return dayLow

def calHighBreak(df, column_name, start_index, dayHigh):
    for i in range(start_index+1,375):
        currHigh = df.loc[i,column_name]
        if currHigh > dayHigh:
            return True
    return False
            
def calLowBreak(df,column_name, start_index, dayLow):
    for i in range(start_index+1,375):
        currLow = df.loc[i,column_name]
        if currLow < dayLow:
            return True
    return False

def comesBackToDayHigh(df, column_name, start_index, dayHigh, isHighBreak):
    if isHighBreak:
        for i in range(start_index+1,375):
            currLow = df.loc[i,column_name]
            if currLow <= dayHigh:
                return True
    return False

def comesBackToDayLow(df, column_name, start_index, dayLow, isLowBreak):
    if isLowBreak:
        for i in range(start_index+1, 375):
            currHigh = df.loc[i,column_name]
            if currHigh >= dayLow:
                return True
    return False
    
def pointsToHighTarget(df, column_name, start_index, dayHigh, isDayHighBreak): #column_name = high
    target = dayHigh
    if isDayHighBreak:
        for i in range(start_index+1, 375):
            currHigh = df.loc[i,column_name]
            if target <= currHigh:
                target = currHigh
    return target

def pointsToHighSL(df, column_name, start_index, dayHigh, isDayHighBreak): #column_name = low
    stop_loss = dayHigh
    if isDayHighBreak:
        for i in range(start_index+1, 375):
            currLow = df.loc[i,column_name]
            if stop_loss >= currLow:
                stop_loss = currLow
    return stop_loss

def pointsToLowTarget(df, column_name, start_index, dayLow, isDayLowBreak): #column_name = low
    target = dayLow
    if isDayLowBreak:
        for i in range(start_index+1, 375):
            currLow = df.loc[i,column_name]
            if target >= currLow:
                target = currLow
    return target

def pointsToLowSL(df, column_name, start_index, dayLow, isDayLowBreak): #column_name = high
    stop_loss = dayLow
    if isDayLowBreak:
        for i in range(start_index+1, 375):
            currHigh = df.loc[i,column_name]
            if currHigh >= stop_loss:
                stop_loss = currHigh
    return stop_loss
         
def df_filter(df, columns):
    for column in columns:
        df[column]=df[column].map('{:.2f}'.format)
    return df

def process_csv(csv_file_path, output_csv_path, index):
    chunk_size = 375
    column_names = ['Instrument', 'Date', 'Time', 'Open', 'High', 'Low', 'Close']
    day_data_chunk = pd.read_csv(csv_file_path, skiprows=1, chunksize=chunk_size, header=None)
    new_df = pd.DataFrame(columns=['Date', 'Time', 'DayHigh', 'DayLow', 'IsDayHighBreak', 'IsDayLowBreak','ComesBackToDayHigh', 'ComesBackToDayLow', 'TargetHigh', 'points_from_target_high', 'SLHigh', 'points_from_SL_High', 'TargetLow', 'points_from_Target_Low', 'SLLow', 'points_from_SL_low', 'Closing', 'ClosingPointDiffForHigh', 'ClosingPointDiffForLow'])

    for day in day_data_chunk:
        if not day.empty:
            isDayHighBreak = False
            isDayLowBreak = False
            isComesBackToDayHigh = False
            isComesBackToDayLow = False
            target_high = 0
            SL_low = 0
            target_low = 0
            SL_low = 0
            
            day.columns = column_names
            day.reset_index(drop=True, inplace=True)
            date = day['Date'].iloc[0].astype(float).astype(int).astype(str)
            closing = day['Close'].iloc[373].astype(float)
            
            dayHigh = calDayHigh(day, 'High', index)
            isDayHighBreak = calHighBreak(day,'High',index,dayHigh)
            
            dayLow = calDayLow(day, 'Low', index)
            isDayLowBreak = calLowBreak(day,'Low',index, dayLow)
            
            isComesBackToDayHigh = comesBackToDayHigh(day,'Low',index,dayHigh,isDayHighBreak)
            isComesBackToDayLow = comesBackToDayHigh(day,'High',index,dayLow,isDayLowBreak)
            
            target_high = pointsToHighTarget(day,'High', index, dayHigh, isDayHighBreak)
            SL_High = pointsToHighSL(day,'Low', index, dayHigh, isDayHighBreak)
            target_low = pointsToLowTarget(day,'Low', index, dayLow, isDayLowBreak)
            SL_low = pointsToLowSL(day,'High', index, dayLow, isDayLowBreak)
            
            new_df.loc[len(new_df)] = [date, time_input, dayHigh, dayLow, isDayHighBreak, isDayLowBreak,isComesBackToDayHigh, isComesBackToDayLow, target_high, target_high-dayHigh, SL_High, SL_High-dayHigh, target_low, dayLow-target_low, SL_low, dayLow-SL_low, closing, closing-dayHigh, dayLow-closing]

    columns_to_format = [
        'DayHigh', 'DayLow', 'TargetHigh', 'points_from_target_high', 'SLHigh', 'points_from_SL_High', 'TargetLow', 'points_from_Target_Low', 'SLLow', 'points_from_SL_low', 'Closing', 'ClosingPointDiffForHigh', 'ClosingPointDiffForLow'
    ]
    new_df = df_filter(new_df, columns_to_format)
    new_df.to_csv(output_csv_path, index=False)

def traverse_every_file(source_dir, target_dir, index):
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.csv'):
                new_filename = file.replace('cleaned_', 'backtest_')
                relative_path = os.path.relpath(root,source_dir)
                new_relative_path = relative_path.replace('-cleaned', '-backtest')
                target_subdir = os.path.join(target_dir, new_relative_path)
                if not os.path.exists(target_subdir):
                    os.makedirs(target_subdir)
                source_csv_path = os.path.join(root, file)
                target_csv_path = os.path.join(target_subdir, new_filename)
                print(f"Processing file: {source_csv_path}")
                process_csv(source_csv_path, target_csv_path, index)
                print(f"Saved processed file to: {target_csv_path}")

if __name__ == "__main__":
    time_input = input("Enter time (HH:MM): ")
    index = timeToIndex(time_input)
    source_dir = 'Refactored-Data'
    target_dir = 'Backtesting'
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    traverse_every_file(source_dir,target_dir,index)