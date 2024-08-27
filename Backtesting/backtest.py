import pandas as pd
from datetime import datetime,timedelta
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
            if currLow < dayHigh:
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
    return target-dayHigh

def pointsToHighSL(df, column_name, start_index, dayHigh, isDayHighBreak): #column_name = low
    stop_loss = dayHigh
    if isDayHighBreak:
        for i in range(start_index+1, 375):
            currLow = df.loc[i,column_name]
            if stop_loss >= currLow:
                stop_loss = currLow
    return stop_loss-dayHigh

def pointsToLowTarget(df, column_name, start_index, dayLow, isDayLowBreak): #column_name = low
    target = dayLow
    if isDayLowBreak:
        for i in range(start_index+1, 375):
            currLow = df.loc[i,column_name]
            if target >= currLow:
                target = currLow
    return dayLow-target

def pointsToLowSL(df, column_name, start_index, dayLow, isDayLowBreak): #column_name = high
    stop_loss = dayLow
    if isDayLowBreak:
        for i in range(start_index+1, 375):
            currHigh = df.loc[i,column_name]
            if currHigh >= stop_loss:
                stop_loss = currHigh
    return dayLow-stop_loss
         
def df_filter(df, columns):
    for column in columns:
        df[column]=df[column].map('{:.2f}'.format)
    return df

def process_csv(csv_file_path, output_csv_path, index, time_input):
    chunk_size = 375
    column_names = ['Instrument', 'Date', 'Time', 'Open', 'High', 'Low', 'Close']
    day_data_chunk = pd.read_csv(csv_file_path, skiprows=1, chunksize=chunk_size, header=None)
    new_df = pd.DataFrame(columns=['Date', 'Time', 'DayHigh', 'DayLow', 'IsDayHighBreak', 'IsDayLowBreak','ComesBackToDayHigh', 'ComesBackToDayLow', 'TargetHigh', 'points_from_target_high', 'percentChangeTargetHigh','SLHigh', 'points_from_SL_High', 'percentageChangeSLHigh' ,'TargetLow', 'points_from_Target_Low', 'percentageChangeTargetLow','SLLow', 'points_from_SL_low','percentageChangeSLLow', 'Closing', 'ClosingPointDiffForHigh', 'ClosingPointDiffForLow', 'noBreak'])

    for day in day_data_chunk:
        if not day.empty:
            isDayHighBreak = False
            isDayLowBreak = False
            isComesBackToDayHigh = False
            isComesBackToDayLow = False
            
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
            
            diff_from_target_high = pointsToHighTarget(day,'High', index, dayHigh, isDayHighBreak)
            diff_from_sl_high = pointsToHighSL(day,'Low', index, dayHigh, isDayHighBreak)
            diff_from_target_low = pointsToLowTarget(day,'Low', index, dayLow, isDayLowBreak)
            diff_from_sl_low = pointsToLowSL(day,'High', index, dayLow, isDayLowBreak)
            
            target_high = 0
            sl_high = 0
            target_low = 0
            sl_low = 0
            closing_for_high = 0
            closing_for_low = 0
            if isDayHighBreak:
                target_high = dayHigh+diff_from_target_high
                sl_high = dayHigh+diff_from_sl_high
                closing_for_high = closing-dayHigh
            if isDayLowBreak:
                target_low = dayLow-diff_from_target_low
                sl_low = dayLow-diff_from_sl_low
                closing_for_low = dayLow-closing
            
            target_high_percent = round((diff_from_target_high / dayHigh) * 100, 2)
            sl_high_percent = round((diff_from_sl_high / dayHigh) * 100, 2)
            target_low_percent = round((diff_from_target_low / dayLow) * 100, 2)
            sl_low_percent = round((diff_from_sl_low / dayLow) * 100, 2)
            
            new_df.loc[len(new_df)] = [date, time_input, dayHigh, dayLow, isDayHighBreak, isDayLowBreak,isComesBackToDayHigh, isComesBackToDayLow, target_high,diff_from_target_high, target_high_percent, sl_high, diff_from_sl_high, sl_high_percent ,target_low, diff_from_target_low, target_low_percent, sl_low, diff_from_sl_low, sl_low_percent, closing, closing_for_high, closing_for_low, not (isDayHighBreak or isDayLowBreak)]

    columns_to_format = [
        'DayHigh', 'DayLow', 'TargetHigh', 'points_from_target_high', 'SLHigh', 'points_from_SL_High', 'TargetLow', 'points_from_Target_Low', 'SLLow', 'points_from_SL_low', 'Closing', 'ClosingPointDiffForHigh', 'ClosingPointDiffForLow'
    ]
    new_df = df_filter(new_df, columns_to_format)
    new_df.to_csv(output_csv_path, index=False)

def traverse_every_file(source_dir, destination_folder, index, time_input):
    for root, dirs, files in os.walk(source_dir):
        for file in files: 
            #files = [BANK_NIFTY_data-cleaned,NIFTY_data-cleaned]
            #file = file in files[0] and files[1] and so on..
            if file.endswith('.csv'): 
                new_filename = file.replace('cleaned_', 'backtest_')
                relative_path = os.path.relpath(root,source_dir) #root = Refactored-Data
                new_relative_path = relative_path.replace('-cleaned', '-backtest')
                target_subdir = os.path.join(destination_folder, new_relative_path)
                os.makedirs(target_subdir,exist_ok=True)
                source_csv_path = os.path.join(root, file)
                target_csv_path = os.path.join(target_subdir, new_filename)
                print(f"Processing file: {source_csv_path}")
                process_csv(source_csv_path, target_csv_path, index, time_input)
                print(f"Saved processed file to: {target_csv_path}\n")

def create_time_folders(destination_dir,source_dir): 
    # This function is used to create subfolders by name 09_30_backtest, 09_45_backtest
    # The subfolders in each of the folders will be handeled by traverse_every_file() function
    start_time = "09:30"
    end_time = "15:00"
    destination_dir += "\CSV"
    interval_minutes = 15
    start = datetime.strptime(start_time, "%H:%M")
    end = datetime.strptime(end_time, "%H:%M")
    
    # Create directories at destination_dir
    current_time = start
    while current_time <= end:
        current_time_str = current_time.strftime("%H:%M")
        index = timeToIndex(current_time_str)
        folder_name = current_time.strftime("%H_%M") + "_backtest"
        folder_path = os.path.join(destination_dir, folder_name)
        os.makedirs(folder_path, exist_ok=True)
        print(f"{folder_path} \n{source_dir} \n{index}")
        traverse_every_file(source_dir,folder_path,index,current_time_str)
        current_time += timedelta(minutes=interval_minutes)

if __name__ == "__main__":
    time_input = input("Enter time (HH:MM): ")
    index = timeToIndex(time_input)
    source_dir = 'Refactored-Data'
    destination_folder = 'Backtesting'
    os.makedirs(destination_folder, exist_ok=True)
    traverse_every_file(source_dir,destination_folder,index,time_input)
    # create_time_folders(destination_folder,source_dir)