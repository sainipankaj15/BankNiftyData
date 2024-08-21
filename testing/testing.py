import pandas as pd
from datetime import datetime

def timeToIndex(time_input, start_time='09:15'):
    time_format = '%H:%M'
    try:
        input_time = datetime.strptime(time_input, time_format)
        start_time = datetime.strptime(start_time, time_format)
        print(f"Input Time: {input_time}, Start Time: {start_time}")
    except ValueError:
        raise ValueError(f"Invalid time format. Please use '{time_format}'.")
    time_difference = (input_time - start_time).total_seconds() / 60
    index = int(time_difference)
    if index < 0 or index > 375:
        raise ValueError("Out of range time")
    return index

def calDayHigh(df, column_name, end_index):
    dayHigh = 0 #set to min value
    for i in range(end_index + 1):
        currHigh = df.loc[i, column_name]
        if currHigh > dayHigh:
            dayHigh = currHigh
    return dayHigh

def calDayLow(df, column_name, end_index):
    dayLow = 1000000 #set to max value
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

def comesToStartingPointForHigh():
    pass

def comesToStartingPointForLow():
    pass
    
time_input = input("Enter time (HH:MM): ")
index = timeToIndex(time_input)

csv_file_path = 'Refactored-Data/NIFTY_data-cleaned/cleaned_NIFTY_2020.csv'
chunk_size = 375
column_names = ['Instrument', 'Date', 'Time', 'Open', 'High', 'Low', 'Close']
day_data_chunk = pd.read_csv(csv_file_path, skiprows=1, chunksize=chunk_size, header=None)
new_df = pd.DataFrame(columns=['Date', 'Time', 'DayHigh', 'DayLow', 'IsDayHighBreak', 'IsDayLowBreak'])

for day in day_data_chunk:
    if not day.empty:
        isDayHighBreak = False
        isDayLowBreak = False
        day.columns = column_names
        day.reset_index(drop=True, inplace=True)
        date = day['Date'].iloc[0]
        dayHigh = calDayHigh(day, 'High', index)
        isDayHighBreak = calHighBreak(day,'High',index,dayHigh)
        dayLow = calDayLow(day, 'Low', index)
        isDayLowBreak = calLowBreak(day,'Low',index, dayLow)
        print(f"Day High: {dayHigh}, Day Low: {dayLow}")
        new_df.loc[len(new_df)] = [date, time_input, dayHigh, dayLow, isDayHighBreak, isDayLowBreak]

new_df['DayHigh'] = new_df['DayHigh'].map('{:.2f}'.format)
new_df['DayLow'] = new_df['DayLow'].map('{:.2f}'.format)
new_df['Date'] = new_df['Date'].astype(float).astype(int).astype(str)

output_csv_path = 'testing/output.csv'
new_df.to_csv(output_csv_path, index=False)
print(f"Results saved to {output_csv_path}")