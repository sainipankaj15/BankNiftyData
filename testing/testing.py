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
    else:
        return 0
    return target

def pointsToHighSL(df, column_name, start_index, dayHigh, isDayHighBreak): #column_name = low
    stop_loss = dayHigh
    if isDayHighBreak:
        for i in range(start_index+1, 375):
            currLow = df.loc[i,column_name]
            if stop_loss >= currLow:
                stop_loss = currLow
    else:
        return 0
    return stop_loss

def pointsToLowTarget(df, column_name, start_index, dayLow, isDayLowBreak): #column_name = low
    target = dayLow
    if isDayLowBreak:
        for i in range(start_index+1, 375):
            currLow = df.loc[i,column_name]
            if target >= currLow:
                target = currLow
    else:
        return 0
    return target

def pointsToLowSL(df, column_name, start_index, dayLow, isDayLowBreak): #column_name = high
    stop_loss = dayLow
    if isDayLowBreak:
        for i in range(start_index+1, 375):
            currHigh = df.loc[i,column_name]
            if currHigh >= stop_loss:
                stop_loss = currHigh
    else:
        return 0
    return stop_loss
            
def closingValue():
    pass

time_input = input("Enter time (HH:MM): ")
index = timeToIndex(time_input)

csv_file_path = 'Refactored-Data/NIFTY_data-cleaned/cleaned_NIFTY_2020.csv'
chunk_size = 375
column_names = ['Instrument', 'Date', 'Time', 'Open', 'High', 'Low', 'Close']
day_data_chunk = pd.read_csv(csv_file_path, skiprows=1, chunksize=chunk_size, header=None)
new_df = pd.DataFrame(columns=['Date', 'Time', 'DayHigh', 'DayLow', 'IsDayHighBreak', 'IsDayLowBreak','ComesBackToDayHigh', 'ComesBackToDayLow', 'pointsFromTargetHigh' , 'pointsFromSLHigh', 'pointsFromTargetLow', 'pointsFromSLLow', 'Closing', 'ClosingPointDiffForHigh', 'ClosingPointDiffForLow'])

for day in day_data_chunk:
    if not day.empty:
        isDayHighBreak = False
        isDayLowBreak = False
        isComesBackToDayHigh = False
        isComesBackToDayLow = False
        pointsFromTargetHigh = 0
        pointsFromSLHigh = 0
        pointsFromTargetLow = 0
        pointsFromSLLow = 0
        
        day.columns = column_names
        day.reset_index(drop=True, inplace=True)
        date = day['Date'].iloc[0].astype(float).astype(int).astype(str)
        closing = day['Close'].iloc[373].astype(float)
        
        dayHigh = calDayHigh(day, 'High', index)
        isDayHighBreak = calHighBreak(day,'High',index,dayHigh)
        
        dayLow = calDayLow(day, 'Low', index)
        isDayLowBreak = calLowBreak(day,'Low',index, dayLow)
        
        print(f"Day High: {dayHigh}, Day Low: {dayLow}")
        isComesBackToDayHigh = comesBackToDayHigh(day,'Low',index,dayHigh,isDayHighBreak)
        isComesBackToDayLow = comesBackToDayHigh(day,'High',index,dayLow,isDayLowBreak)
        
        pointsFromTargetHigh = pointsToHighTarget(day,'High', index, dayHigh, isDayHighBreak)
        pointsFromSLHigh = pointsToHighSL(day,'Low', index, dayHigh, isDayHighBreak)
        pointsFromTargetLow = pointsToLowTarget(day,'Low', index, dayLow, isDayLowBreak)
        pointsFromSLLow = pointsToLowSL(day,'High', index, dayLow, isDayLowBreak)
        
        
        new_df.loc[len(new_df)] = [date, time_input, dayHigh, dayLow, isDayHighBreak, isDayLowBreak,isComesBackToDayHigh, isComesBackToDayLow, pointsFromTargetHigh, pointsFromSLHigh, pointsFromTargetLow, pointsFromSLLow, closing, closing-dayHigh, dayLow-closing]

new_df['DayHigh'] = new_df['DayHigh'].map('{:.2f}'.format)
new_df['DayLow'] = new_df['DayLow'].map('{:.2f}'.format)
new_df['pointsFromTargetHigh'] = new_df['pointsFromTargetHigh'].map('{:.2f}'.format)
new_df['pointsFromSLLow'] = new_df['pointsFromSLLow'].map('{:.2f}'.format)
new_df['pointsFromSLHigh'] = new_df['pointsFromSLHigh'].map('{:.2f}'.format)
new_df['pointsFromTargetLow'] = new_df['pointsFromTargetLow'].map('{:.2f}'.format)
new_df['Closing'] = new_df['Closing'].map('{:.2f}'.format)
new_df['ClosingPointDiffForHigh'] = new_df['ClosingPointDiffForHigh'].map('{:.2f}'.format)
new_df['ClosingPointDiffForLow'] = new_df['ClosingPointDiffForLow'].map('{:.2f}'.format)

output_csv_path = 'testing/output.csv'
new_df.to_csv(output_csv_path, index=False)
print(f"Results saved to {output_csv_path}")