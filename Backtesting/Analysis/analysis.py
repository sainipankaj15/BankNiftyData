import pandas as pd
import os

def countBreak(df, colName):
    cnt = 0
    for val in df[colName]:
        if val==True:
            cnt += 1
    return cnt

def countBothBreak(df, col1, col2):
    cnt = 0
    for i in range(df.shape[0]):
        if df[col1].iloc[i] and df[col2].iloc[i]:
            cnt += 1
    return cnt

def maxVal(df, colName):
    temp = float('-inf')
    for i in range(df.shape[0]):
        currTarget = df.loc[i,colName]
        if temp<currTarget:
            temp = currTarget
    return temp

def minVal(df, colName):
    temp = float('inf')
    for i in range(df.shape[0]):
        curr = df.loc[i,colName]
        if temp>curr:
            temp = curr
    return temp

def avg(df, colName):
    sum = 0
    freq = 0
    for i in range(df.shape[0]):
        val = df.loc[i,colName]
        sum += val
        if val!=0:
            freq+=1
    average = sum/freq if freq>0 else 0
    return round(average,3)

def processCSV(input_folder, output_file_path):
    all_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    newdf = pd.DataFrame(
        columns=['Year', 'NumberOfTradingDays', 'NumberOfNoBreak', 'NumberOfHighBreak', 'NumberOfLowBreak', 'NumberOfBothBreak','MaximumTargetForHigh', 'MaximumSLforHigh', 'MaximumTargetForLow', 'MaximumSLforLow', 'AvgTargetforHigh', 'AvgSLforHigh', 'AvgTargetforLow', 'AvgSLforLow']
    )
    for file_name in all_files:
        file_path = os.path.join(input_folder, file_name)
        df = pd.read_csv(file_path)
        df['Date'] = df['Date'].astype(str)
        year = df['Date'].str[:4].iloc[0]
        tradingDays = df.shape[0]
        numberOfNoBreak = countBreak(df,'noBreak')
        numberOfHighBreak = countBreak(df,'IsDayHighBreak') 
        numberOfLowBreak = countBreak(df,'IsDayLowBreak')
        numberOfBothBreak = countBothBreak(df,'IsDayHighBreak','IsDayLowBreak')
        maxTargetHigh = maxVal(df,'percentChangeTargetHigh')
        maxSLHigh = minVal(df,'percentageChangeSLHigh')
        maxTargetLow = maxVal(df,'percentageChangeTargetLow')
        maxSLLow = minVal(df,'percentageChangeSLLow')
        avgTargetHigh = avg(df,'percentChangeTargetHigh')
        avgSLHigh = avg(df,'percentageChangeSLHigh')
        avgTargetLow = avg(df,'percentageChangeTargetLow')
        avgSLLow = avg(df,'percentageChangeSLLow')
        
        
        newdf.loc[len(newdf)] = [year,tradingDays,numberOfNoBreak,numberOfHighBreak,numberOfLowBreak,numberOfBothBreak,maxTargetHigh,maxSLHigh,maxTargetLow,maxSLLow,avgTargetHigh,avgSLHigh,avgTargetLow,avgSLLow]
    newdf.to_csv(output_file_path, index=False)
    
def main():
    base_dir = 'Backtesting/CSV'
    analysis_dir = 'Backtesting/Analysis'
    os.makedirs(analysis_dir, exist_ok=True)

    time_folders = [f for f in os.listdir(base_dir) if 'backtest' in f]
    
    for time_folder in time_folders:
        for folder_name in ['BANK_NIFTY_data-backtest', 'NIFTY_data-backtest']:
            input_folder = os.path.join(base_dir, time_folder, folder_name)
            output_file_name = f"{folder_name}.csv"
            output_file_path = os.path.join(analysis_dir, time_folder, output_file_name)
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            processCSV(input_folder, output_file_path)

if __name__ == "__main__":
    main()