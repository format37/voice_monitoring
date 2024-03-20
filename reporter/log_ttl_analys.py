from uuid import uuid4
rand_token = uuid4()
str(rand_token)
import numpy as np
import pandas as pd
import math
from onec_request import OneC_Request
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

def get_len(last_string):
    try:
        splitters = ['*','a','b','c','d','e','f','g','h','i','j']
        ttl = []
        for splitter in splitters:
            new_string = last_string.split(splitter)
            last_string = new_string[1]
            ttl.append(new_string[0])
        ttl.append(last_string)
        first_value = int(ttl[2])
        last_value = int(ttl[-2:][0])
        return last_value-first_value
    except Exception as e:
        return 0

def get_func(last_string):
    return last_string.split('*')[0]

def get_timers(last_string,step):
    try:
        splitters = ['*','a','b','c','d','e','f','g','h','i','j']
        ttl = []
        for splitter in splitters:
            new_string = last_string.split(splitter)
            last_string = new_string[1]
            ttl.append(new_string[0])
        ttl.append(last_string)    
        return int(ttl[step])/1000
    except Exception as e:
        return 0 

def plot_dates(df, func):
    # Exclude partlist function
    if func in ['partlist']:
        return []
    df['day'] = pd.to_datetime(df['date'].str.split().str[0], format='%d.%m.%Y')
    start_date = df['day'].min()
    end_date = df['day'].max()
    mask = (df['day'] >= start_date) & (df['day'] <= end_date)
    filtered_df = df[mask]
    
    time_fields = [
        'ab_mrm_to_back',
        'bc_back_to_back',
        'cd_back_to_1c',
        'de_1c_to_1c',
        'ef_1c_to_back',
        'fg_back_to_back',
        'gh_back_to_mrm',
        'hi_mrm_to_mrm',
        'full_len'
    ]
    
    filenames = []
    
    # Plot mean values
    plt.figure(figsize=(15, 10))
    for field in time_fields:
        mean_data = filtered_df[filtered_df.func == func].groupby('day')[field].mean()
        plt.plot(mean_data.index, mean_data.values, label=field)
    
    filename = f"mean_log_ttl_{func}.png"
    plt.xlabel('Date')
    plt.ylabel('Time (seconds)')
    plt.title(f'Average Time by Date - {func}')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=75)  # Rotate x-axis labels by 75 degrees
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    filenames.append(filename)
    
    # Plot maximum values
    plt.figure(figsize=(15, 10))
    for field in time_fields:
        max_data = filtered_df[filtered_df.func == func].groupby('day')[field].max()
        plt.plot(max_data.index, max_data.values, label=field)
    
    filename = f"max_log_ttl_{func}.png"
    plt.xlabel('Date')
    plt.ylabel('Time (seconds)')
    plt.title(f'Maximum Time by Date - {func}')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=75)  # Rotate x-axis labels by 75 degrees
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    filenames.append(filename)
    
    # Plot median values
    plt.figure(figsize=(15, 10))
    for field in time_fields:
        median_data = filtered_df[filtered_df.func == func].groupby('day')[field].median()
        plt.plot(median_data.index, median_data.values, label=field)
    
    filename = f"median_log_ttl_{func}.png"
    plt.xlabel('Date')
    plt.ylabel('Time (seconds)')
    plt.title(f'Median Time by Date - {func}')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=75)  # Rotate x-axis labels by 75 degrees
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    filenames.append(filename)
    
    return filenames

def log_ttl_report(days_back):
    config_file = 'config.json'
    onec_request = OneC_Request(config_file)
    query_params = {
        "Идентификатор": "logttl",
        "date_from": (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00"),
        "date_to": datetime.now().strftime("%Y-%m-%dT00:00:00")
    }
    print(query_params)
    result_dfs = onec_request.execute_query(query_params)
    for key in result_dfs.keys():
        print(key)
    df = result_dfs['msk']
    print(f'len of df: {len(df)}')
    df = df.iloc[:, :-1]
    df['Backend'] = 'not_defined'
    # report(df)
    df['dev_len']=df['ttl'].apply(get_len)
    df['func']=df['ttl'].apply(get_func)

    timer_columns = ['a','b','c','d','e','f','g','h','i','j']
    for i in range(0,len(timer_columns)):
        column_name = timer_columns[i]
        df[column_name]=df['ttl'].apply(get_timers,step=i+2)

    # report(df)

    for phone in df["phone"].unique():
        for backend in df[df['phone'] == phone].Backend.unique():
            mask = ( df['phone'] == phone) & (df['Backend']==backend)
            mr = df[mask].sort_values(by=['dev_len']).iloc[0] #minimal delay record
            bias_top=(mr.h-mr.a-(mr.g-mr.b))/2-(mr.b-mr.a)
            bias_bottom=(mr.f+bias_top-(mr.c+bias_top)-(mr.e-mr.d))/2-(mr.d-(mr.c+bias_top))
            df.loc[mask, 'bias_top'] = bias_top
            df.loc[mask, 'bias_bottom'] = bias_bottom

    df['ab_mrm_to_back']      = df.b - df.a + df.bias_top    
    df['bc_back_to_back']     = df.c - df.b
    df['cd_back_to_1c']       = df.d - df.c + df.bias_bottom - df.bias_top
    df['de_1c_to_1c']         = df.e - df.d
    df['ef_1c_to_back']       = df.f - df.e + df.bias_top - df.bias_bottom
    df['fg_back_to_back']     = df.g - df.f
    df['gh_back_to_mrm']      = df.h - df.g - df.bias_top
    df['hi_mrm_to_mrm']       = df.i - df.h
    df['full_len']=df['ab_mrm_to_back']+df['bc_back_to_back']+df['cd_back_to_1c']+df['de_1c_to_1c']+df['ef_1c_to_back']+df['fg_back_to_back']+df['gh_back_to_mrm']+df['hi_mrm_to_mrm']
    # report(df,0)
    max(df[np.abs(df.ef_1c_to_back-df.ef_1c_to_back.mean()) <= (3*df.ef_1c_to_back.std())].ef_1c_to_back)

    df_b = df
    time_fields = [
        'ab_mrm_to_back',
        'bc_back_to_back',
        'cd_back_to_1c',
        'de_1c_to_1c',
        'ef_1c_to_back',
        'fg_back_to_back',
        'gh_back_to_mrm',
        'hi_mrm_to_mrm'
    ]
    for field in time_fields:
        df_b = df_b[np.abs(df_b[field]-df_b[field].mean()) <= (2.1*df_b[field].std())]

    file_list = []
    for func in list(set(df.func)):
        result = plot_dates(df, func)
        for r in result:
            file_list.append(r)

    # Sort file list by name
    file_list = sorted(file_list)

    return file_list
    
def main():
    file_list = log_ttl_report(14)
    for f in file_list:
        print(f)

if __name__ == "__main__":
    main()
