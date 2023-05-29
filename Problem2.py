import pandas as pd

df= pd.read_parquet('D:\Euan Ramnarine\Documents\WorkSamples\RiskThinking.AI\stocksandetfs\MergedData.parquet.gzip',engine='pyarrow')
#Feature Engineering
df['Date']=pd.to_datetime(df['Date'])
df = df.reset_index()
df=df.groupby('Symbol').apply(lambda x:x.sort_values('Date'))
df = df.set_index('Date')
#grouped_df.set_index('Date', inplace=True)
#rolling_avg = grouped_df['Volume'].rolling('30D').mean()
#data_df['Rolling Average']=rolling_avg.reset_index(drop=True)
df['vol_moving_avg']=df.groupby('Symbol')['Volume'].rolling('30D').mean().reset_index(0,drop=True)
df['adj_close_rolling_med']=df.groupby('Symbol')['Volume'].rolling('30D').median().reset_index(0,drop=True)
print(df)
df.to_parquet('Problem2.parquet.gzip',compression='gzip')