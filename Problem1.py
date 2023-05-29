import os
import glob
import pandas as pd

csv_files1 = glob.glob('stocks/*.csv') #pull all stock csv files within workspace
data_df1 = pd.concat([pd.read_csv(fp).assign(StockName=os.path.basename(fp).split('.')[0]) for fp in csv_files1]) #merge into singular file with stock names from csv filenames
csv_files2 = glob.glob('etfs/*.csv') #pull all stock csv files within workspace
data_df2 = pd.concat([pd.read_csv(fp).assign(StockName=os.path.basename(fp).split('.')[0]) for fp in csv_files2]) #merge into singular file with etf names from csv filenames

data_df=pd.concat([data_df1, data_df2]) #combine all into singular file

key_df=pd.read_csv('symbols_valid_meta.csv') #add join field
data_df=data_df.set_index('StockName').join(key_df.set_index('NASDAQ Symbol')) #define join criteria

data_df.drop(data_df.columns.difference(['Symbol','Security Name','Date','Open','High','Low','Close','Adj Close','Volume']),axis=1,inplace=True) #select columns to display
cols=['Symbol','Security Name','Date','Open','High','Low','Close','Adj Close','Volume'] #choose order to display in
data_df=data_df.reset_index(drop=True)
data_df=data_df.reindex(cols,axis='columns')
data_df=data_df.convert_dtypes() #choose datatypes
print(data_df)
# print("Done!")
print(data_df.dtypes)

#Convert to Parquet
data_df.to_parquet('MergedData.parquet.gzip',compression='gzip')
pd.read_parquet('MergedData.parquet.gzip')

