import pandas as pd
import os

cwd = os.getcwd()
file_name = os.environ.get('CSVNAME')
year_month = os.environ.get('PREFIX')
print("cwd:", cwd)
print("filename: ", df_name)

df = pd.read_csv(file_name)
print(df.shape)
print(df.colnames)
df['year_month'] = year_month

df.to_csv(file_name)