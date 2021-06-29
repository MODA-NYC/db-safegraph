import pandas as pd
import os

file_name = os.environ.get('CSVNAME')
year_month = os.environ.get('PREFIX')
print("filename: " df_name)

df = pd.read_csv(file_name)
print(df.shape)
print(df.colnames)
df['year_month'] = year_month

df.to_csv(file_name)