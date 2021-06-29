import pandas as pd
import os

cwd = os.getcwd()
file_name = os.environ.get('CSVNAME')
year_month = os.environ.get('PREFIX')
print("cwd:", cwd)
print("filename: ", file_name)

df = pd.read_csv(file_name)
df['year_month'] = year_month
print(df.shape)
print(df.colnames)

df.to_csv(file_name)