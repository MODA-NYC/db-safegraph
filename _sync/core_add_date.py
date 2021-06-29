import pandas as pd
import os

file_name = [f for f in os.listdir('.') if f[-3:]=='csv'][0]
year_month = file_name[:6]
print("filename: ", file_name)
print("year-month: ", year_month)

df = pd.read_csv(file_name)
df['year_month'] = year_month
print(df.shape)
print(df.columns)

df.to_csv(file_name)