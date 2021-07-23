import pandas as pd
import os

files = [f for f in os.listdir('.') if f[-6:]=='csv.gz']
print("Python script")
print("files: ", files)

for f in files:
    print("filename: ", f)
    year_month = f[:6]
    df = pd.read_csv(file, compression='gzip')
    df['year_month'] = year_month
    print(df.shape)
    print(list(df))
    df.to_csv('_'+file, compression='gzip')