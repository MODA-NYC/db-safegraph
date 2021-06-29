import pandas as pd
import os

files = [f for f in os.listdir('.') if f[-3:]=='csv']
print("files: ", files)

for f in files:
    year_month = f[:6]
    print("filename: ", f)
    print("year-month: ", year_month)

    df = pd.read_csv(f)
    df['year_month'] = year_month
    print(df.shape)

    df.to_csv(f)