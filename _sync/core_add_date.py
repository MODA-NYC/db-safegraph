import pandas as pd
import os

filename = os.getenv('YEAR_MONTH_FILENAME')
print(filename)

df = pd.read_csv(filename, compression='gzip')
df['year_month'] = filename[:6]
print(df.shape)
df.to_csv(filename, compression='gzip')

# files = [f for f in os.listdir('.') if f[-6:]=='csv.gz']
# print("Python script")
# print("files: ", files)

# for f in files:
#     print("filename: ", f)
#     year_month = f[:6]
#     df = pd.read_csv(f, compression='gzip')
#     df['year_month'] = year_month
#     print(df.shape)
#     print(list(df))
#     df.to_csv(f, compression='gzip')