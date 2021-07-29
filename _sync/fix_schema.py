import pandas as pd
import os

filename = os.getenv('YEAR_MONTH_FILENAME')
print(filename)

df = pd.read_csv(filename, compression='gzip')
ym = filename[:6]
ym_dt = pd.to_datetime(ym[:4]+'-'+ym[-2:])

print("before: ", df.shape)
print(ym_dt)

if ym_dt < pd.to_datetime('2021-07-01'):
    df.drop(columns=[
        'parent_safegraph_place_id', 
        'safegraph_place_id', 
        'tracking_opened_since'],
        inplace=True)
    df['geometry_type'] = ''

df['year_month'] = ym_dt

print("after: ", df.shape)

df.to_csv(filename, compression='gzip', index=False)
