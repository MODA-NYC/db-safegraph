import pandas as pd
import os

filename = os.getenv('YEAR_MONTH_FILENAME')
print(filename)

df = pd.read_csv(filename, compression='gzip')

# first 6 chars contain year and month of snapshot
ym = filename[:6]
# convert yearmonth to datetime
ym_dt = pd.to_datetime(ym[:4]+'-'+ym[-2:])

print("before: ", df.shape)

# adjust schema on old data to match new schema
if ym_dt < pd.to_datetime('2021-07-01'):
    df.drop(columns=[
        'parent_safegraph_place_id', 
        'safegraph_place_id', 
        'tracking_opened_since'],
        inplace=True)
    df['geometry_type'] = ''

# append yearmonth to all datasets
df['year_month'] = ym_dt

print("after: ", df.shape)

df.to_csv(filename, compression='gzip', index=False)
