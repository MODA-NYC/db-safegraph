import os
from _helper import aws
import pandas as pd
import boto3
from pathlib import Path
import json



date_query ='''
  SELECT MAX(date_range_start) as max_date
  FROM weekly_patterns_202107;
'''
output_date_path = f"output/dev/parks/latest_date.csv"
#make sure to uncomment this in production.
#aws.execute_query(query=date_query,
#                  database="safegraph",
#                  output=output_date_path)


#run query on it and get CSV
s3 = boto3.resource('s3')
cwd = os.getcwd()
s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/parks')
#s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/latest_date.csv', str(Path(cwd) / "latest_date.csv"))

df = pd.read_csv(Path(cwd) / "latest_date.csv")
latest_date = df['max_date'][0][:10]
#os.remove(Path(cwd) / "latest_date.csv")
print('max date: "{}"'.format(latest_date))

query = '''
SELECT * FROM weekly_patterns_202107 
WHERE substr(date_range_start, 1, 10) = '{}'        
AND substr(poi_cbg, 1, 5) in ('36005', '36047', '36061', '36081', '36085');
'''.format(latest_date)

output_csv_path = f"output/dev/parks/nyc_weekly_patterns_latest.csv.zip"
#make sure to uncomment this in production
#aws.execute_query(
#    query=query,
#    database="safegraph",
#    output=output_csv_path
#)
output_csv_path_2 = "output/dev/parks/{}.csv.zip".format(latest_date)
#aws.execute_query(
#    query=query,
#    database="safegraph",
#    output=output_csv_path_2
#)
#s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/nyc_weekly_patterns_latest.csv.zip', str(Path(cwd) / 'nyc_weekly_patterns_latest.csv.zip'))

##### get multiplier #####
#latest_date = '2021-08-09'
query = '''
SELECT hps.date_range_start as my_date_range_start, hps.census_block_group as cbg, hps.number_devices_residing as devices_residing, census.b01001e1 as cbg_pop, census.b01001e1 / (hps.number_devices_residing + 1.0) as pop_multiplier
FROM home_panel_summary_202107 AS hps
INNER JOIN census on hps.census_block_group = census.census_block_group
WHERE substr(hps.date_range_start, 1, 10) = {}
AND substr(hps.census_block_group, 1, 5) IN ('36005', '36047', '36061', '36081', '36085')
'''.format(latest_date)

output_csv_path = f"output/dev/parks/pop_to_device_multiplier.csv"
##uncomment in production
#aws.execute_query(
#    query=query,
#    database="safegraph",
#    output=output_csv_path
#)

#s3.Bucket('recovery-data-partnership').download_file("output/dev/parks/pop_to_device_multiplier.csv", str(Path(cwd) / 'multiplier_temp.csv'))
df_mult = pd.read_csv(Path(cwd) / 'multiplier_temp.csv')
print(df_mult.info())
print(df_mult.head())


##### join census to cbg to weekly patterns and multiply #####

print(df_mult.head())
# for each row in the dataframe

for index, row in df.iterrows():
    #read the visitor home cbgs and parse json
    #print(row['visitor_home_cbgs'])
    this_json = json.loads(row['visitor_home_cbgs'])
    pop_count = 0.0
    #for each item in the dictionary
    for key, value in this_json.items():
        #multiply devices by people per device table
        key = int(key)
        #print(key)
        #print("is in? {}".format(key in df_mult['cbg']))
        #print("type key: {}; type series: {}".format(type(key), type(df_mult['cbg'])))
        multiplier = df_mult.loc['cbg' == int(key)]
        pop_count = pop_count + multiplier * value * 1.0
        #add to list
    print(pop_count)

#uncomment in production
#os.remove(Path(cwd) / 'nyc_weekly_patterns_latest.csv.zip')
#os.remove(Path(cwd) / 'census_tmp.csv')
#os.remove(Path(cwd) / 'multiplier_temp.csv')
