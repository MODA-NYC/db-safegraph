import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from calendar import monthrange
import json
import boto3


load_dotenv()
cwd = Path(os.getcwd())

query = '''
SELECT * FROM neighborhood_patterns_202107 
WHERE date_range_start = (SELECT MAX(date_range_start)
                          FROM neighborhood_patterns_202107 as np2
                          );
'''
output_csv_path =f"output/dev/ops/neighborhood_patterns_US_latest.csv.zip"
#this is done in OMB_origin_destination in a previous step in the yaml file.
#aws.execute_query(
#    query=query,
#    database="safegraph",
#    output=output_csv_path
#)
  
    
#run query on it and get CSV
s3 = boto3.resource('s3')
cwd = os.getcwd()
s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/ops')
s3.Bucket('recovery-data-partnership').download_file('output/dev/ops/neighborhood_patterns_US_latest.csv.zip', str(Path(cwd) / "neighborhood_patterns_US_latest.csv.zip"))
#s3.Bucket('recovery-data-partnership').download_file('output/dev/ops/poi_info/poi_info.csv.zip',str(Path(cwd) / "foo.zip"))

###### taken from query folder #####
df = pd.read_csv(Path(cwd) / "neighborhood_patterns_US_latest.csv.zip")
os.remove(Path(cwd) / "neighborhood_patterns_US_latest.csv.zip")   

df = df.drop(columns=['area_type', 'origin_area_type', 'date_range_end', 'day_counts', 'weekday_device_home_areas', 'weekend_device_home_areas', 'breakfast_device_home_areas', 'lunch_device_home_areas', 'afternoon_tea_device_home_areas', 'dinner_device_home_areas', 'nightlife_device_home_areas', 'work_hours_device_home_areas', 'work_behavior_device_home_areas', 'device_daytime_areas', 'distance_from_home', 'distance_from_primary_daytime_location', 'top_same_day_brand', 'top_same_month_brand', 'popularity_by_hour_monday', 'popularity_by_hour_tuesday', 'popularity_by_hour_wednesday', 'popularity_by_hour_thursday', 'popularity_by_hour_friday', 'popularity_by_hour_saturday', 'popularity_by_hour_sunday', 'device_type', 'iso_country_code', 'region'])

def number_of_days_in_month(year, month):
    return monthrange(year, month)[1]

month_days = (number_of_days_in_month(df['y'][0], df['m'][0]))
month_hours = month_days * 24

#create columns 
df_copy = df.copy()
for index, row in df_copy.iterrows():
    row_days = json.loads(row['stops_by_day'])
    for i in range(0, len(row_days)):
        #make a column for every day
        col_name = "dstopsD{}".format(i + 1)
        if (col_name not in df.columns.to_list()):
            df[col_name] = row_days[i]
        else:
            df.loc[index, col_name] = row_days[i]
    row_hours = json.loads(row['stops_by_each_hour'])
    d = 0
    h = 0
    for i in range(0, len(row_hours)):
        #cycle days and hours to be readable
        h = h + 1
        if (i % 24 == 0):
            d = d + 1
            h = 0
        col_name = "stopsD{}H{}".format(d, h)
        #make a column for every hour of the month!
        if (col_name not in df.columns.to_list()):
            df[col_name] = row_hours[i] 
        else:
            df.loc[index, col_name] = row_hours[i]
    row_pop_hours = json.loads(row['popularity_by_each_hour'])
    d = 0
    h = 0
    for i in range(0, len(row_pop_hours)):
        h = h + 1
        if (i % 24 == 0):
            d = d + 1
            h = 0
        col_name = "popularityD{}H{}".format(d, h)
        #make a column for populartity for every hour of the month!
        if (col_name not in df.columns.to_list()):
            df[col_name] = row_pop_hours[i] 
        else:
            df.loc[index, col_name] = row_pop_hours[i]
       
#print(df.columns.to_list())    
df.to_csv( Path(cwd) / 'python_1500_columns.csv')

#upload CSV to AWS S3
s3.Bucket('recovery-data-partnership').upload_file(str((Path(cwd) / 'python_1500_columns.csv')), 'output/dev/omb/omb_1500_columns.csv')
#aws.move_output(queryLoc=str((Path(cwd) / 'OMB_query_temp.csv')), queryMetadata=None, outputLoc=f"output/dev/ops/OMB_origin_destination.py")
os.remove('python_1500_columns.csv')