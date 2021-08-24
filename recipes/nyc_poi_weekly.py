import os
from _helper import aws
import pandas as pd
import boto3
from pathlib import Path



date_query ='''
  SELECT MAX(date_range_start) as max_date
  FROM weekly_patterns_202107;
'''
output_date_path = f"output/dev/parks/latest_date.csv"
#aws.execute_query(query=date_query,
#                  database="safegraph",
#                  output=output_date_path)


#run query on it and get CSV
s3 = boto3.resource('s3')
cwd = os.getcwd()
s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/parks')
s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/latest_date.csv', str(Path(cwd) / "latest_date.csv"))

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
aws.execute_query(
    query=query,
    database="safegraph",
    output=output_csv_path
)

s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/nyc_weekly_patterns_latest.csv.zip', str(Path(cwd) / 'nyc_weekly_patterns_latest.csv.zip'))

df = pd.read_csv(Path(cwd) / 'nyc_weekly_patterns_latest.csv.zip')
print(df.info())
os.remove(Path(cwd) / 'nyc_weekly_patterns_latest.csv.zip')
                
'''    
#run query on it and get CSV
s3 = boto3.resource('s3')
cwd = os.getcwd()
s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/ops')
s3.Bucket('recovery-data-partnership').download_file('output/dev/ops/neighborhood_patterns_US_latest.csv.zip', str(Path(cwd) / "neighborhood_patterns_US_latest.csv.zip"))
#s3.Bucket('recovery-data-partnership').download_file('output/dev/ops/poi_info/poi_info.csv.zip',str(Path(cwd) / "foo.zip"))

###### taken from query folder #####
df = pd.read_csv(Path(cwd) / "neighborhood_patterns_US_latest.csv.zip")
os.remove(Path(cwd) / "neighborhood_patterns_US_latest.csv.zip")
'''