import os
from _helper import aws
import pandas as pd
import boto3
from pathlib import Path
import json
import warnings
from datetime import datetime
import time

start_time = time.perf_counter()
print("start time is {}".format(datetime.now()))
date_query ='''
  SELECT MAX(date_range_start) as max_date
  FROM weekly_patterns_202107;
 '''
#can only upload as a a zip file or _helper.aws will break
output_date_path = f"output/dev/parks/latest_date.csv.zip"
print("output_date_path: {}".format(output_date_path))
#make sure to uncomment this in production.

print('executing latest date query')
aws.execute_query(query=date_query,
                  database="safegraph",
                  output=output_date_path)

#run query on it and get CSV
s3 = boto3.resource('s3')
cwd = os.getcwd()
s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/parks')
print('downloading latest date')
s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/latest_date.csv.zip', str(Path(cwd) / "latest_date.csv.zip"))
print('reading latest date')
df = pd.read_csv(Path(cwd) / "latest_date.csv.zip")
latest_date = df['max_date'][0][:10]
print('removing latest date')
os.remove(Path(cwd) / "latest_date.csv.zip")
print('max date: "{}"'.format(latest_date))

query = '''
SELECT * FROM weekly_patterns_202107 
WHERE substr(date_range_start, 1, 10) = '{}'        
AND substr(poi_cbg, 1, 5) in ('36005', '36047', '36061', '36081', '36085');
'''.format(latest_date)

output_csv_path = "output/dev/parks/nyc_weekly_patterns_latest.csv.zip"
#make sure to uncomment this in production
aws.execute_query(
    query=query,
    database="safegraph",
    output=output_csv_path
)
output_csv_path_2 = "output/dev/parks/{}.csv.zip".format(latest_date)
aws.execute_query(
    query=query,
    database="safegraph",
    output=output_csv_path_2
)
s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/nyc_weekly_patterns_latest.csv.zip', str(Path(cwd) / 'nyc_weekly_patterns_latest.csv.zip'))

##### get multiplier #####
#latest_date = '2021-08-09'
query = '''
SELECT hps.date_range_start as my_date_range_start, hps.census_block_group as cbg, hps.number_devices_residing as devices_residing, census.b01001e1 as cbg_pop, census.b01001e1 / (hps.number_devices_residing + 1.0) as pop_multiplier
FROM home_panel_summary_202107 AS hps
INNER JOIN census on hps.census_block_group = census.census_block_group
WHERE substr(hps.date_range_start, 1, 10) = '{}'

'''.format(latest_date)
#we want to include the entire census for multipliers (out of state visitors)
#AND substr(hps.census_block_group, 1, 5) IN ('36005', '36047', '36061', '36081', '36085')

output_csv_path = f"output/dev/parks/pop_to_device_multiplier.csv.zip"
#uncomment in production
aws.execute_query(
    query=query,
    database="safegraph",
    output=output_csv_path
)

s3.Bucket('recovery-data-partnership').download_file("output/dev/parks/pop_to_device_multiplier.csv.zip", str(Path(cwd) / 'multiplier_temp.csv.zip'))

df_mult = pd.read_csv(Path(cwd) / 'multiplier_temp.csv.zip', dtype={'cbg': object})

##### join census to cbg to weekly patterns and multiply #####
df = pd.read_csv(Path(cwd) / "nyc_weekly_patterns_latest.csv.zip" )
# for each row in the dataframe

visitors_pop_list = []
visits_pop_list = []
multiplier_list = []

for index, row in df.iterrows():
    #read the visitor home cbgs and parse json
    #print(row['visitor_home_cbgs'])
    iter = 0

    pop_count = 0.0
    no_match_count = 0
    no_match_count_rows = 0
    sum_cbg_pop = 0
    sum_cbg_devices = 0

    this_json = json.loads(row['visitor_home_cbgs'])
    #for each item in the dictionary
    for key, value in this_json.items():
        #multiply devices by people per device table
        iter = iter + 1
        selected_rows = df_mult.iloc[:, df_mult.columns.get_loc('cbg')] == key
        #filter df_mult
        selected_rows_mult_df = df_mult[selected_rows]
        #isolate multiplier
        try:
            #take the first row. should only be one match. Need to get this check working.
            if len(selected_rows_mult_df[selected_rows_mult_df['cbg'] == key]) > 1:
                warning_message = "more than one match for key {}".format(key)
                warnings.warn(warning_message)
            multiplier = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('pop_multiplier')]
            cbg_pop = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('cbg_pop')]
            devices_residing = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('devices_residing')]



        except IndexError:
            warning_message = 'warning: there is no row zero for key {}'.format(key)
            warnings.warn(warning_message)
            no_match_count = no_match_count + value
            no_match_count_rows = no_match_count_rows + 1
            #if no multiplier, pop_count stays the same. Added back after the loop.
            multiplier = 0
            cbg_pop = 0
            devices_residing = 0
        pop_calc = pop_count + multiplier * value * 1.0
        sum_cbg_pop = sum_cbg_pop + cbg_pop
        sum_cbg_devices = sum_cbg_devices + devices_residing
    #to fill in the missing values (i.e. Canada) take the average population of the other cbgs
    no_zero_lambda_func = (lambda x : x if x > 0 else 1)

    synthetic_mult = sum_cbg_pop / no_zero_lambda_func(sum_cbg_devices * 1.0)
    visitors_pop_count = row['raw_visitor_counts'] * synthetic_mult
    visits_pop_count = row['raw_visit_counts'] * synthetic_mult
    #add value to list
    visitors_pop_list.append(visitors_pop_count)
    visits_pop_list.append(visits_pop_count)
    multiplier_list.append(synthetic_mult)
    #print("final pop count: {}".format(pop_count))

df['visits_pop_calc'] = visits_pop_list
df['visitors_pop_calc'] = visitors_pop_list
df['pop_multiplier'] = multiplier_list

print(df.info())
df.to_csv(Path(cwd) /'poi_weekly_pop_added.csv')
s3.Bucket('recovery-data-partnership').upload_file(str(Path(cwd) / 'poi_weekly_pop_added.csv'), "output/dev/parks/poi_with_population_count.csv")

df_ans = pd.read_csv('poi_weekly_pop_added.csv')
print(df_ans.info())
print(df_ans.head(20))


#uncomment in production
#os.remove(Path(cwd) / 'nyc_weekly_patterns_latest.csv.zip')
#os.remove(Path(cwd) / 'census_tmp.csv')
#os.remove(Path(cwd) / 'multiplier_temp.csv')
#os.remove(Path(cwd) / 'poi_weekly_pop_added.csv')

print("Successfully completed at {}".format(datetime.now()))
end_time = time.perf_counter()
elapsed_time = end_time - start_time
time_string = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
print('elapsed time: {}'.format(time_string))
