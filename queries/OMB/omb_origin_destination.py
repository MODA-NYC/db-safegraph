import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from calendar import monthrange
import json

load_dotenv()
root = Path(os.getenv('ROOT'))
df = pd.read_csv(root / 'queries' / 'OMB' / 'test_files' / 'raw_sample.csv')
df = df.drop(columns=[ 'median_dwell', 'popularity_by_each_hour', 'area_type', 'origin_area_type', 'date_range_end', 'day_counts', 'weekday_device_home_areas', 'weekend_device_home_areas', 'breakfast_device_home_areas', 'lunch_device_home_areas', 'afternoon_tea_device_home_areas', 'dinner_device_home_areas', 'nightlife_device_home_areas', 'work_hours_device_home_areas', 'work_behavior_device_home_areas', 'device_daytime_areas', 'distance_from_home', 'distance_from_primary_daytime_location', 'top_same_day_brand', 'top_same_month_brand', 'popularity_by_hour_monday', 'popularity_by_hour_tuesday', 'popularity_by_hour_wednesday', 'popularity_by_hour_thursday', 'popularity_by_hour_friday', 'popularity_by_hour_saturday', 'popularity_by_hour_sunday', 'device_type', 'iso_country_code', 'region'])

answer_df = pd.DataFrame(columns=['visited_cbg','start_date','visitor_cbg','visitor_count','total_visits_to_cbg', 'year', 'month'])

def is_in_nyc(cbg):
    nyc_county_fips = ['36005', '36047', '36061', '36081', '36085']
    test_cbg = cbg[0:5]
    if test_cbg in nyc_county_fips:
        return True
    else:
        return False



#print(df['device_home_areas'][0])
for index, row in df.iterrows():
    destinations = json.loads(row['device_home_areas'])
    #print(destinations)
    for destination in destinations:
        #test for applicability to nyc
        if not (is_in_nyc(str(destination)) or is_in_nyc(str(row['area']))):
            continue
        new_row = {'visited_cbg': row['area'], 'start_date': row['date_range_start'], 'visitor_cbg': destination, 'visitor_count': destinations.get(destination), 'total_visits_to_cbg': row['raw_device_counts'], 'year':row['y'], 'month':row['m']}
        answer_df = answer_df.append(new_row, ignore_index=True)     

answer_df.to_csv(root / 'queries' / 'OMB' / 'untracked_output' / 'origin_destination_pairs.csv')
#end