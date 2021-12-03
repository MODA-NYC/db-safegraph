import os
from _helper import aws
import pandas as pd
import boto3
from pathlib import Path
import json
import warnings
from datetime import datetime
import time
import numpy as np
from multiprocessing import log_to_stderr
from multiprocessing import Pool
import logging
import sys
from ast import literal_eval

is_prod = True
n_cores = 1
#You need to be located inside the recipes folder
#loop through the dates.
def my_main(split_chunk):
    s3 = boto3.resource('s3')
    cwd = os.getcwd()
    s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/parks')
    for my_date in split_chunk:
        latest_date = my_date
        print('for date: "{}"'.format(latest_date))
        query = '''
        SELECT * FROM weekly_patterns_202107
        WHERE substr(date_range_start, 1, 10) = '{}'
        AND substr(poi_cbg, 1, 5) in ('36005', '36047', '36061', '36081', '36085');

        '''.format(latest_date)

        output_csv_path_2 = 'output/dev/parks/nyc-weekly-patterns/nyc_weekly_patterns_{}.csv.zip'.format(latest_date)
        if is_prod:
            aws.execute_query(
                query=query,
                database="safegraph",
                output=output_csv_path_2
            )


            s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/nyc-weekly-patterns/nyc_weekly_patterns_{}.csv.zip'.format(latest_date), str(Path(cwd) / f'nyc_weekly_patterns_temp_{latest_date}.csv.zip'))

        ##### get multiplier #####
        
        query = '''
        SELECT (substr(hps.date_range_start, 1, 10)) as date_range_start, hps.census_block_group as cbg, hps.number_devices_residing as devices_residing, census.b01001e1 as cbg_pop, census.b01001e1 / (CASE WHEN(hps.number_devices_residing = 0) THEN 1 ELSE hps.number_devices_residing END)as pop_multiplier
        FROM hps_crawled22 AS hps
        INNER JOIN census on hps.census_block_group = census.census_block_group
        WHERE substr(hps.date_range_start, 1, 10) = '{}'
        UNION
        SELECT (substr(hps2.date_range_start, 1, 10)) as date_range_start, hps2.census_block_group as cbg, hps2.number_devices_residing as devices_residing, census.b01001e1 as cbg_pop, census.b01001e1 / (CASE WHEN(hps2.number_devices_residing = 0) THEN 1 ELSE hps2.number_devices_residing END) as pop_multiplier
        FROM hps_crawledhome_panel_summary_202107 AS hps2
        INNER JOIN census on hps2.census_block_group = census.census_block_group
        WHERE substr(hps2.date_range_start, 1, 10) = '{}';
        '''.format(latest_date, latest_date)
        #we want to include the entire census for multipliers (out of state visitors)
        #AND substr(hps.census_block_group, 1, 5) IN ('36005', '36047', '36061', '36081', '36085')

        output_csv_path = f'output/dev/parks/safegraph-with-population/multipliers/pop_to_device_multiplier_{latest_date}.csv.zip'
        if is_prod:
            aws.execute_query(
                query=query,
                database="safegraph",
                output=output_csv_path
            )

        s3.Bucket('recovery-data-partnership').download_file(f'output/dev/parks/safegraph-with-population/multipliers/pop_to_device_multiplier_{latest_date}.csv.zip', str(Path(cwd) / f'multiplier_temp_{latest_date}.csv.zip'))

        df_mult = pd.read_csv(Path(cwd) / f'multiplier_temp_{latest_date}.csv.zip', dtype={'cbg': object})
        #print(df_mult.info())
        #print(df_mult.head())
        #works

        ##### join census to cbg to weekly patterns and multiply #####
        df = pd.read_csv(Path(cwd) / f'nyc_weekly_patterns_temp_{latest_date}.csv.zip' )
        # for each row in the dataframe
        if ((len(df) == 0) or (len(df_mult) == 0)) :
            warnings.warn(f"{latest_date} Either the home panel summary or the weekly patterns were not found")
            continue

        multiplier_list = []
        sys.stdout.flush()
        sys.stderr.flush()
        if is_prod:

            for index, row in df.iterrows():
                iter = 0
                pop_count = 0.0
                #no_match_count isn't being used in prod.
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
                        #take the first row. should only be one match. 
                        if len(selected_rows_mult_df[selected_rows_mult_df['cbg'] == key]) > 1:
                            warning_message = "more than one match for key {}".format(key)
                            warnings.warn(warning_message)
                        #multiplier = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('pop_multiplier')]
                        cbg_pop = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('cbg_pop')]
                        # need to multiply by the number of visitors to get a weighted average
                        cbg_pop = cbg_pop * value

                        devices_residing = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('devices_residing')]
                        #and here
                        devices_residing = devices_residing * value



                    except IndexError:

                        warning_message = 'warning: there is no row zero for key {}'.format(key)
                        #warnings.warn(warning_message)
                        #no_match_count isn't being used
                        no_match_count = no_match_count + value
                        no_match_count_rows = no_match_count_rows + 1
                        #if no multiplier, pop_count stays the same. Added back after the loop.
                        #multiplier = 0
                        cbg_pop = 0
                        devices_residing = 0
                    #this is done below. see synthtetic mult.
                    #pop_calc = pop_count + multiplier * value * 1.0
                    sum_cbg_pop = sum_cbg_pop + cbg_pop
                    sum_cbg_devices = sum_cbg_devices + devices_residing
                    sys.stdout.flush()
                    sys.stderr.flush()
                #to fill in the missing values (i.e. Canada) take the average population of the other cbgs
                no_zero_lambda_func = (lambda x : x if x > 0 else 1)
                synthetic_mult = sum_cbg_pop / no_zero_lambda_func(sum_cbg_devices * 1.0)

                #if mutliplier is zero (for small visit counts without home block groups)

                sys.stdout.flush()
                sys.stderr.flush()
                multiplier_list.append(synthetic_mult)
                #print("final pop count: {}".format(pop_count))




            non_zero_multipliers =  [x for x in multiplier_list if x > 0]
            #print(f"non zero multipliers: {non_zero_multipliers}")
            #take the average of all the multipliers.
            avg_multiplier = np.mean(non_zero_multipliers)
            #fill multipliers with imputed multiplier.
            multiplier_list = [x if x > 0 else avg_multiplier for x in multiplier_list]

            #convert device counts to population counts based on multiplier series.
            df['pop_multiplier'] = multiplier_list
            df['visits_pop_calc'] = multiplier_list * df['raw_visit_counts']
            df['visitors_pop_calc'] = multiplier_list * df['raw_visitor_counts']
            #it is a bit more complex to multiply a list
            df['visits_by_day_pop_calc'] = None
            df['visits_by_hour_pop_calc'] = None
            df_copy = df.copy()

            def multiply_list(by_day_list, multiplier_list):
                visits_by_day = [float(x) * 1.0 for x in literal_eval(by_day_list)]
                this_multiplier = multiplier_list[index]
                return list(np.multiply(visits_by_day, (np.repeat(this_multiplier * 1.0, len(visits_by_day)))))
                
            for index, row in df_copy.iterrows():
                df.at[index, 'visits_by_day_pop_calc' ] = multiply_list(row['visits_by_day'], multiplier_list)
                df.at[index, 'visits_by_hour_pop_calc'] = multiply_list(row['visits_by_each_hour'], multiplier_list)
            #garbage collection
            df_copy = None
            #df['visits_by_day_pop_calc'] = multiplier_list * df['visits_by_day']
            warnings.warn(str(df.head(5)))
            sys.stdout.flush()
            sys.stderr.flush()

        print(df.info())
        df.to_csv(Path(cwd) /f'poi_weekly_pop_added_{latest_date}.csv')
        s3.Bucket('recovery-data-partnership').upload_file(str(Path(cwd) / f'poi_weekly_pop_added_{latest_date}.csv'), f"output/dev/parks/safegraph-with-population/poi_with_population_count_{latest_date}.csv")
        sys.stdout.flush()
        sys.stderr.flush()
        df_ans = pd.read_csv(f'poi_weekly_pop_added_{latest_date}.csv')
        #print(df_ans.info())
        #print(df_ans.head(20))

        #Extract parks data
        parks_poi_df = pd.read_csv(Path(cwd) / 'recipes' / 'nyc_parks_pois_keys_082021.csv')
        df_ans['placekey'] = df_ans['placekey'].astype(str)
        parks_poi_df['placekey'] = parks_poi_df['placekey'].astype(str)


        #df_parks = df_ans.join(parks_poi_df, on='placekey', how='right')
        df_parks = pd.merge(parks_poi_df, df_ans, how='left', on='placekey')
        print(df_parks.head(6))

        print('saving parks slice csv')
        df_parks.to_csv(f'parks_slice_poi_{latest_date}.csv')
        s3.Bucket('recovery-data-partnership').upload_file(str(Path(cwd) / f'parks_slice_poi_{latest_date}.csv'), f"output/dev/parks/parks-slice-poi/parks_slice_poi_{latest_date}.csv")


        if is_prod: #uncomment in production
            try:
                os.remove(Path(cwd)) / f'parks_slice_poi_{latest_date}.csv'
                os.remove(Path(cwd) / f'nyc_weekly_patterns_temp_{latest_date}.csv.zip')
                os.remove(Path(cwd) / f'multiplier_temp_{latest_date}.csv.zip')
                os.remove(Path(cwd) / f'poi_weekly_pop_added_{latest_date}.csv')
            except FileNotFoundError:
                print("file not found to remove")
            except PermissionError:
                print("couldn't remove because file is in use.")
        print("{} Successfully completed at {}".format(latest_date, datetime.now()))
        sys.stdout.flush()
        sys.stderr.flush()

#setup paralell processing:
if __name__=='__main__':
    log_to_stderr(logging.DEBUG)
    start_time = time.perf_counter()
    print("start time is {}".format(datetime.now()))
    date_query ='''
      SELECT DISTINCT(substr(date_range_start, 1, 10)) as date_range_start
      FROM hps_crawledhome_panel_summary_202107
      UNION
      SELECT DISTINCT(substr(date_range_start, 1, 10)) as date_range_start
      FROM hps_crawled22
      ORDER BY date_range_start DESC;
     '''
    #can only upload as a a zip file or _helper.aws will break
    output_date_path = f"output/dev/parks/latest_date.csv.zip"
    print("output_date_path: {}".format(output_date_path))
    #make sure to uncomment this in production.
    if is_prod:
        print('executing latest date query')
        aws.execute_query(query=date_query,
                      database="safegraph",
                      output=output_date_path)

    #run query on it and get CSV
    s3 = boto3.resource('s3')
    cwd = os.getcwd()
    s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/parks')
    print('downloading latest date')
    if is_prod:
        s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/latest_date.csv.zip', str(Path(cwd) / "latest_date.csv.zip"))

    print('reading latest date')

    dates_df = pd.read_csv(Path(cwd) / "latest_date.csv.zip")



    #removed the pool code
    dates_list = dates_df['date_range_start']
    cutoff_date = "2021-09-27"
    dates_list = [x for x in dates_list if x > cutoff_date]
    #get the latest date
    dates_list.sort(reverse=True)
    dates_list = [dates_list[0]]
    my_main(dates_list)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    time_string = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    print("elapsed time: {}".format(time_string))
