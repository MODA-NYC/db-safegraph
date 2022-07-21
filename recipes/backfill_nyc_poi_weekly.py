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

#need to execute this script on a local machine from the recipes directory as current working directory.
is_prod = True
n_cores = 2
#You need to be located inside the recipes folder
#loop through the dates.
def my_main(split_chunk):
    #this is the main function it takes the list of dates for that thread and saves a transformed output CSV with normalized counts
    #the counts have to be noramalized to convert device counts into population counts.
    s3 = boto3.resource('s3')
    cwd = os.getcwd()
    s3_obj = s3.Bucket('recovery-data-partnership').Object('output/dev/parks')

    for my_date in split_chunk:
        #takes a date in the list.
        latest_date = my_date
        warnings.warn('for date: "{}"'.format(latest_date))
        #queries weekly patterns for the given start date. The start dates are every week.
        #All the POIs for that time period have the same start date.
        #the data is stored in a AWS Glue S3 Data Cataglog.
        query = '''
        SELECT * FROM weekly_patterns_202107
        WHERE substr(date_range_start, 1, 10) = '{}'
        AND substr(poi_cbg, 1, 5) in ('36005', '36047', '36061', '36081', '36085');

        '''.format(latest_date)
        #executes query for the date
        output_csv_path_2 = 'output/dev/parks/nyc-weekly-patterns/nyc_weekly_patterns_{}.csv.zip'.format(latest_date)
        if is_prod:
            aws.execute_query(
                query=query,
                database="safegraph",
                output=output_csv_path_2
            )


            s3.Bucket('recovery-data-partnership').download_file('output/dev/parks/nyc-weekly-patterns/nyc_weekly_patterns_{}.csv.zip'.format(latest_date), str(Path(cwd) / f'nyc_weekly_patterns_temp_{latest_date}.csv.zip'))

        ##### get multiplier #####
        #This query joins the census data (census) to the home panel summary (hps). It is a union between two different date ranges but it looks like it uses the same census data
        #ideally you would union each year home panel summary to each year census.
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
        #and not filter by NYC
        
        #xecutes the census/hps query and saves the file.
        output_csv_path = f'output/dev/parks/safegraph-with-population/multipliers/pop_to_device_multiplier_{latest_date}.csv.zip'
        if is_prod:
            aws.execute_query(
                query=query,
                database="safegraph",
                output=output_csv_path
            )
        #downoads the census/hps query as multiplier temp
        s3.Bucket('recovery-data-partnership').download_file(f'output/dev/parks/safegraph-with-population/multipliers/pop_to_device_multiplier_{latest_date}.csv.zip', str(Path(cwd) / f'multiplier_temp_{latest_date}.csv.zip'))

        #filters out multiplier temp to NYC and names it df_mult
        df_mult = pd.read_csv(Path(cwd) / f'multiplier_temp_{latest_date}.csv.zip', dtype={'cbg': object})
        is_in_nyc = [True if row[:5] in ['36005', '36047', '36061', '36081', '36085'] else False for row in df_mult['cbg']]
        #filters home panel/census data to nyc counties (but I said we don't want to do that?)
        #instead of filtering the home pannel summary, consider filtering weekly patterns. But this would be a massive performance hit. (Order n^2)
        df_mult_nyc = df_mult[is_in_nyc]
        #want to reset index because you just took a slice.
        df_mult_nyc.reset_index()

        #you will loop through and sum up the population and devices residing, weighted by the number of visitors
        #this calculates the citywide multiplier for the entire city. It is called macro_multiplier and is used when data is not available.
        sum_pop = df_mult_nyc['cbg_pop'].sum()
        sum_dc = df_mult_nyc['devices_residing'].sum()
        macro_multiplier = sum_pop / (sum_dc * 1.0)
        
        ##### join census to cbg to weekly patterns and find multiplier by row #####
        df = pd.read_csv(Path(cwd) / f'nyc_weekly_patterns_temp_{latest_date}.csv.zip' )
        # for each POI row in the dataframe
        if ((len(df) == 0) or (len(df_mult) == 0)) :
            warnings.warn(f"{latest_date} Either the home panel summary or the weekly patterns were not found")
            continue

        multiplier_list = []
        sys.stdout.flush()
        sys.stderr.flush()
        if is_prod:
            #iterate over all the POIs
            for index, row in df.iterrows():
                iter = 0

                #for each home block group (key) in key in the visitor_home_cbg field, the program:
                #will take the population of the home block group multiplied by the devices seen
                sum_cbg_pop = 0
                #will take the devices of the home block group (key) multiplied by the devices seen
                sum_cbg_devices = 0

                this_json = json.loads(row['visitor_home_cbgs'])
                #for each home census block group (key) in the dictionary 
                for key, value in this_json.items():
                    #multiply devices by people per device table. look up form the home pannel summary/census table (df_mult) the current cbg (key)
                    iter = iter + 1
                    selected_rows = df_mult.iloc[:, df_mult.columns.get_loc('cbg')] == key
                    #filter df_mult to just the current cbg.
                    selected_rows_mult_df = df_mult[selected_rows]
                    #isolate multiplier
                    try:
                        #take the first row from the filtered census/home panel summary table. should only be one match. 
                        if len(selected_rows_mult_df[selected_rows_mult_df['cbg'] == key]) > 1:
                            warning_message = "more than one match for key {}".format(key)
                            warnings.warn(warning_message)
                        
                        #get the population from the census/hps row
                        cbg_pop = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('cbg_pop')]
                        # need to multiply by the number of visitors to get a weighted average
                        cbg_pop = cbg_pop * value

                        #get devices residing from the census/hps table
                        devices_residing = selected_rows_mult_df.iloc[0, selected_rows_mult_df.columns.get_loc('devices_residing')]
                        #multiply number devices by the visitors to weight the average
                        devices_residing = devices_residing * value



                    except IndexError:

                        warning_message = 'warning: there is no row zero for key {}'.format(key)
                        #warnings.warn(warning_message)
                        #if no multiplier, pop_count stays the same. They do not go into computing the weighted average. 
                        #multiplier = 0
                        cbg_pop = 0
                        devices_residing = 0
                    #this is done below. see synthtetic mult.
                    #add this key's population and devices to the running tally.
                    sum_cbg_pop = sum_cbg_pop + cbg_pop
                    sum_cbg_devices = sum_cbg_devices + devices_residing
                    #print any errors immediately.
                    sys.stdout.flush()
                    sys.stderr.flush()
                # to fill in the missing values outside the NYC census/home pannel summary, including international, 
                # calculates a multiplier from they home pannel summary keys the program did find.
                # Don't divide by zero! (for small visit counts without home block groups)
                no_zero_lambda_func = (lambda x : x if x > 0 else 1)
                synthetic_mult = sum_cbg_pop / no_zero_lambda_func(sum_cbg_devices * 1.0)
                sys.stdout.flush()
                sys.stderr.flush()
                # add multiplier to a series. At the end you will have a series the length of the table that the program
                # will append to create a column of multipliers.
                multiplier_list.append(synthetic_mult)
                #print("final pop count: {}".format(pop_count))
            # all POIs have been looped through
            
            #fill null or zero multipliers with imputed citywide macro_multiplier. 
            multiplier_list = [x if x > 0 else macro_multiplier for x in multiplier_list]
            # this creates the multiplier_list. This list is multiplied by the column to convert from devices to population.  

            #take the average of all the calculated multipliers and compare to the citywide macro_multiplier for diagnostics.
            non_zero_multipliers =  [x for x in multiplier_list if x > 0]
            avg_multiplier = np.mean(non_zero_multipliers)
            print(f"avg_multiplier: {avg_multiplier}, macro_multiplier:{macro_multiplier}")

            #save the calculated multiplier as a column
            df['pop_multiplier'] = multiplier_list
            #convert device counts to population counts based on multiplier series.
            df['visits_pop_calc'] = multiplier_list * df['raw_visit_counts']
            df['visitors_pop_calc'] = multiplier_list * df['raw_visitor_counts']
            #it is a bit more complex to multiply a list
            df['visits_by_day_pop_calc'] = None
            df['visits_by_hour_pop_calc'] = None
            df_copy = df.copy()
            # the values in day and hour are a list of integers. The program multiplies the entire list by making an array the length of the list.
            def multiply_list(by_day_list, multiplier_list):
                visits_by_day = [float(x) * 1.0 for x in literal_eval(by_day_list)]
                this_multiplier = multiplier_list[index]
                #create the matricies and multiply
                return list(np.multiply(visits_by_day, (np.repeat(this_multiplier * 1.0, len(visits_by_day)))))
                
            for index, row in df_copy.iterrows():
                #multiply the lists by the multiplier.
                df.at[index, 'visits_by_day_pop_calc' ] = multiply_list(row['visits_by_day'], multiplier_list)
                df.at[index, 'visits_by_hour_pop_calc'] = multiply_list(row['visits_by_each_hour'], multiplier_list)
            #garbage collection
            df_copy = None
            warnings.info(str(df.head(5)))
            sys.stdout.flush()
            sys.stderr.flush()
        #multiplier calculation complete

        #upload results for this date.
        df.to_csv(Path(cwd) /f'poi_weekly_pop_added_{latest_date}.csv')
        s3.Bucket('recovery-data-partnership').upload_file(str(Path(cwd) / f'poi_weekly_pop_added_{latest_date}.csv'), f"output/dev/parks/safegraph-with-population/poi_with_population_count_{latest_date}.csv")
        sys.stdout.flush()
        sys.stderr.flush()
        df_ans = pd.read_csv(f'poi_weekly_pop_added_{latest_date}.csv')
        #print(df_ans.info())
        #print(df_ans.head(20))

        #Extract parks data filtered by parks poi keys.
        parks_poi_df = pd.read_csv('nyc_parks_pois_keys_082021.csv')
        df_ans['placekey'] = df_ans['placekey'].astype(str)
        parks_poi_df['placekey'] = parks_poi_df['placekey'].astype(str)


        #df_parks = df_ans.join(parks_poi_df, on='placekey', how='right')
        df_parks = pd.merge(parks_poi_df, df_ans, how='left', on='placekey')
        print(df_parks.head(6))

        print('saving parks slice csv')
        df_parks.to_csv(f'parks_slice_poi_{latest_date}.csv')
        s3.Bucket('recovery-data-partnership').upload_file(str(Path(cwd) / f'parks_slice_poi_{latest_date}.csv'), f"output/dev/parks/parks-slice-poi/parks_slice_poi_{latest_date}.csv")


        if is_prod: #clean up temp files.
            try:
                os.remove(Path(cwd) / f'parks_slice_poi_{latest_date}.csv')
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
        #proceed to the next date in the batch
    #batch for this worker complete.

#setup paralell processing:
if __name__=='__main__':
    log_to_stderr(logging.DEBUG)

    start_time = time.perf_counter()
    print("start time is {}".format(datetime.now()))
    #query the available dates from the home panel summary
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
    #execute the date query
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



    #date_list_split = np.array_split(dates_df, n_cores)
    #split list of dates in such a way that they are all working on the same dates and increase lowest to high.
    dates_list = dates_df['date_range_start']
    #filter dates
    #cutoff_date = "2021-09-27"
    cutoff_date = "2021-10-01"
    max_date = "2021-11-01"
    dates_list = [x for x in dates_list if (x > cutoff_date and x < max_date)]
    def form_lists(n_cores, list):
        i = 0
        c = 0
        ans = [ [] for n in range(n_cores)]

        while i < len(list):
            ans[ c % n_cores].append(list[i])
            c = c + 1
            i = i + 1
        return [np.array(x) for x in ans]
    datetime_list = [x[:10] for x in dates_list]
    date_list_split = form_lists(n_cores, dates_list)
    print(date_list_split)

    #create a multithreaded pool of threads
    pool = Pool(n_cores)
    #execute the main function in the pool, based on the split lists of dates. Go to main
    return_series = pd.concat(pool.map(my_main, date_list_split))
    #all dates on all batches complete.
    pool.close()
    pool.join()
    #I am not sure what the return series is. What the pool returns when it is complete.
    print(f"return series: {return_series}")
    #prints runtimes
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    time_string = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    print("elapsed time: {}".format(time_string))
