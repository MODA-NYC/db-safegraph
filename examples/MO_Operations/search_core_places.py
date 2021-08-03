###########
### If you have the name of the place but not the ID, this will help you find the ID. Note Safegraph Place IDs are deprecated and replaced with placekey id
###########


import os
from dateutil import rrule
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from pandasql import sqldf
import json
import copy
from os.path import join, dirname
from dotenv import load_dotenv
import numpy as np
from dateutil.relativedelta import relativedelta
import ast


#specific_beaches_path=Path(os.getenv('SPECIFIC_BEACHES'))

def search_for_specific_beaches():
    specific_beaches_path=Path('//*****/specific_beaches.csv')
    specific_beaches_df = pd.read_csv(specific_beaches_path)
    #read the file of beaches and get the ids. put it into a string for sql
    beach_search_str =""
    for index, row in specific_beaches_df.iterrows():
        beach_search_str = beach_search_str + ",'{}' ".format(row['safegraph_place_id'])
    beach_search_str = np.core.char.strip(beach_search_str, chars=',')
    pysqldf = lambda q: sqldf(q, globals())
    return pysqldf("SELECT * FROM safeGraph_csv WHERE safegraph_place_id IN ({});".format(beach_search_str))

def get_all_points():
    specific_beaches_path=Path('//*****/specific_beaches.csv')
    specific_beaches_df = pd.read_csv(specific_beaches_path)
    #read the file of beaches and get the ids. put it into a string for sql
    beach_search_str =""
    for index, row in specific_beaches_df.iterrows():
        beach_search_str = beach_search_str + ",'{}' ".format(row['safegraph_place_id'])
    beach_search_str = np.core.char.strip(beach_search_str, chars=',')
    pysqldf = lambda q: sqldf(q, globals())
    return pysqldf("SELECT * FROM safeGraph_csv WHERE safeGraph_csv.region = 'NY' AND (safeGraph_csv.city = 'Manhattan' or safeGraph_csv.city = 'Brooklyn' OR safeGraph_csv.city = 'Queens' OR safeGraph_csv.city = 'Staten Island' OR safeGraph_csv.city = 'Bronx' OR safeGraph_csv.city = 'New York' or safeGraph_csv.city='Long Island City' );".format(beach_search_str))

def search_all_files():
   
    root = Path('//*****/')
    #you have to use global variable because sqldf looks there for the table.
    print('part 1')
    global safeGraph_csv 
    safeGraph_csv = pd.read_csv(root / 'core_poi-part1.csv')
    results = search_for_specific_beaches()
    print("part 2")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part2.csv')
    results = results.append(search_for_specific_beaches(), ignore_index=True)
    print("part 3")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part3.csv' )
    results = results.append(search_for_specific_beaches(), ignore_index=True)
    print("part 4")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part4.csv')
    results = results.append(search_for_specific_beaches(), ignore_index=True)
    print("part 5")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part5.csv')
    results = results.append(search_for_specific_beaches(), ignore_index=True)
    results = results.sort_values('location_name')
    return results

def get_every_single_point():
   
    root = Path('//*****')
    #you have to use global variable because sqldf looks there for the table.
    print('part 1')
    global safeGraph_csv 
    safeGraph_csv = pd.read_csv(root / 'core_poi-part1.csv')
    results = get_all_points()
    print("part 2")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part2.csv')
    results = results.append(get_all_points(), ignore_index=True)
    print("part 3")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part3.csv' )
    results = results.append(get_all_points(), ignore_index=True)
    print("part 4")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part4.csv')
    results = results.append(get_all_points(), ignore_index=True)
    print("part 5")
    safeGraph_csv = pd.read_csv(root / 'core_poi-part5.csv')
    results = results.append(get_all_points(), ignore_index=True)
    results = results.sort_values('location_name')
    return results
core_place_path = Path('//*****')

#results = search_all_files()
#results.to_csv(core_place_path / 'core_place_search_results.csv')
results = get_every_single_point()
print("writing...")
results.to_csv(core_place_path / 'every_core_POI.csv')
print("done")