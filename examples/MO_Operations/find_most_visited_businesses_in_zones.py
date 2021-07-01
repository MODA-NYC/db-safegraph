##########
### A list of most frequented businesses confined to NYS-designated hot spots
##########

import pandas as pd
from pathlib import Path
import numpy as np
from datetime import datetime
start_year=2020
end_year=2020
start_week=43
end_week=43

start_week_string= str(start_year) + '-' + str(start_week)
end_week_string = str(end_year) + '-' + str(end_week)


poi_info_path = Path('//*****/poi_info.csv')
poi = pd.read_csv(poi_info_path)
#print(poi.head())


#print(poi['latitude'])
poi = poi[['safegraph_place_id', 'naics_code', 'latitude', 'longitude', 'area_square_feet', 'zonename', 'zonecolor']]
#print(poi.info())
poi = poi.copy()
#print(poi.info())

poi_visits_path = Path('//*****/weekly_nyc_poivisits_2020Q4.csv')
poi_visits = pd.read_csv(poi_visits_path)





df = poi.merge(poi_visits, on='safegraph_place_id', how='inner')
#df = pd.concat([poi, poi_visits], ignore_index=True, keys='safegraph_place_id')
#print(df.info())

'''
#some pandasql syntax in case you need it
from pandasql import sqldf
q="""SELECT DISTINCT year_week  FROM df;"""
pysqldf = lambda q: sqldf(q, globals())
a_df = pysqldf(q)
print(a_df)
'''
zonename_list = ['Brooklyn', 'Flushing', 'Rockaway']
zonecolor_list = ['Yellow', 'Orange', 'Red']

#find the multiplier
home_pannel_summary_path = Path('//*****/weekly_nyc_summary.csv')
home_pannel_summary = pd.read_csv(home_pannel_summary_path)
home_pannel_summary = home_pannel_summary.loc[home_pannel_summary['year_week'].ge(start_week_string) & home_pannel_summary['year_week'].le(end_week_string)]
device_count = home_pannel_summary['number_devices_residing'].mean()
population = 8399000.0
multiplier = population / device_count
print("type home_pannel_summary: {}".format(type(home_pannel_summary)))
print(home_pannel_summary.head())
print("population: {}, device_count: {}, multiplier: {}".format(population, device_count, multiplier))
if np.isnan(multiplier):
    raise Exception("multiplier is not a number")


#trim the date range
df = df.loc[df['year_week'].ge(start_week_string) & df['year_week'].le(end_week_string)]

#calculate density
df['visits_per_body_space_per_hour'] = ((df['visits_total']  / 168.0) / (df['area_square_feet'])) * 113
df['pop_per_body_space_per_hour'] = df['visits_per_body_space_per_hour'] * multiplier
df['max_visits_per_hour_pop'] = df['max_visits_per_hour'] * multiplier
df['max_visits_density'] = ((df['max_visits_per_hour']) / (df['area_square_feet'])) * 113
df['max_pop_density'] = df['max_visits_density'] * multiplier
df['visits_total_pop'] = df['visits_total'] * multiplier
df['visitors_total_pop'] = df['visitors_total'] * multiplier
df['modified_on'] = str(datetime.now())




#Sort the list
#can't sort until after grouping
#df = df.sort_values('visits_per_body_space_per_hour', axis=0, ascending=False, na_position='last')
#print(df.head(50))



root = Path('//*****')
poi2 = poi_visits[['safegraph_place_id', 'poi']]


def output_layer(this_df, zn, zc, opt_name=None):     
    #Aggregate
    this_df = this_df.groupby(['safegraph_place_id']).mean()

    #then sort
    this_df = this_df.sort_values('max_visits_density', axis=0, ascending=False, na_position='last')

    #this_df = this_df[:100]

    #add back in place names
    this_df = this_df.merge(poi2, on='safegraph_place_id')

    #drop duplicates
    this_df = this_df.drop_duplicates()

    #sort again?
    this_df = this_df.sort_values('max_visits_density', axis=0, ascending=False, na_position='last')

    #write file
    filename = '{}-{}_to_{}-{}-visitation_in_NYS_hotspot_{}-{}_with_pop.csv'.format(start_year, start_week, end_year, end_week, zn, zc)
    if(opt_name):
        filename = opt_name
    print("writing {}".format(filename))
    this_df.to_csv(root / filename)
   



for zn in zonename_list:
    for zc in zonecolor_list:
        this_df = df.loc[df['zonename'].eq(zn) & (df['zonecolor'].eq(zc))]
        #this_df = df[np.where((df['zonename'] == i) & (df['zonecolor'] == j))]
        #max_value = this_df['max_visits_density'].max()
        #aggregate
        #Aggregate
        output_layer(this_df, zn, zc)

output_layer(df, 'all', 'all')
print(df.head())

output_layer(df, '', '', "visitation_all_recent_run_carto.csv")
