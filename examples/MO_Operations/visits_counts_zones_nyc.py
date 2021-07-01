##########
### Visits to designated employment zones.
##########

#imports
import datetime
import dotenv
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import os
from pandasql import sqldf
import json
from functools import reduce
import copy
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point
import numpy as np



load_dotenv()
def main():
    #load env variables
    date_list_path = Path(os.getenv('DATE_LIST'))
    dcp_zones_path = Path(os.getenv('DCP_ZONES_PATH'))
    monthly_patterns_path = Path(os.getenv('MONTHLY_PATTERNS_PATH'))
    core_places_path = Path(os.getenv('CORE_PLACES_PATH'))
    #import date from csv
    print("reading date list")
    dates_df = pd.read_csv(date_list_path, dtype='object')
    #print("date list:")

    #import shapefile
    print("reading dcp shapefile")
    employment_zones = gpd.read_file(dcp_zones_path)
    #print(dcp_zones.head())
    
   
    #loop goes here:
    for i in range(0, len(dates_df)):
        #load data from csvs, filtering by NYC, combine into single df
        this_date = dates_df.iloc[i, :]
        monthly_patterns_df = load_monthly_patterns(this_date, monthly_patterns_path)
        #print(monthly_patterns_df.info())
        core_places_df = load_core_places(this_date, core_places_path)
        #create point file out of core places and monthly patterns.
        #print("core places info: {}".format(core_places_df.info()))
        #print("monthly_patterns info:{}".format(monthly_patterns_df.info()))
        core_and_pattern_gdf = join_core_and_monthly_patterns(core_places_df, monthly_patterns_df)
        #drop extra columns
        core_and_pattern_gdf = core_and_pattern_gdf[['geometry', 'longitude', 'latitude',  'raw_visit_counts',]]
        #print(core_and_pattern_gdf.info())
        #print(core_and_pattern_gdf.head())
        #clear memory
        core_places_df = None
        monthly_patterns_df = None
        #join core places gdf with DCP shapefile
        employment_zones = employment_zones[['HubName', 'geometry']]

        #test output
        #employment_zones.to_file(Path('//CHGOLDFS/Operations/DEV_Team/GIS/employment_zones_test.shp'))
        #core_and_pattern_gdf.to_file(Path('//CHGOLDFS/Operations/DEV_Team/GIS/candpgdf_test.shp'))

        print("spatial join...")
        #could you flip it around and keep points instead of polygons? Although honestly you aren't keeping either.
        gdf = gpd.sjoin(employment_zones, core_and_pattern_gdf, how='left')
        core_and_pattern_gdf = None
        df_pivot = pd.pivot_table(data=gdf, values='raw_visit_counts', index='HubName', aggfunc=np.sum)
        df_pivot=df_pivot.reset_index()
        
        #write as file
        filename = "raw_visits_to_employment_zones_{}_{}.csv".format(this_date['Year'], this_date['Month'])
        print('writing {}'.format(filename))
        df_pivot.to_csv(Path('//*****/visits_employment_zones/{}'.format(filename)))
        #make new geometry
        #df_poly_new = employment_zones.merge(df_pivot, how='left', on='HubName')
        #df_poly_new.to_file(Path('//*****/patterns_zones_aggregation.shp'))

        #print(df_poly_new.info())
        #print(df_poly_new.head(10))

    #create column for broad naics catagory

    #For group by:
        #zone
        #total
            #count visits, count rows, calculate visitors/row
    #calculate multiplier with home pannel and population estimate
    #create population counts.
    #pass

#def get_date_from_csv(date_list):
#    print('reading date list')
#    return pd.read_csv(date_list)

def load_monthly_patterns(date_row, monthly_patterns_path):
    directory = monthly_patterns_path / date_row['Year'] / date_row['Month'] / date_row['Day'] / date_row['Hour']
    print(str(date_row['Year']) + '-' + str(date_row['Month']))
    print('reading patterns-part1.csv')
    df = read_and_filter_monthly_patterns(directory, 'patterns-part1.csv')
    print('reading patterns-part2.csv')
    df = df.append(read_and_filter_monthly_patterns(directory, 'patterns-part2.csv'))
    print('reading patterns-part3.csv')
    df = df.append(read_and_filter_monthly_patterns(directory, 'patterns-part3.csv'))
    print('reading patterns-part4.csv')
    df = df.append(read_and_filter_monthly_patterns(directory, 'patterns-part4.csv'))
    return df

def read_and_filter_monthly_patterns(directory, filename):
    #print("reading monthly pattern {}".format(filename))
    df = pd.read_csv(directory / filename)
    nyc_county_fips = [36005, 36047, 36061, 36081, 36085]
    filter_list1 = df['poi_cbg'].astype(str).str[:5]
    nyc_counties_str = list(map(str, nyc_county_fips))
    filter_list2 = filter_list1.isin(nyc_counties_str).tolist()
    return df.loc[filter_list2]

def load_core_places(date_row, core_places_path):
    print('reading core_poi-part1.csv')
    directory = core_places_path / date_row['Year'] / date_row['Month'] 
    df = read_and_filter_core_places(directory, 'core_poi-part1.csv')
    print('reading core_poi-part2.csv')
    df = df.append(read_and_filter_core_places(directory, 'core_poi-part2.csv'))
    print('reading core_poi-part3.csv')
    df = df.append(read_and_filter_core_places(directory, 'core_poi-part3.csv'))
    print('reading core_poi-part4.csv')
    df = df.append(read_and_filter_core_places(directory, 'core_poi-part4.csv'))
    print('reading core_poi-part5.csv')
    df = df.append(read_and_filter_core_places(directory, 'core_poi-part5.csv'))
    return df

def read_and_filter_core_places(directory, filename):
    return pd.read_csv(directory / filename)
    #print(df.info())

def join_core_and_monthly_patterns(core_places_df, monthly_patterns_df):
    #create the monthly patterns and core places into a point-based geodataframe
    df = pd.merge(core_places_df, monthly_patterns_df, on='safegraph_place_id', how='inner')
    geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
    #return GeoDataFrame(df, crs="EPSG:2263", geometry=geometry)
    gdf = GeoDataFrame(df, crs="EPSG:4326", geometry=geometry)
    return gdf.to_crs(epsg='2263')
    #print(gdf.info())
    



if __name__ == "__main__":
    main()
