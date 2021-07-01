###########
### An attempt to use device counts to determine population.
##########


import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
from datetime import date, datetime
import copy
import fnmatch
from dateutil.rrule import rrule, MONTHLY



def main():
    #read actual US population table
    real_us_population = pd.read_csv(Path('//*****/us_population.csv'))
    #load NYC zipcodes as geometry
    nyc_zcta = gpd.read_file(Path('//*****/NYC_ZIP_Codes.shp'))
    #load NYC block groups
    nyc_block_groups = gpd.read_file(Path('//*****/nyc_block_groups_clean.shp'))



    #create an empty point geodataframe. this is the master list.
    master_points = None
    #find the centroids of the zip codes (this will be reused)
    centroids= nyc_zcta.centroid
  
  
    centroids_gdf = gpd.GeoDataFrame()
    centroids_gdf.crs="EPSG:4269"
    centroids_gdf['geometry'] = centroids
    #print("centroids info:")
    #print(centroids_gdf.info())
    february_datetime = datetime(2020, 2, 1)
    february_tuple = find_pop_zip_for_month(february_datetime, real_us_population, nyc_zcta, nyc_block_groups, centroids_gdf)
    february_points = february_tuple[0]
    february_points['february_pop'] = february_points['pop']
    february_poly = february_tuple[1]
    master_points = february_points
    master_points['date'] = str(february_datetime)
    
    #iterate over months
    save_path = Path('//*****/zip_code_population')
    for m in month_iter(6, 2019, 6, 2020):
        this_month_datetime = datetime(m[1], m[0], 1)
        this_month_tuple = find_pop_zip_for_month(this_month_datetime, real_us_population, nyc_zcta, nyc_block_groups, centroids_gdf)
        this_month_points = this_month_tuple[0]
        #this_month_poly = this_month_tuple[1]
        this_month_points = add_february_calculations(this_month_points, february_poly)
        this_month_points['date'] = str(this_month_datetime)
        this_month_points.to_file(save_path/ '{}-{}_population_by_zip.shp'.format(m[1], m[0]))
        if (m[1] != 2020 or m[0] != 2):
            master_points = master_points.append(this_month_points)
    
    save_path = Path('//*****/shapefiles')
    master_points.to_file(save_path / "population_time_series_zip.geojson", driver='GeoJSON')
    master_points['date'] = master_points['date'].astype(str)
    master_points.to_file(save_path / "population_time_series_points.shp")
    #Upload master points geodataframe to Carto as time series.


def find_pop_zip_for_month(this_month_datetime, real_us_population, nyc_zcta, nyc_block_groups, centroids):
#for every month (start with february. you will have to make two paths for before and after May):
        
        this_month_pop = get_this_month_pop(this_month_datetime, real_us_population)
        #read home summary table
        home_summary = read_home_summary(this_month_datetime)
        #print("home summary info: ")
        #print(home_summary.head())
        #find the US population multiplier for a given month by dividing population by device count (exclude outlying areas? Make sure they match)
        us_device_count = home_summary['number_devices_residing'].sum()
        multiplier = 1.0 * this_month_pop / us_device_count
        #print("multiplier: {}".format(multiplier))
        #spatial aggregate NYC visits into zip code
        #append device count to census tract
        home_summary['GEOID'] = home_summary['census_block_group'].astype(str)
        nyc_block_groups = nyc_block_groups.merge(home_summary, how='inner', on='GEOID')
        #spatial join block groups to zcta
        #nyc_block_groups = gpd.sjoin(nyc_block_groups, nyc_zcta, how='inner' )
        nyc_block_groups = gpd.sjoin(nyc_zcta, nyc_block_groups, how="inner")
        nyc_block_groups = nyc_block_groups[['geometry', 'ZCTA5CE10', 'number_devices_residing']]
        #print("nyc block groups:")
        #print(nyc_block_groups.info())
        zip_code_counts = nyc_block_groups.dissolve(by='ZCTA5CE10', aggfunc='sum')
        
        zip_code_counts = zip_code_counts.reset_index()
        #print("zip code counts:")
        #print(zip_code_counts.info())

        #multiply zip code by multiplier to get population estimate.
        zip_code_counts['pop'] = zip_code_counts['number_devices_residing'] * multiplier
        #spatially join centroids with population 
        #print("zip_code_counts : {}".format(len(zip_code_counts)))

        this_month_centroids = gpd.sjoin(centroids, zip_code_counts, how="inner", op="intersects")
     
        #print("this_month_centroids")
        #print(this_month_centroids.head(30))
        #print("{}-{} centroids info".format(this_month_datetime.year, this_month_datetime.month))
        #print(this_month_centroids.info())
        #print("original centroids count: {}, this month centroids count: {}, zip_code_counts poly {}".format(len(centroids), len(this_month_centroids), len(zip_code_counts) ))
        return (this_month_centroids, zip_code_counts)
      

def add_february_calculations(this_month_points, february_poly):
    #february 2020 is the baseline to find percent change.
    #append february to points
    february_poly = february_poly[['geometry', 'pop']]
    february_poly = february_poly.copy()
    february_poly['february_pop'] = february_poly['pop']
    february_poly = february_poly.drop(columns=['pop'])

    this_month_points = this_month_points.drop(columns=['index_right'])
    this_month_points = gpd.sjoin(this_month_points, february_poly)
    
    #Normalize with February as 0 as an additional column (all zip codes should be zeroed to february) 
    #find difference from february in pop
    this_month_points['pop_change_feb'] = this_month_points['pop'] - this_month_points['february_pop']
    #calculate percent difference from february
    this_month_points['percent_change_february'] = ((this_month_points['pop'] - this_month_points['february_pop']) / (this_month_points['february_pop'] * 1.0)) * 100.0
    return this_month_points


def read_home_summary(this_month_datetime):
 
    this_path = Path('//*****/home-summary-file')
    this_month = str(this_month_datetime.month)
    if (len(this_month) == 1):
        this_month = "0" + this_month
    this_year = str(this_month_datetime.year)
    #read the filenames. Find one with the correct month. 
    files = os.listdir(this_path)
    year_string = "{}-{}-*".format(this_year, this_month)
    match_file = fnmatch.filter(files, year_string)[0]
    
    print("reading {}-{}".format(this_year, this_month))
    home_panel_summary = pd.read_csv(this_path / match_file)

    #print("done reading {}-{}".format(this_year, this_month))
    return home_panel_summary

def get_this_month_pop(this_datetime, real_us_population):
    this_year = str(this_datetime.year)
    this_month = str(this_datetime.month)
    search_string = this_datetime.strftime('%b-%y')
    #print(real_us_population.head())
    answer = int(real_us_population.loc[real_us_population['date'] == search_string, 'pop'] * 1000000)
    #print(answer)
    return answer

def month_iter(start_month, start_year, end_month, end_year):
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    return ((d.month, d.year) for d in rrule(MONTHLY, dtstart=start, until=end))

if __name__ == "__main__":
    main()