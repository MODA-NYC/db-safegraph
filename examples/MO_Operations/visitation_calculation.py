##########
### Find visitation at a specific hour.
##########
#imports
import datetime
import dotenv
from pathlib import Path
import pandas as pd
import os
from pandasql import sqldf
import json
from functools import reduce

dotenv.load_dotenv()
def main():
    nyc_county_fips = [36005, 36047, 36061, 36081, 36085]
    #find 2019 date 7 pm
    YEAR = int(os.getenv('YEAR'))
    MONTH = int(os.getenv('MONTH'))
    DAY = int(os.getenv('DAY'))
    TIME = int(os.getenv('TIME'))
    LAST_YEAR = int(os.getenv('LAST_YEAR'))
    LAST_MONTH = int(os.getenv('LAST_MONTH'))
    LAST_DAY = int(os.getenv('LAST_DAY'))
    MARCH_DAY = int(os.getenv('MARCH_DAY'))
    this_date = datetime.datetime(YEAR, MONTH, DAY, TIME, 0, 0,0 )
    last_year_date = datetime.datetime(LAST_YEAR, LAST_MONTH, LAST_DAY, TIME, 0, 0, 0)
    march_date = datetime.datetime(2020, 4, MARCH_DAY, TIME, 0, 0 ,0)
    #create dto for
    current_dto = week_dto(this_date, nyc_county_fips)
    march_dto = week_dto(march_date, nyc_county_fips)
    last_year_dto = week_dto(last_year_date, nyc_county_fips)
    #load core places
    core_places_df = load_core_places()
    #get the visitation for each place at a specific hour (7:00 Saturday)
    #my_hour = 139
    #set in the .env file
    my_hour = this_date.weekday() * 24 + this_date.hour
    current_dto.get_visits_specific_hour(my_hour)
    march_dto.get_visits_specific_hour(my_hour)
    last_year_dto.get_visits_specific_hour(my_hour)
    #get density  (to do)
        #join sqft to df on sgid
        #divide visit by sqft to get density

    #filter core places with current places
    filtered_current_places = core_patterns_inner_join(core_places_df, current_dto)  
    filtered_march_places = core_patterns_inner_join(core_places_df, march_dto)
    filtered_last_year_places = core_patterns_inner_join(core_places_df, last_year_dto)
    #print(filtered_core_places.info())

    #massive_join = pd.merge(filtered_current_places, filtered_march_places, on='safegraph_place_id', how='inner')
    #massive_join = pd.merge(massive_join, filtered_march_places, on='safegraph_place_id', how='inner')
    
    #need to clear memory
    core_places_df = None
    dfs = [filtered_current_places, filtered_march_places, filtered_last_year_places]
    massive_join = reduce(lambda left, right: pd.merge(left, right, on='safegraph_place_id', how='inner'), dfs)
    for col in massive_join.columns:
       print(col)
    #dfs = [df.set_index('safegraph_place_id') for df in dfs]
    #dfs[0].join(dfs[1:])
    visits_pop_current_str = 'visits_at_hour_pop_{}_{}_{}_{}'.format(current_dto.my_year, current_dto.my_month, current_dto.my_day, my_hour)
    visits_pop_march_str = 'visits_at_hour_pop_{}_{}_{}_{}'.format(march_dto.my_year, march_dto.my_month, march_dto.my_day, my_hour)
    visits_pop_last_year_str = 'visits_at_hour_pop_{}_{}_{}_{}'.format(last_year_dto.my_year, last_year_dto.my_month, last_year_dto.my_day, my_hour)
    #all these xs and ys are caused by the three-way join
    pruned_df = massive_join[['safegraph_place_id', 'location_name_x', 'street_address_y_y', 'city_x_x', 'naics_code_x', 'poi_cbg', 'latitude_x', 'longitude_x', visits_pop_current_str, visits_pop_march_str, visits_pop_last_year_str ]]
    pruned_df['location_name'] = pruned_df['location_name_x']
    pruned_df['street_address'] = pruned_df['street_address_y_y']
    pruned_df['city'] = pruned_df['city_x_x']
    pruned_df['naics_code'] = pruned_df['naics_code_x']
    pruned_df['latitude'] = pruned_df['latitude_x']
    pruned_df['longitude'] = pruned_df['longitude_x']
    pruned_df = pruned_df.drop(['location_name_x', 'street_address_y_y', 'city_x_x', 'naics_code_x', 'latitude_x', 'longitude_x'],  axis=1)
    #for this month's data find the percent recovery ((current - march) / (2019 - march)), 2019 visits (pop), march visits(pop), current visits(pop), current(pop) density
    pruned_df['percent_recovered'] = (pruned_df[visits_pop_current_str] - pruned_df[visits_pop_march_str]) / (pruned_df[visits_pop_last_year_str] - pruned_df[visits_pop_march_str] + 1)
    #print(pruned_df['percent_recovered'][0:50])
        
    #create a new df.
    #save this raw file.
    OUTPUT_PATH = os.getenv('OUTPUT_PATH')
    filename = 'visitation_{}_{}_{}_{}.csv'.format(YEAR, MONTH, DAY, TIME)
    pruned_df.to_csv(Path(OUTPUT_PATH + filename))

    #to do
    #aggregate all the fields by block group (group by)
    #open NYC geometry
    #create a geodataframe joining the NYC BG geometry to the aggregated dataframe (done in carto now)
    #(manual)save the geodataframe as a shapefile (not necessary)
    #upload geodataframe to Carto
class week_dto:
      # a class to hold all data (given date, core data, nyc block groups)

            #query weekly patterns
            #query footprints
            #query home panel
        #return a DTO  
    def __init__(self, my_date, nyc_counties):
        self.my_date = my_date
        #self.nyc_counties = nyc_counties
        #find monday of that week to search for weekly files.
        self.monday = self.find_monday(my_date)
        #create the  weekly patterns dataframe wp_df that has been filtered to county.
        self.wp_df = self.get_weekly_patterns(self.monday, nyc_counties)
        #get the multiplier for that week
        self.multiplier = self.lookup_multiplier(self.monday, nyc_counties)

    def get_weekly_patterns(self, monday, nyc_counties):
        self.my_year = self.monday.strftime('%Y')
        self.my_month = self.monday.strftime('%m')
        self.my_day = self.monday.strftime('%d')
        
        #query by select all core data sgid that start with the five county codes
        #build path
        self.weekly_patterns_path = os.getenv('WEEKLY_PATTERNS_PATH')
        self.weekly_patterns_filename = '{0}-{1}-{2}-weekly-patterns.csv'.format(self.my_year, self.my_month, self.my_day)
       
        #open_weekly_patterns
        print('reading_weekly_patterns')
        self.wp_df_raw= pd.read_csv(Path(self.weekly_patterns_path + '/' + self.weekly_patterns_filename), nrows=None)
        #filter by county
        print('filtering weekly patterns')
        self.filter_list1 = self.wp_df_raw['poi_cbg'].astype(str).str[:5]
        self.nyc_counties_str = list(map(str, nyc_counties))
        self.filter_list2 = self.filter_list1.isin(self.nyc_counties_str).tolist()
        self.wp_df = self.wp_df_raw.loc[self.filter_list2]
        
        #clear the memory
        wp_df_raw = None
        
        return self.wp_df

    def lookup_multiplier(self, monday, nyc_counties):
        #calculates the devices per population
        #parse monday of that week
        self.my_year = self.monday.strftime('%Y')
        self.my_month = self.monday.strftime('%m')
        self.my_day = self.monday.strftime('%d')
        
        #build path
        self.home_summary_path = os.getenv('HOME_SUMMARY_PATH')
        self.home_summary_filename = '{}-{}-{}-home-panel-summary.csv'.format(self.my_year, self.my_month, self.my_day)
        '''found multipliers previously
        01/2019 7.887170034958399 
        06/2019 7.681584843610387
        01/2020 6.982711310647716
        04/2020 11.80717926115546
        There is a population jump so I will use a constant of january 2020 multiplier for 2020 and the June 2019 value for 2019
        '''
        if self.my_year == '2019':
            return 7.681584843610387
        if self.my_year == '2020':
            return 6.982711310647716

    def find_monday(self, my_date):
        monday = my_date - datetime.timedelta(days=my_date.weekday())
        return monday
    
    def get_visits_specific_hour(self, my_hour):
        #takes the weighted average of the hour and the two adjacent hours.
        #self.visit_series = self.wp_df['visits_by_each_hour']
        self.wp_df['visits_by_each_hour'] = self.wp_df['visits_by_each_hour'].apply(lambda x: json.loads(x))
        visits_at_hour_string = 'visits_at_hour_{}_{}_{}_{}'.format(self.my_year, self.my_month, self.my_day, my_hour)
        self.wp_df[visits_at_hour_string] = self.wp_df['visits_by_each_hour'].apply(lambda x: (2.0 * float(x[my_hour]) + float(x[my_hour + 1]) + float(x[my_hour - 1])) / 4.0)
        #print(self.wp_df['visits_at_hour']) 
        self.wp_df['visits_at_hour_pop_{}_{}_{}_{}'.format(self.my_year, self.my_month, self.my_day, my_hour)] = self.wp_df[visits_at_hour_string] * self.multiplier
        #print(self.wp_df['visits_at_hour_pop'])

def load_core_places():
    print("loading core places")
    CORE_PLACES_PATH = os.getenv('CORE_PLACES_PATH')
    print("part 1")
    df = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part1.csv'), nrows=None)
    print("part 2")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part2.csv'), nrows=None)
    df = df.append(temp, ignore_index=True)
    print("part 3")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part3.csv'), nrows=None)
    df = df.append(temp, ignore_index=True)
    print("part 4")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part4.csv'), nrows=None)
    df = df.append(temp, ignore_index=True)
    print("part 5")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part5.csv'))
    df = df.append(temp, ignore_index=True)
    return df

def core_patterns_inner_join(core_places_df, current_dto):
    return pd.merge(core_places_df, current_dto.wp_df, how='inner', on='safegraph_place_id')

if __name__ == "__main__":
    main()