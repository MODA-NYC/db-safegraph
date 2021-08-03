##########
### finds hotspots filtered by on NAICS code. 
### Includes a calculation for density of visits using square footage data from the geo_supplement provided during the pandemic.
##########

#imports
import datetime
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import os
from pandasql import sqldf
import json
from functools import reduce
import copy

load_dotenv()
def main():
    try:
        NROWS = int(os.getenv('NROWS'))
    except ValueError:
        NROWS = None
    nyc_county_fips = [36005, 36047, 36061, 36081, 36085]
    #find 2019 date 7 pm
    #year month and day only used to set the file name. Actual date set by path
    YEAR = int(os.getenv('V2_YEAR'))
    MONTH = int(os.getenv('V2_MONTH'))
    DAY = int(os.getenv('V2_DAY'))
    TIME = int(os.getenv('V2_HOUR'))
    NAICS_CODE = os.getenv('NAICS_CODE') # keep as string
    this_date = datetime.datetime(YEAR, MONTH, DAY, TIME, 0, 0,0 )
    #create dto for
    current_dto = week_dto(this_date, nyc_county_fips, NROWS)
    #march_dto = week_dto(march_date, nyc_county_fips)
    #last_year_dto = week_dto(last_year_date, nyc_county_fips)
    #load core places
    core_places_df = load_core_places(NROWS)

    #filter by naics code
    core_places_df = filter_by_naics_code(core_places_df, 722)
    filtered_current_places = core_patterns_inner_join(core_places_df, current_dto)

    #create a new df.
    #save this raw file.
    OUTPUT_PATH = os.getenv('OUTPUT_PATH')
    filename = 'food_service_{}_{}_{}.csv'.format(YEAR, MONTH, DAY)
    
    filtered_current_places = filtered_current_places.drop(['parent_safegraph_place_id',
                                                            'safegraph_brand_ids_x',
                                                            'brands_x',
                                                            'iso_country_code',
                                                            'index',
                                                            'location_name_y',
                                                            'street_address_y',
                                                            'city_y',
                                                            'region_y',
                                                            'postal_code_y',
                                                            'iso_country_code_x',
                                                            'safegraph_brand_ids_y',
                                                            'brands_y',
                                                            'visits_by_each_hour',
                                                            'visitor_home_cbgs',
                                                            'visitor_daytime_cbgs',
                                                            'related_same_day_brand',
                                                            'related_same_week_brand',
                                                            'device_type',
                                                            'visits_by_each_hour_list',
                                                            'is_synthetic',
                                                            'iso_country_code_y',
                                                            ], axis=1)
    print("exporting...")
    filtered_current_places.to_csv(Path(OUTPUT_PATH +'/all_' + filename))
    top_count = int(os.getenv('TOP_COUNT'))
    save_top(filtered_current_places, 'wd_ln_density', top_count, Path(OUTPUT_PATH + '/' + 'weekday_late_night_top_{}_'.format(top_count) + filename))
    save_top(filtered_current_places, 'wd_m_density', top_count, Path(OUTPUT_PATH + '/' + 'weekday_morning_top_{}_'.format(top_count) + filename))
    save_top(filtered_current_places, 'wd_a_density', top_count, Path(OUTPUT_PATH + '/' + 'weekday_afternoon_top_{}_'.format(top_count) + filename))
    save_top(filtered_current_places, 'wd_e_density', top_count, Path(OUTPUT_PATH + '/' + 'weekday_evening_top_{}_'.format(top_count) + filename))
    save_top(filtered_current_places, 'we_ln_density', top_count, Path(OUTPUT_PATH + '/' + 'weekend_late_night_top_{}_'.format(top_count) + filename))
    save_top(filtered_current_places, 'we_m_density', top_count, Path(OUTPUT_PATH + '/' + 'weekend_morning_top_{}_'.format(top_count) + filename))
    save_top(filtered_current_places, 'we_a_density', top_count, Path(OUTPUT_PATH + '/' + 'weekend_afternoon_top_{}_'.format(top_count) + filename))
    save_top(filtered_current_places, 'we_e_density', top_count, Path(OUTPUT_PATH + '/' + 'weekend_evening_top_{}_'.format(top_count) + filename))
    print("successfully completed!")

def save_top(df, col_name, top_no, filename):
    df = df.sort_values(by=col_name, ascending=False).head(top_no)
    df.to_csv(filename)
    
class week_dto:
      # a class to hold all data (given date, core data, nyc block groups)

    def __init__(self, my_date, nyc_counties, nrows):
        self.my_date = my_date
        #self.nyc_counties = nyc_counties
        #find monday of that week to search for weekly files.
        self.monday = self.find_monday(my_date)
        #create the  weekly patterns dataframe wp_df that has been filtered to county.
        print("getting weekly patterns")
        self.wp_df = self.get_weekly_patterns_v2(nyc_counties, nrows).reset_index()
        #get the multiplier for that week
        print("looking up multiplier")
        #multiplier reduced to a magic number found in other applications.
        self.multiplier = self.lookup_multiplier()
        #get the square feet
        print("getting visits for time blocks")
        self.wp_df = self.get_visits_quarter_day(self.wp_df)
        print("getting squarefeet")
        self.wp_df = self.get_sqft(self.wp_df)
        print("getting density")
        self.wp_df = self.get_density(self.wp_df)

    def get_weekly_patterns_v2(self, nyc_counties, nrows):        
        #query by select all core data sgid that start with the five county codes
        #build path
        self.weekly_patterns_path = os.getenv('V2_WEEKLY_PATTERNS_PATH')
        #self.weekly_patterns_filename = '{0}-{1}-{2}-weekly-patterns.csv'.format(self.my_year, self.my_month, self.my_day)
       
        #open_weekly_patterns
        print('reading_weekly_patterns')
        print("part 1")
        self.wp_df_raw= pd.read_csv(Path(self.weekly_patterns_path + '/' + 'patterns-part1.csv'), nrows=nrows)
        print("part 2")
        part_2 = pd.read_csv(Path(self.weekly_patterns_path + '/' + 'patterns-part2.csv'), nrows=nrows)
        print("part 3")
        part_3 = pd.read_csv(Path(self.weekly_patterns_path + '/' + 'patterns-part3.csv'), nrows=nrows)
        print("part 4")
        part_4 = pd.read_csv(Path(self.weekly_patterns_path + '/' + 'patterns-part4.csv'), nrows=nrows)
        self.wp_df_raw.append(part_2)
        self.wp_df_raw.append(part_3)
        self.wp_df_raw.append(part_4)
        
        #clear memory ASAP
        part_2 = None
        part_3 = None
        part_4 = None
        
        #filter by county
        print('filtering weekly patterns')
        self.filter_list1 = self.wp_df_raw['poi_cbg'].astype(str).str[:5]
        self.nyc_counties_str = list(map(str, nyc_counties))
        self.filter_list2 = self.filter_list1.isin(self.nyc_counties_str).tolist()
        self.wp_df = self.wp_df_raw.loc[self.filter_list2]

        #clear the memory
        wp_df_raw = None
        
        return self.wp_df

    def get_sqft(self, df):
        self.sqft_path = os.getenv('SQFT_PATH')
        self.sqft_filename = 'SafeGraphPlacesGeoSupplementSquareFeet.csv'
        sqft_raw = pd.read_csv(Path(self.sqft_path) / self.sqft_filename)
        sqft_raw = sqft_raw.drop(['location_name', 'polygon_class'], axis=1)
        return pd.merge(df,sqft_raw, on='safegraph_place_id')
        

    def lookup_multiplier(self):
        return 6.982711310647716

    def find_monday(self, my_date):
        monday = my_date - datetime.timedelta(days=my_date.weekday())
        return monday

    def get_visits_quarter_day(self, df):
        df = copy.deepcopy(df)
        df['visits_by_each_hour_list'] = df['visits_by_each_hour'].apply(lambda x: json.loads(x))
        #you need to get this working. see comment below
        #df['visits_pop_each_hour'] = [[0] * len(df.loc[:, 'visits_by_each_hour_list'])] * len(df)
        #print(df['visits_pop_each_hour'])
        #print(df['visits_by_each_hour'])
        #you have to initialize them to look them up by index
        #print("len df: {}".format(len(df)))
        
        df['weekday_late_night'] = 0
        df['weekday_morning'] = 0
        df['weekday_afternoon'] = 0
        df['weekday_evening'] =0
        df['weekend_late_night'] = 0
        df['weekend_morning'] = 0
        df['weekend_afternoon'] = 0
        df['weekend_evening'] = 0

        df2 = copy.deepcopy(df)
        #print("multiplier: {}".format(self.multiplier))
        for index, row in df2.iterrows():
            #print(index)
            #you need to get this working. If you do, change the subsequent multipliers to 1 and set get_average_visit(row['visits_pop_each_hour'])
            #df.iloc[index, df.columns.get_loc('visits_pop_each_hour')] = my_mult_list2(row['visits_by_each_hour_list'], self.multiplier)
            
            df.iloc[index, df.columns.get_loc('weekday_late_night')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekday_late_night'], self.multiplier)
            df.iloc[index, df.columns.get_loc('weekday_morning')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekday_morning'], self.multiplier)
            df.iloc[index, df.columns.get_loc('weekday_afternoon')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekday_afternoon'], self.multiplier)
            df.iloc[index, df.columns.get_loc('weekday_evening')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekday_evening'], self.multiplier)
            df.iloc[index, df.columns.get_loc('weekend_late_night')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekend_late_night'], self.multiplier)
            df.iloc[index, df.columns.get_loc('weekend_morning')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekend_morning'], self.multiplier)
            df.iloc[index, df.columns.get_loc('weekend_afternoon')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekend_afternoon'], self.multiplier)
            df.iloc[index, df.columns.get_loc('weekend_evening')] = get_average_visit(row['visits_by_each_hour_list'], day_quarter['weekend_evening'], self.multiplier)
        return df

    def get_density(self, df):
        #sqft_mult = 1000
        sqft_mult = 133
        df['wd_ln_density'] = df['weekday_late_night'] / (df['area_square_feet'] + 1) * sqft_mult
        df['wd_m_density'] = df['weekday_morning'] / (df['area_square_feet'] + 1) * sqft_mult
        df['wd_a_density'] = df['weekday_afternoon'] / (df['area_square_feet'] + 1) * sqft_mult
        df['wd_e_density'] = df['weekday_evening'] / (df['area_square_feet'] + 1) * sqft_mult
        df['we_ln_density'] = df['weekend_late_night'] / (df['area_square_feet'] + 1) * sqft_mult
        df['we_m_density'] = df['weekend_morning'] / (df['area_square_feet'] + 1) * sqft_mult
        df['we_a_density'] = df['weekend_afternoon'] / (df['area_square_feet'] + 1) * sqft_mult
        df['we_e_density'] = df['weekend_evening'] / (df['area_square_feet'] + 1) * sqft_mult
        #print("wd_a_density: \n{}".format(df['wd_a_density']))
        return df

def filter_by_naics_code(core_df, naics):
        filter_list1 = core_df['naics_code'].astype(str)
        #in case it comes in as an int
        naics = str(naics)
        #self.nyc_counties_str = list(map(str, nyc_counties))
        #filter_list2 = self.filter_list1.isin(self.nyc_counties_str).tolist()
        filter_list2 = filter_list1.apply(lambda x: x.startswith(naics))
        #self.wp_df = self.wp_df_raw.loc[self.filter_list2]
        core_places_df = core_df.loc[filter_list2]
        return core_places_df


def my_add_lists(list_x, addend):
    list_y = my_mult_list(len(list_x) * [1], addend)
    return [x + y for x, y in zip(list_x, list_y)]

def my_mult_list(list_a, multiplier):
    list_a = pd.Series(list_a)
    list_b = [multiplier] * len(list_a)
    return [x * y  for x, y in zip(list_a, list_b )]

def my_mult_list2(list_a, multiplier):
    return [x * multiplier for x in list_a]

def make_hour_list_weekday(l_a):
    return l_a + my_add_lists(l_a, 24) + my_add_lists(l_a, 24 * 2) + my_add_lists(l_a, 24 * 3) + my_add_lists(l_a, 24 * 4)

def make_hour_list_weekend_day(l):
    return my_add_lists(l, 24 * 5) + my_add_lists(l, 24 * 6)

def make_hour_list_weekend_night(l):
    return my_add_lists(l, 24 * 4) + my_add_lists(l, 24 * 5) + my_add_lists(l, 24 * 6)

def get_average_visit(all_hours, my_filter, multiplier):
    if type(all_hours) == str:
        all_hours = json.loads(all_hours)
    return (sum([all_hours[i] for i in my_filter]) / (len(my_filter) * 1.0)) * multiplier

day_quarter = {
    "weekday_late_night" : make_hour_list_weekday([0,1,2,3,4,5]),
    "weekday_morning": make_hour_list_weekday([6,7,8,9,10,11]),
    "weekday_afternoon": make_hour_list_weekday([12, 13, 14, 15, 16, 17]),
    "weekday_evening": make_hour_list_weekday([18, 19, 20, 21, 22, 23]),
    "weekend_late_night": make_hour_list_weekend_day([0,1,2,3,4,5]),
    "weekend_morning": make_hour_list_weekend_day([6,7,8,9,10,11]),
    "weekend_afternoon": make_hour_list_weekend_day([12, 13, 14, 15, 16, 17]),
    "weekend_evening": make_hour_list_weekend_night([18, 19, 20, 21, 22, 23])
}
def load_core_places(my_n_rows):
    print("loading core places")
    CORE_PLACES_PATH = os.getenv('CORE_PLACES_PATH')
    print("part 1")
    df = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part1.csv'), nrows=my_n_rows)
    print("part 2")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part2.csv'), nrows=my_n_rows)
    df = df.append(temp, ignore_index=True)
    print("part 3")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part3.csv'), nrows=my_n_rows)
    df = df.append(temp, ignore_index=True)
    print("part 4")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part4.csv'), nrows=my_n_rows)
    df = df.append(temp, ignore_index=True)
    print("part 5")
    temp = pd.read_csv(Path(CORE_PLACES_PATH + '/core_poi-part5.csv'), nrows=my_n_rows)
    df = df.append(temp, ignore_index=True)
    return df

def core_patterns_inner_join(core_places_df, current_dto):
    return pd.merge(core_places_df, current_dto.wp_df, how='inner', on='safegraph_place_id')

if __name__ == "__main__":
    main()