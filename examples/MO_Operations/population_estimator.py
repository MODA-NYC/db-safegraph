###########
### Attempt to find population using safegraph data
###########


import pandas as pd
from pathlib import Path
from datetime import date
import copy

def read_home_summary(year, month, summary_root):
    file_path = summary_root / year / month / 'home_panel_summary.csv'
    return pd.read_csv(file_path)

def main():
    summary_root = Path('//*****/SafeGraph/monthly_patterns/home_panel_summary_backfill/2021/04/13/10')
    nyc_county_fips = ['36005', '36047', '36061', '36081', '36085']
    actual_pop_month = pd.read_csv(Path('//*****/us_population.csv'))
    population_estimate_output_path = Path('//*****/population_estimate')


    actual_pop_month['date'] = pd.to_datetime(actual_pop_month['date'], yearfirst=True, format=str("%b-%y"))
    actual_pop_month['us_devices'] = 0
    actual_pop_month['nyc_devices'] = 0
    #actual_pop_month['nyc_percent_us']
    actual_pop_month['ny_percent_us'] = 0
    actual_pop_month['nyc_pop'] = 0
    actual_pop_month['multiplier'] = 0

    nyc_population = 8400000
    #print(actual_pop_month.info())
    actual_pop_month_copy = actual_pop_month.copy()
    for index, row in actual_pop_month_copy.iterrows():
        #iterate over the actual pop table,
        #read the year and month from the date column of the actual pop table
        # find the date, 
        my_year = str(row['date'].year)
        my_month = str(row['date'].month)
        if (len(my_month) == 1 ):
            my_month = '0' + str(my_month)
        #and look up the folder based on that date.
        try:
            hs = read_home_summary(my_year, my_month, summary_root)
        except FileNotFoundError:
            print("file {}/{} not found".format(my_month, my_year))
            continue
        #count the devices in home summary that month
        actual_pop_month.iloc[index, actual_pop_month.columns.get_loc('nyc_devices')] = count_nyc_devices(hs, nyc_county_fips)

        actual_pop_month.iloc[index, actual_pop_month.columns.get_loc('us_devices')] = count_us_devices(hs)

    actual_pop_month['nyc_percent_us'] = actual_pop_month['nyc_devices'] / actual_pop_month['us_devices']
    actual_pop_month['nyc_pop'] = actual_pop_month['nyc_percent_us'] * actual_pop_month['pop']
    actual_pop_month['multiplier'] = nyc_population * 1.0 / actual_pop_month['nyc_devices'] 
    print(actual_pop_month.head(20))

    print("writing population_estimate.csv")
    actual_pop_month.to_csv(population_estimate_output_path / "population_estimate.csv")
    #home_summary = read_home_summary('2020', '01', summary_root)
    #print(home_summary.info())
    #print(sum(home_summary.number_devices_residing))
    print("success!")
    '''
    for year in os.listdir(summary_root):
        try:
            int(year)
        except(ValueError)
            continue
        for month in os.listdir(home_summary / year):
    '''
def count_us_devices(df):
    return sum(df.number_devices_residing)

def count_nyc_devices(df, counties):
    df['shortened'] = df.census_block_group.apply(lambda x: str(x)[0:5])
    #print("shortened: {}".format(df['shortened']))
    #print(df.head())
    #print(df.info())
    df2 = df.where(df['shortened'].isin(counties))
    df2 = df2.dropna()
    #print(df2.head())
    return sum(df2['number_devices_residing'])

if __name__ == "__main__":
    main()