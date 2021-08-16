from _helper import aws
from calendar import monthrange
import sys
import datetime


# query = """
# SELECT *
# FROM "safegraph"."weekly_patterns_202107"
# WHERE SUBSTR(poi_cbg,1,5) IN ('36085','36081','36061','36047','36005')  
#       AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) >= CAST('{0}' AS DATE)
#       AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) <= CAST('{1}' AS DATE) 
# """

query = """
SELECT *
FROM "safegraph"."weekly_patterns_202107"
WHERE region = 'NY'  
      AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) >= CAST('{0}' AS DATE)
      AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) <= CAST('{1}' AS DATE) 
"""

# get today's date to limit the date range for pulled data
today = datetime.date.today()
cur_year = (today.year)
cur_month = (today.month)
cur_day = (today.day)

### CURRENT MONTH WEEKLY PATTERNS FOR NYS
# weekly patterns file is weekly and uploaded in the middle of the week
# this script will pull the data for the current month
# if today is 11th of the month or later. otherwise, it'll pull
# previous month data.

if (cur_month == 1) and (cur_day < 10):
    pull_year = cur_year - 1
    pull_month = 12
elif cur_day > 10:
    pull_year = cur_year
    pull_month = cur_month
else:
    pull_year = cur_year
    pull_month = cur_month-1

# pull number of days in a given month
_, numdays = monthrange(pull_year,pull_month)

# create a string yyyy-mm-01 for the beginning of the month
start = '-'.join([str(pull_year),str(pull_month).zfill(2),'01'])
# create a string yyyy-mm-dd for the end of the month
end = '-'.join([str(pull_year),str(pull_month).zfill(2),str(numdays)])
# create a string yyyy-mm
data_month = '_'.join([str(pull_year),str(pull_month).zfill(2)])

print(f"pulling data for {start} through {end}")

aws.execute_query(
    query=query.format(start, end), 
    database="safegraph", 
    output=f"output/dev/wp_NYS/weekly_patterns_NYS_{data_month}.csv.zip"
            )

### HISTORICAL WEEKLY PATTERNS FOR NYS

# next_month = '-'.join([str(cur_year),str(cur_month+1).zfill(2),'01'])
# print(next_month)

# # date range
# years = list(range(2018,2023))
# months = list(range(1,13))

# for y in years:
#     for m in months:
#         # pull number of days in a given month
#         _, numdays = monthrange(y,m)
#         # create a string yyyy-mm
#         data_month = '_'.join([str(y),str(m).zfill(2)])
#         # create a string yyyy-mm-01 for the beginning of the month
#         start = '-'.join([str(y),str(m).zfill(2),'01'])
#         # create a string yyyy-mm-dd for the end of the month
#         end = '-'.join([str(y),str(m).zfill(2),str(numdays)])
#         # pull the data only through the last day of the current month
#         if start < next_month:
#             print(start,end)
#             aws.execute_query(
#                 query=query.format(start, end), 
#                 database="safegraph", 
#                 output=f"output/dev/wp_NYS/weekly_patterns_NYS_{data_month}.csv.zip"
#             )