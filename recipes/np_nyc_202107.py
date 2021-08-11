from _helper import aws
from calendar import monthrange
import sys
import datetime


query = """
SELECT *
FROM "safegraph"."neighborhood_patterns_202107"
WHERE SUBSTR(area,1,5) IN ('36085','36081','36061','36047','36005')  
      AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) >= CAST('{0}' AS DATE)
      AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) <= CAST('{1}' AS DATE) 
"""

today = datetime.date.today()
next_month = '-'.join([str(today.year),str(today.month+1).zfill(2),'01'])
print(next_month)

years = list(range(2018,2023))
months = list(range(1,13))

for y in years:
    for m in months:
        _, numdays = monthrange(y,m)
        data_month = '_'.join([str(y),str(m).zfill(2)])
        start = '-'.join([str(y),str(m).zfill(2),'01'])
        end = '-'.join([str(y),str(m).zfill(2),str(numdays)])
        if start < next_month:
            print(start,end)
            aws.execute_query(
                query=query.format(start, end), 
                database="safegraph", 
                output=f"output/dev/wp_NYC/neighborhood_patterns_NYC_{data_month}.csv.zip"
            )

