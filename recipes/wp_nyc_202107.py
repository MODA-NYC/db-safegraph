from _helper import aws
from _helper.quarters import PastQs, get_quarter
# from _helper.poi import poi_latest_date
import sys


query = """
SELECT *
FROM "safegraph"."weekly_patterns_202107"
WHERE SUBSTR(poi_cbg,1,5) IN ('36085','36081','36061','36047','36005')  
      AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) >= CAST('{0}' AS DATE)
      AND CAST(SUBSTR(date_range_start, 1, 10) AS DATE) <= CAST('{1}' AS DATE) 
"""

# Load the current quarter
quarters = get_quarter()

# quarters = PastQs

for year_qrtr, range in quarters.items():
    start = range[0]
    end = range[1]
    print(year_qrtr, start, end) 
    aws.execute_query(
        query=query.format(start, end), 
        database="safegraph", 
        output=f"output/dev/wp_NYC/weekly_patterns_NYC_{year_qrtr}.csv.zip"
    )
