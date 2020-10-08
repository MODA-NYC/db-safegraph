import sys
import os

sys.path.insert(0, "..")
from _helper import aws

query = """
SELECT 
    date_range_start,
    device_count,
    completely_home_device_count,
    candidate_device_count
FROM social_distancing LIMIT 5
"""

aws.execute_query(
    query=query, 
    database="safegraph", 
    output="output/example/example.csv"
)