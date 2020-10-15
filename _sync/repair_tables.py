import sys
sys.path.append('../')
from recipes._helper import aws


query = 'MSCK REPAIR TABLE weekly_patterns;'
aws.start_query(
        query=query, 
        database="safegraph")

query = 'MSCK REPAIR TABLE core_poi;'
aws.start_query(
        query=query, 
        database="safegraph")
        
query = 'MSCK REPAIR TABLE social_distancing;'
aws.start_query(
        query=query, 
        database="safegraph")
