import sys
sys.path.append('../')
from recipes._helper import aws


def msck_repair(tablename:str, database:str='safegraph'):
        return aws.start_query(query=f'MSCK REPAIR TABLE {tablename};', database=database)

msck_repair('core_poi') 
msck_repair('social_distancing')
msck_repair('weekly_patterns') 
msck_repair('geo_supplement')
msck_repair('home_panel_summary')
