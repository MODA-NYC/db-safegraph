from .__init__ import aws
import datetime

s3_client = aws.s3_client
def get_poi_latest_date():
    response = s3_client.list_objects(Bucket=aws.bucket, Prefix='core_poi/poi/', Delimiter='/')
    dates = [
        i['Prefix']\
            .replace('core_poi/poi/dt=', '')\
            .replace('/', '')
        for i in response['CommonPrefixes']
    ]
    return max(dates, key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d')) 

def get_geo_latest_date():
    response = s3_client.list_objects(Bucket=aws.bucket, Prefix='geo_supplement/', Delimiter='/')
    dates = [
        i['Prefix']\
            .replace('geo_supplement/dt=', '')\
            .replace('/', '')
        for i in response['CommonPrefixes']
    ]
    return max(dates, key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))

poi_latest_date = get_poi_latest_date()
geo_latest_date = get_geo_latest_date()

