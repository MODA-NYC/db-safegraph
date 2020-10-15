from .__init__ import aws
import datetime

s3_client = aws.s3_client
response = s3_client.list_objects(Bucket=aws.bucket, Prefix='core_poi/poi/', Delimiter='/')
dates = [
    i['Prefix']\
        .replace('core_poi/poi/dt=', '')\
        .replace('/', '')
    for i in response['CommonPrefixes']
]
poi_latest_date = max(dates, key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))