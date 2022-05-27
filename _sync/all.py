import os
import boto3
session = boto3.Session(
    aws_access_key_id=f'{os.getenv("SG_ACCESS_KEY_ID")}',
    aws_secret_access_key=f'{os.getenv("SG_SECRET_ACCESS_KEY")}'
)
s3 = boto3.resource('s3')
KEY=os.getenv('SG_SECRET_ACCESS_KEY')
copy_source = {
    'Bucket': 'safegraph-places-outgoing',
    'Key': 'nyc_gov/weekly/'
}
bucket = s3.Bucket('safegraph-post-rdp')
bucket.copy(copy_source, 'patterns') #other key. In this case the same.
