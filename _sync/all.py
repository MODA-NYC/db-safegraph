import os
import boto3
s3 = boto3.resource('s3')
KEY=os.getenv('SG_SECRET_ACCESS_KEY')
copy_source = {
    'Bucket': 's3://safegraph-places-outgoing/nyc_gov/weekly',
    'Key': KEY
}
bucket = s3.Bucket('s3://safegraph-post-rdp/patterns')
bucket.copy(copy_source, KEY)
