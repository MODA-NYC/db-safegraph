import os
import boto3
s3 = boto3.resource('s3')
KEY=os.getenv('SG_SECRET_ACCESS_KEY')
copy_source = {
    'Bucket': 'safegraph-places-outgoing/nyc_gov/weekly',
    'Key': KEY
}
bucket = s3.Bucket('safegraph-post-rdp/patterns')
bucket.copy(copy_source, KEY) #other key. In this case the same.
