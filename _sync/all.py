import os
import boto3
session = boto3.Session(
    aws_access_key_id=f'{os.getenv("SG_ACCESS_KEY_ID")}',
    aws_secret_access_key=f'{os.getenv("SG_SECRET_ACCESS_KEY")}'
)
s3 = session.resource('s3')
copy_source = {
    'Bucket': 'safegraph-places-outgoing',
    'Key': 'nyc_gov/weekly/'
}
srcbucket = s3.Bucket('safegraph-places-outgoing')
tgtbucket = s3.Bucket('safegraph-post-rdp')
for file in srcbucket.objects.filter(Prefix='nyc_gov/weekly/'):
    copy_source = {
        'Bucket': 'safegraph-places-outgoing/',
        'Key': file.key
    }
    tgtbucket.copy(copy_source, file.key)
    print(f"{file.key} File Copied" )


#bucket = s3.Bucket('safegraph-post-rdp')
#bucket.copy(copy_source, 'patterns/') #other key. In this case the same.
