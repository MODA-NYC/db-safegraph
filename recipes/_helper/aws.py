import boto3
import os
import time
from botocore.errorfactory import ClientError

class Aws:
    def __init__(
        self,
        aws_region_name,
        rdp_access_key_id,
        rdp_secret_access_key,
        aws_access_key_id,
        aws_secret_access_key
    ):
    
        """ 
        initialize class Aws with the following attributes
        athena, s3, s3_client
        """

        self.athena = boto3.client(
            "athena",
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        self.s3 = boto3.Session(
            aws_access_key_id=rdp_access_key_id, 
            aws_secret_access_key=rdp_secret_access_key
        ).resource("s3")

        self.s3_client = boto3.client(
            "s3",
            region_name=aws_region_name,
            aws_access_key_id=rdp_access_key_id,
            aws_secret_access_key=rdp_secret_access_key,
        )

        self.bucket = 'recovery-data-partnership'
        self.temporary_location = f'{self.bucket}/tmp/'
    
    def execute_query(
        self,
        query:str, 
        database:str,
        output:str
        ) -> dict:
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html

        This function is a wraper for start_query_execution, 
        Given query, database, output location, execute the query, 
        and print out temporary output location. check official 
        documentation for more detail.
        """

        queryStart = self.athena.start_query_execution(
            QueryString = query, 
            QueryExecutionContext = {'Database': database},
            ResultConfiguration = {"OutputLocation": f's3://{self.temporary_location}'}
        )

        queryId = queryStart['QueryExecutionId']
        queryLoc = f'{self.temporary_location}{queryId}.csv'
        queryLoc_metadata = f'{self.temporary_location}{queryId}.csv.metadata'
        
        retry = True
        n = 10
        while n > 0 and retry:
            try:
                # Check if output is ready yet
                self.s3_client.head_object(Bucket=self.bucket, Key=queryLoc)
                retry=False
            except:
                time.sleep(3)
                n-=1

        # If file is ready, then move file
        self.s3.Object(self.bucket, output)\
            .copy_from(CopySource = queryLoc)
        
        # Remove file from temporary location
        self.s3.Object(self.bucket, queryLoc).delete()
        self.s3.Object(self.bucket, queryLoc_metadata).delete()