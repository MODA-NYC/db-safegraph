import boto3
import os


class Aws:
    def __init__(
        self,
        aws_region_name,
        rdp_access_key_id,
        rdp_secret_access_key,
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
    ):
    
        """ 
        initialize class Aws with the following attributes
        s3_client
        athena_client
        session

        """

        self.s3_client = boto3.client(
            "s3",
            region_name=aws_region_name,
            aws_access_key_id=rdp_access_key_id,
            aws_secret_access_key=rdp_secret_access_key,
        )

        self.athena_client = boto3.client(
            "athena",
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

        self.s3_resource = boto3.Session(
            aws_access_key_id=rdp_access_key_id, 
            aws_secret_access_key=rdp_secret_access_key
        ).resource("s3")
    
    def execute_query(
        self, 
        query:str, 
        database:str, 
        output_location:str='s3://recovery-data-partnership/tmp/'
        ) -> dict:
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html

        This function is a wraper for start_query_execution, 
        Given query, database, output location, execute the query, 
        and print out temporary output location. check official 
        documentation for more detail.

        Response Syntax:
            {
                'QueryExecutionId': 'string'
            }
        """

        queryStart = self.athena_client.start_query_execution(
            QueryString = query, 
            QueryExecutionContext = {'Database': database},
            ResultConfiguration = {"OutputLocation": output_location}
        )

        return queryStart

    def move_file(self, source_location:str, target_location:str):
        """
        given source_location, move file from source_location to
        target_location and remove the original file at source_location
        """
        return None

