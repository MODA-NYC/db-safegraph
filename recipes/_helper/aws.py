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
        aws_secret_access_key,
    ):

        """ 
        initialize class Aws with the following attributes
        athena, s3, s3_client
        """

        self.athena = boto3.client(
            "athena",
            region_name=aws_region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.s3 = boto3.Session(
            aws_access_key_id=rdp_access_key_id,
            aws_secret_access_key=rdp_secret_access_key,
        ).resource("s3")

        self.s3_client = boto3.client(
            "s3",
            region_name=aws_region_name,
            aws_access_key_id=rdp_access_key_id,
            aws_secret_access_key=rdp_secret_access_key,
        )

        self.bucket = "recovery-data-partnership"
        self.temporary_location = f"{self.bucket}/tmp/"

    def execute_query(self, query: str, database: str, output: str):
        """
        This is the interface function that 
        1. start query
        2. wait for output to materialize
        3. move output to target location
        """

        queryId, queryLoc, queryMetadata = self.start_query(query, database)
        response = self.wait_till_finish(queryId)
        if response["Status"] == "SUCCEEDED":
            moved = self.move_output(queryLoc, queryMetadata, output)
            if moved:
                print("Done !")
                return response

    def start_query(self, query: str, database: str) -> str:
        """
        Start a Query and return queryId
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.start_query_execution
        """
        queryStart = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": f"s3://{self.temporary_location}"},
        )
        queryId = queryStart["QueryExecutionId"]
        queryLoc = f"{self.temporary_location}{queryId}.csv"
        queryMetadata = f"{self.temporary_location}{queryId}.csv.metadata"

        return queryId, queryLoc, queryMetadata

    def get_query_status(self, queryId: str):
        """
        Check the status of the query
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.batch_get_query_execution
        """
        response = self.athena.batch_get_query_execution(QueryExecutionIds=[queryId])
        QueryExecution = response["QueryExecutions"][0]
        UnprocessedQueryExecutionId = response["UnprocessedQueryExecutionIds"][0] if len(response["UnprocessedQueryExecutionIds"]) > 0 else {}
        return QueryExecution, UnprocessedQueryExecutionId

    def wait_till_finish(self, queryId: str):
        QueryExecution, UnprocessedQueryExecutionId = self.get_query_status(queryId)
        status = QueryExecution["Status"]["State"]
        TotalExecutionTimeInSeconds = (
                    QueryExecution["Statistics"]["TotalExecutionTimeInMillis"] / 1000
                )
        print(f"Time elapsed: {TotalExecutionTimeInSeconds}")

        if status in ("QUEUED", "RUNNING"):
            # If query is in queue, or query is running, 
            # then sleep for 10s then check status again
            time.sleep(10)
            return self.wait_till_finish(queryId)

        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            # If status in any above, 
            # then break recursion and make response
            response = {
                "Status": status,
                "Exception": UnprocessedQueryExecutionId,
                "Stats": QueryExecution["Statistics"],
            }
            return response

    def move_output(self, queryLoc: str, queryMetadata: str, outputLoc: str):
        """
        1. Assuming file is ready, then copy file (queryLoc -> outputLoc)
        2. Remove file from temporary location (queryLoc, queryMetadata)
        """
        self.s3.Object(self.bucket, outputLoc).copy_from(CopySource=queryLoc)

        if self.check_file_exisitence(outputLoc):
            # If file is successly moved queryLoc -> outputLoc
            # Then delete the files stored at temporary location
            self.s3.Object(self.bucket, queryLoc).delete()
            self.s3.Object(self.bucket, queryMetadata).delete()

            if self.check_file_exisitence(queryLoc) == self.check_file_exisitence(
                queryMetadata
            ):
                print("Filed moved, clean up complete")
                return True
            else:
                print("Filed moved, clean up incomplete")
                return False
        else:
            print("Filed not moved, cannot proceed")
            return False

    def check_file_exisitence(self, fileLoc):
        """
        Given file location (fileLoc), check if file exists
        if exists, return True, else False
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=fileLoc)
            return True
        except ClientError:
            return False
