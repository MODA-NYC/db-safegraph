import sys
import os
from .aws import Aws

from dotenv import load_dotenv

#load_dotenv()

aws = Aws(
    aws_region_name=os.environ["AWS_REGION_NAME"],
    rdp_access_key_id=os.environ["RDP_ACCESS_KEY_ID"],
    rdp_secret_access_key=os.environ["RDP_SECRET_ACCESS_KEY"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)
