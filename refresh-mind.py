import boto3
import os
from datetime import datetime
from dotenv import load_dotenv
import argparse

# PRESCRIPT DETAILS
load_dotenv()
ACCESS_KEY_ID: str = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID: str | None = os.getenv("secret_access_key_id")
S3C = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)
S3R = boto3.resource(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)


def main():
    pass


if __name__ == "__main__":
    main()
