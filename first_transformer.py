# The first transformer parses a page and returns a new file for s3 where the data for a fight is
# parsed and formatted clearly
# pushes this new file back to S3
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
ACCESS_KEY_ID = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID = os.getenv("secret_access_key_id")


def __main__():
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY_ID,
    )


# grabs a fight from s3
def fetch_fight():
    pass


# turns a fight page into a json object
def parse_fight():
    pass


# pushes json object back to s3
def push_fight():
    pass


__main__()
