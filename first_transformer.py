# The first transformer parses a page and returns a new file for s3 where the data for a fight is
# parsed and formatted clearly
# pushes this new file back to S3

# TO-DO list:
#   Make the transformer run in parrallel, taking advantage of S3 pagination
#


from ast import AsyncFunctionDef
from errno import EALREADY
from lib2to3.pgen2 import token
from sys import prefix
from urllib import response
from xmlrpc.client import boolean
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os
import datetime
from pprint import pprint
import sys

load_dotenv()
ACCESS_KEY_ID: str = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID: str = os.getenv("secret_access_key_id")
DATE: datetime.datetime = datetime.date.today()
S3C = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)

# refactor this into an argparse + write argpaser.py to process argparsses from all python.
BUCKET_NAME: str = "ufc-big-data"
DEV_MODE: bool = False
PREFIX_STRING: str = ""
EARLY_EXIT: bool = False


def main():
    transformer()


def transformer() -> None:
    count = 0  # delete
    if DEV_MODE:
        prefix_string = "fight-2022-04-09alexandervolkanovskichansungjung"
        EARLY_EXIT = True
    else:
        prefix_string = ""

    files = set()

    response = S3C.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX_STRING)
    while True:
        for page in response["Contents"]:
            # do stuff
            count += 1
            if page["Key"] in files:
                print("error")
                sys.exit()
            else:
                files.add(page["Key"])
                print(len(files))
        if not "NextContinuationToken" in response:
            break
        t = response["NextContinuationToken"]
        response = S3C.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=PREFIX_STRING, ContinuationToken=t
        )


def fetch_fight(k):
    pass


# turns a fight page into a json object
def parse_fight(fp):
    pass


# pushes json object back to s3
def push_fight(fo):
    pass


main()
