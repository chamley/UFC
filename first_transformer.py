# The first transformer parses a page and returns a new file for s3 where the data for a fight is
# parsed and formatted clearly
# pushes this new file back to S3

# TO-DO list:
#   Make the transformer run in parrallel, taking advantage of S3 pagination
#


from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os
import datetime
import logging


load_dotenv()
logging.basicConfig(filename="logs/db_build.log", encoding="utf-8", level=logging.DEBUG)

ACCESS_KEY_ID: str | None = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID: str = os.getenv("secret_access_key_id")
DATE: datetime.date = datetime.date.today()
S3C = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)

# refactor this into an argparse + write argpaser.py to process argparsses from all python.
BUCKET_NAME: str = "ufc-big-data"
DEV_MODE: bool = False
prefix_string: str = ""
EARLY_EXIT: bool = False
if DEV_MODE:
    PREFIX_STRING = "fight-2022-04-09alexandervolkanovskichansungjung"
    EARLY_EXIT = True
else:
    prefix_string = ""


def main():
    transformer()


def transformer() -> None:
    logging.info("Entering first transformer")
    files_transformed: int = 0  # delete

    logging.info(f"parsed {files_transformed} files.")
    logging.info("Exiting first transformer")


def count_files() -> int:
    count: int = 0
    response = S3C.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX_STRING)
    while True:
        for page in response["Contents"]:
            count += 1
        if not "NextContinuationToken" in response:
            break
        t = response["NextContinuationToken"]
        response = S3C.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=PREFIX_STRING, ContinuationToken=t
        )
    return ""


def fetch_fight(k):
    pass


# turns a fight page into a json object
def parse_fight(fp):
    pass


# pushes json object back to s3
def push_fight(fo):
    pass


main()
