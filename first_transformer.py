# The first transformer parses a page and returns a new file for s3 where the data for a fight is
# parsed and formatted clearly
# pushes this new file back to S3

# TO-DO list:
#   Make the transformer run in parrallel, taking advantage of S3 pagination
#


from ast import Bytes
from distutils.debug import DEBUG
from pprint import pprint
from re import X
from sqlite3 import Timestamp
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os
import datetime
import logging
import sys
from importlib import reload
import json

T = datetime.datetime.today()
load_dotenv()

# short on time, I dont know why im forced to do this.
logging.shutdown()
reload(logging)
logging.basicConfig(
    filename=f"logs/first_transformer-{T}.log", encoding="utf-8", level=logging.DEBUG
)


Key_Vector = list[dict]
ACCESS_KEY_ID: str = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID: str | None = os.getenv("secret_access_key_id")
DATE: datetime.date = datetime.date.today()
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

# refactor this into an argparse + write argpaser.py to process argparsses from all python.
BUCKET_NAME: str = "ufc-big-data"
DEV_MODE: bool = True
prefix_string: str = ""
early_exit: bool = False
if DEV_MODE:
    prefix_string = "fight-2022-04-09alexandervolkanovskichansungjung"
    early_exit = True
else:
    prefix_string = ""


def main():
    transformer()


def transformer() -> None:
    logging.info("Entering first transformer")
    keys: Key_Vector = get_file_keys()  # O(n)
    print(f"Found {len(keys)} files to transform")
    for k in keys:
        object = S3R.Object(bucket_name=BUCKET_NAME, key=k["Key"]).get()
        file = object["Body"].read()
        parse_fight(file)

        break  # debug

    logging.info("Exiting first transformer")


def parse_fight(file):
    # ugly script that turns a fight page into a giant dict with the relevant data
    d = {"red": {}, "blue": {}, "metadata": {}}
    parser = BeautifulSoup(file, "html.parser")
    d["red"]["name"], d["blue"]["name"] = [
        x.text for x in parser.find_all(class_="b-link b-fight-details__person-link")
    ]
    d["metadata"]["weight class"] = parser.find(
        class_="b-fight-details__fight-title"
    ).text.strip()

    d["metadata"]["method"] = (
        parser.find(class_="b-fight-details__text-item_first")
        .find_all("i")[1]
        .text.strip()
    )
    d["red"]["result"], d["blue"]["result"] = [
        x.text.strip() for x in parser.find_all(class_="b-fight-details__person-status")
    ]

    (
        d["metadata"]["final_round"],
        d["metadata"]["final_round_duration"],
        d["metadata"]["round_format"],
        _,
    ) = [
        x.find("i").next_sibling.strip()
        for x in parser.find_all("i", class_="b-fight-details__text-item")
    ]
    d["metadata"]["referee"] = parser.find_all(class_="b-fight-details__text-item")[
        3
    ].span.text.strip()

    d["metadata"]["details"] = (
        parser.find_all(class_="b-fight-details__text")[1]
        .text.strip()
        .replace("\n", "")
        .replace("  ", "")
    )
    table_one = parser.find_all(class_="b-fight-details__table-body")[1]
    table_two = parser.find_all(class_="b-fight-details__table-body")[3]

    n = table_one.find_all(class_="b-fight-details__table-col")

    print(n)
    # print(json.dumps(d, sort_keys=True, indent=4))
    return d


def get_file_keys() -> Key_Vector:
    keys: Key_Vector = []
    res: dict = S3C.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix_string)
    while True:
        items = res["Contents"]
        for i in items:
            keys.append(i)
        if not "NextContinuationToken" in res:
            break
        t = res["NextContinuationToken"]
        res = S3C.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=prefix_string, ContinuationToken=t
        )
    return keys


# pushes json object back to s3
def push_fight(fo):
    pass


main()
