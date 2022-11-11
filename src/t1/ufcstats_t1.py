import sys

sys.path.append(".")


from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os
import datetime
import sys
from importlib import reload
import json
from collections import defaultdict
import pandas as pd
import awswrangler as wr
from t1_helper import my_argument_parser
from t1_exceptions import InvalidDates, InvalidHTMLTableDimensions
from datetime import date
from configfile import STAGE_LAYER_ONE, STAGE_LAYER_TWO, REGION_NAME


load_dotenv()


ACCESS_KEY_ID = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID = os.getenv("secret_access_key_id")
DATE = date.today()
S3C = boto3.client(
    "s3",
    region_name=REGION_NAME,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)
S3R = boto3.resource(
    "s3",
    region_name=REGION_NAME,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)

STATE = {
    "STAGE_LAYER_ONE": STAGE_LAYER_ONE,
    "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
    "REGION_NAME": REGION_NAME,
    "START_DATE": None,
    "END_DATE": None,
    "TODAY": datetime.datetime.today(),
    "PREFIX_STRING": "",
}


# method to invoke
def main(event={}, context=None) -> None:
    # required here and nowhere else
    global STATE
    event = defaultdict(lambda: None, event)
    STATE = prepstate(event, STATE)

    # find all valid keys to transform
    keys = get_keys(STATE)

    # we find idempotency on write as we may be missing a
    # fight file or a rounds file but not its partner

    print(keys)


def get_keys(STATE) -> list:

    # get keys in SL1 that fall in specified date range
    valid_sl1_keys = []
    start = STATE["START_DATE"].year
    end = STATE["END_DATE"].year
    years = []  # [2009,2010,2011 ...]

    while start <= end:
        years.append(start)
        start += 1

    for y in years:
        keys = S3C.list_objects_v2(
            Bucket=STATE["STAGE_LAYER_ONE"], Prefix=f"fight-{y}"
        ).get("Contents")
        if keys:
            for k in keys:
                valid_sl1_keys.append(k["Key"])
    print(valid_sl1_keys)
    return valid_sl1_keys


def prepstate(event, STATE) -> dict:
    try:
        if (
            not event
            or not event["dates"]
            or not event["dates"]["start"]
            or not event["dates"]["end"]
            or date.fromisoformat(event["dates"]["start"])
            > date.fromisoformat(event["dates"]["end"])
        ):
            raise ValueError
    except TypeError:
        raise ValueError("invalid dates")

    STATE["START_DATE"] = date.fromisoformat(event["dates"]["start"])
    STATE["END_DATE"] = date.fromisoformat(event["dates"]["end"])

    return STATE


# run as script
if __name__ == "__main__":
    main()
