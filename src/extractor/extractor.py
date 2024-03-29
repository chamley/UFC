"""
ohai
"""

# this script currently grabs the webpage of every single fight listed in ufcfights.com

# Functionality:
# - dates: scrap all fights for given dates
# - csv: grab a csv from s3 with a set of dates
# - dev:


# Policy
#  no-overwrite. output in logs.

# TODO:
#   Refactor into a STATE object
#

import sys

sys.path.append(".")
from collections import defaultdict
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import boto3
import sys
import time
from datetime import datetime
from e1_helper import my_argument_parser, get_dates
from datetime import date
import json
from configfile import config_settings
from e1_exceptions import InvalidDates
import logging


load_dotenv()
access_key_id = os.getenv("access_key_id")
secret_access_key_id = os.getenv("secret_access_key_id")

STATE = {**config_settings}

TODAY = datetime.today().date()  # make sure we don't parse the future event promo
START_DATE = END_DATE = date.fromisoformat("1111-11-11")

clean_dates = []


S3C = boto3.client(
    "s3",
    region_name=STATE["REGION_NAME"],
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key_id,
)
S3R = boto3.resource(
    "s3",
    region_name=STATE["REGION_NAME"],
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key_id,
)

DEV_MODE: bool = False
PROD_MODE: bool = False
args = my_argument_parser().parse_args()
DATE_SPECIFIED: bool = False

if args.dev:
    DEV_MODE = True  # not implemented curr
elif args.dates:
    DATE_SPECIFIED = True
    try:
        START_DATE = date.fromisoformat(args.dates[0])
        END_DATE = date.fromisoformat(args.dates[1])
        if END_DATE < START_DATE:
            raise InvalidDates
        logging.info(f"extracting fights from {START_DATE} to {END_DATE}")
    except:
        logging.error("invalid dates")
        sys.exit()
elif args.csv:
    clean_dates = get_dates(args.csv, S3R)
else:
    PROD_MODE = True

# main function
def main(event, context):
    global DEV_MODE, DATE_SPECIFIED, clean_dates, START_DATE, END_DATE

    if PROD_MODE:
        event = defaultdict(lambda: None, event)
        if event["dev"]:
            DEV_MODE = True  # not implemented curr
        elif event["dates"]:
            DATE_SPECIFIED = True
            logging.info("date specified flag set")
            try:
                START_DATE = date.fromisoformat(event["dates"]["start"])
                END_DATE = date.fromisoformat(event["dates"]["end"])
                if END_DATE < START_DATE:
                    raise InvalidDates
                logging.info(f"transforming fights from {START_DATE} to {END_DATE}")
            except:
                logging.error("invalid dates")
                sys.exit()
        elif event["csv"]:
            clean_dates = get_dates(event["csv"], S3R)

    logging.info("starting script ..\n#\n#\n#\n#\n#\n#\n#")
    stage_layer_1()
    logging.info("ending script ......")
    logging.info("no return values")


# we grab the latest raw data. We transform in another stage
def stage_layer_1():

    # dictionary of past cards with their dates
    card_urls_dic = get_card_urls_dic()
    if clean_dates:
        clean_dic = {}

        for x in card_urls_dic.keys():
            if x in clean_dates:
                logging.info(x)
                clean_dic[x] = card_urls_dic[x]

        card_urls_dic = clean_dic

    logging.info("finished getting EVENT (not singleton fight) urls:")
    logging.info(json.dumps(card_urls_dic, sort_keys=False, indent=4))

    res2 = S3C.list_objects_v2(Bucket=STATE["STAGE_LAYER_ONE"])
    current_fight_pages = []
    while True:
        items2 = res2["Contents"]
        for i in items2:
            current_fight_pages.append(i["Key"])
        if not "NextContinuationToken" in res2:
            break
        t = res2["NextContinuationToken"]

        res2 = S3C.list_objects_v2(Bucket=STATE["STAGE_LAYER_ONE"], ContinuationToken=t)

    # date is the key, url is the val
    for date_, card_url in card_urls_dic.items():
        # list of fight urls for that card
        fight_urls_list = get_fight_url_list(card_url)
        logging.info(f"fight urls for {card_url} \n: {fight_urls_list}")

        for f in fight_urls_list:
            logging.info("creating fight_page. ..")
            fight_page, names = create_fight_page(f, date_)
            logging.info("fight page created.")
            # pushes card page to s3 with date added somewhere

            key = "fight-" + date_.replace("_", "-") + names.lower() + ".txt"

            if key in current_fight_pages:
                logging.info(
                    f"we already have {key} in SL1, no-overwrite policy at the moment"
                )
            else:
                push_fight_page(fight_page, key)


# fetch the urls of all past cards with date
def get_card_urls_dic(
    START_DATE=START_DATE,
    END_DATE=END_DATE,
    TODAY=TODAY,
    DEV_MODE=DEV_MODE,
    DATE_SPECIFIED=DATE_SPECIFIED,
):

    new_urls = {}
    endpoint = "http://ufcstats.com/statistics/events/completed?page=all"
    response = requests.get(endpoint)

    parser = BeautifulSoup(response.text, "html.parser")

    events = parser.find_all("i", class_="b-statistics__table-content")  # ohai
    for e in events:
        s = e.span.text.strip().replace(",", "").split()
        event_date = datetime.strptime(
            f"{s[2]}-{datetime.strptime(s[0], '%B').month}-{s[1]}", "%Y-%m-%d"
        ).date()

        # conditions ##
        # no future
        if TODAY <= event_date:
            continue
        # if date constrained and not inside date interval
        if (DATE_SPECIFIED) and not (
            START_DATE <= event_date and event_date <= END_DATE
        ):
            continue
        # grow list
        new_urls[str(event_date)] = e.find("a").get("href")

        if DEV_MODE:
            break

    return new_urls


# given a card url return all fight urls and the date of the card
def get_fight_url_list(card_url):
    fight_urls = []
    response = requests.get(card_url)

    parser = BeautifulSoup(response.text, "html.parser")
    fights = parser.find_all("tr", class_="b-fight-details__table-row")
    for f in fights:
        # he might move around the table structure
        if f.get("data-link"):
            fight_urls.append(f.get("data-link"))

    return fight_urls


def create_fight_page(fight_url, date):
    fight_page = requests.get(fight_url)
    parser = BeautifulSoup(fight_page.text, "html.parser")
    x = parser.find_all(class_="b-link b-fight-details__person-link")

    names = x[0].text + x[1].text

    return [date + "\n" + fight_page.text, names.replace(" ", "")]


def push_fight_page(fight_page, object_name):
    logging.info("pushing: " + object_name + " to: " + STATE["STAGE_LAYER_ONE"])

    # we add tmp as it seems to be the only folder that we can write to in AWS Lambda
    with open(f"/tmp/{object_name}", "w") as f:
        f.write(fight_page)
        logging.info(
            "trying to write filename:{}, bucket:{} key:{}".format(
                object_name, STATE["STAGE_LAYER_ONE"], object_name
            )
        )

        S3C.upload_file(
            Filename=f"/tmp/{object_name}",
            Bucket=STATE["STAGE_LAYER_ONE"],
            Key=object_name,
        )
        logging.info("fight uploaded successfully")


if not PROD_MODE:
    main({}, None)
