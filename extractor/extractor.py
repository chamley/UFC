"""
ohai
"""

# this script currently grabs the webpage of every single fight listed in ufcfights.com

# desired functionality:
# - dates: scrap all fights for given dates
# - csv: grab a csv from s3 with a set of dates
# - dev:


# Desired policy
# - no-overwrite. output in logs.


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

load_dotenv()
access_key_id = os.getenv("access_key_id")
secret_access_key_id = os.getenv("secret_access_key_id")
STAGE_LAYER_ONE: str = "ufc-big-data"
STAGE_LAYER_TWO: str = "ufc-big-data-2"
TODAY = datetime.today().date()  # make sure we don't parse the future event promo
START_DATE: date
END_DATE: date
clean_dates = []

S3C = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key_id,
)
S3R = boto3.resource(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key_id,
)

DEV_MODE: bool = False
PROD_MODE: bool = False
args = my_argument_parser().parse_args()


if args.dev:
    DEV_MODE = True  # not implemented curr
elif args.dates:
    try:
        START_DATE = date.fromisoformat(args.dates[0])
        END_DATE = date.fromisoformat(args.dates[1])
        if END_DATE < START_DATE:
            raise Exception
        print(f"transforming fights from {START_DATE} to {END_DATE}")
    except:
        print("invalid dates")
        sys.exit()
elif args.csv:
    clean_dates = get_dates(args.csv, S3R)
else:
    PROD_MODE = True


def main(event, context):
    if PROD_MODE:
        if event["dev"]:
            DEV_MODE = True  # not implemented curr
        elif event["dates"]:
            try:
                START_DATE = date.fromisoformat(event["dates"]["start"])
                END_DATE = date.fromisoformat(event["dates"]["end"])
                if END_DATE < START_DATE:
                    raise Exception
                print(f"transforming fights from {START_DATE} to {END_DATE}")
            except:
                print("invalid dates")
                sys.exit()
        elif event["csv"]:
            clean_dates = get_dates(event["csv"], S3R)

    print("starting script ..\n#\n#\n#\n#\n#\n#\n#")
    stage_layer_1()
    return 1


# we grab the latest raw data. We transform in another stage
def stage_layer_1():

    # dictionary of past cards with their dates
    card_urls_dic = get_card_urls_dic()
    if clean_dates:
        clean_dic = {}

        for x in card_urls_dic.keys():
            if x in clean_dates:
                print(x)
                clean_dic[x] = card_urls_dic[x]

        card_urls_dic = clean_dic

    print("finished getting EVENT (not singleton fight) urls:")
    print(json.dumps(card_urls_dic, sort_keys=False, indent=4))

    res2 = S3C.list_objects_v2(Bucket=STAGE_LAYER_ONE)
    current_fight_pages = []
    while True:
        items2 = res2["Contents"]
        for i in items2:
            current_fight_pages.append(i["Key"])
        if not "NextContinuationToken" in res2:
            break
        t = res2["NextContinuationToken"]

        res2 = S3C.list_objects_v2(Bucket=STAGE_LAYER_ONE, ContinuationToken=t)

    # date is the key, url is the val
    for date_, card_url in card_urls_dic.items():
        # list of fight urls for that card
        time.sleep(1)  # being respectful of their servers
        fight_urls_list = get_fight_url_list(card_url)
        print(f"fight urls for {card_url} \n: {fight_urls_list}")

        for f in fight_urls_list:
            print("creating fight_page. ..")
            fight_page, names = create_fight_page(f, date_)
            print("fight page created.")
            time.sleep(1)  # being respectful of their servers
            # pushes card page to s3 with date added somewhere

            key = "fight-" + date_.replace("_", "-") + names.lower() + ".txt"

            if key in current_fight_pages:
                print(
                    f"we already have {key} in SL1, no-overwrite policy at the moment"
                )
            else:
                push_fight_page(fight_page, key)


# fetch the urls of all past cards with date
def get_card_urls_dic():
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
        # print(START_DATE, event_date, END_DATE, TODAY)

        # conditions. verbosity for clarity ##
        # no future
        if TODAY <= event_date:
            continue
        # if date constrained and not inside date interval
        if args.dates and not (START_DATE <= event_date and event_date <= END_DATE):
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
    print("pushing: " + object_name + " to: " + STAGE_LAYER_ONE)

    # we add tmp as it seems to be the only folder that we can write to in AWS Lambda
    with open(f"/tmp/{object_name}", "w") as f:
        f.write(fight_page)
        print(
            "trying to write filename:{}, bucket:{} key:{}".format(
                object_name, STAGE_LAYER_ONE, object_name
            )
        )

        S3C.upload_file(
            Filename=f"/tmp/{object_name}", Bucket=STAGE_LAYER_ONE, Key=object_name
        )
        print("fight uploaded successfully")


if not PROD_MODE:
    main({}, None)
