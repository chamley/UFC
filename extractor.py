# To do list:
#   remove limiter on get_card urls dic

# this script currently grabs the webpage of every single fight listed in ufcfights.com


import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import datetime
import boto3
import sys
import time

load_dotenv()
access_key_id = os.getenv("access_key_id")
secret_access_key_id = os.getenv("secret_access_key_id")
DATE = datetime.date.today()  # get all events previous to DATE


def __main__():
    print("starting script ..\n#\n#\n#\n#\n#\n#\n#")
    stage_layer_1()


# we grab the latest raw data. We transform in another stage
def stage_layer_1():
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key_id,
    )
    # dictionary of past cards with their dates
    card_urls_dic = get_card_urls_dic()

    # date is the key, url is the val
    for date, card_url in card_urls_dic.items():
        # list of fight urls for that card
        time.sleep(1)  # being respectful of their servers
        fight_urls_list = get_fight_url_list(card_url)

        for f in fight_urls_list:
            fight_page, names = create_fight_page(f, date)
            time.sleep(1)  # being respectful of their servers
            # pushes card page to s3 with date added somewhere

            push_fight_page(
                fight_page,
                "ufc-night-" + date.replace("_", "-").replace(" ", ""),
                "fight-" + date.replace("_", "-") + names.lower() + ".txt",
                s3,
            )


# fetch the urls of all past cards with date
def get_card_urls_dic():
    new_urls = {}
    endpoint = "http://ufcstats.com/statistics/events/completed?page=all"
    response = requests.get(endpoint)

    parser = BeautifulSoup(response.text, "html.parser")

    events = parser.find_all("i", class_="b-statistics__table-content")
    for e in events:
        s = e.span.text.strip().replace(",", "").split()
        date = datetime.date(
            int(s[2]), datetime.datetime.strptime(s[0], "%B").month, int(s[1])
        )
        # get past events only
        if date < DATE:
            new_urls[str(date)] = e.find("a").get("href")

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


def push_fight_page(fight_page, bucket, object_name, s3):
    bucket = "ufc-big-data"
    print("pushing: " + object_name + " to: " + bucket)
    try:
        # have to open twice for some reason idk
        with (open(object_name, "w") as f):
            f.write(fight_page)

        current_buckets = (
            s3.list_buckets()
        )  # we're still pretty low data so network calls dont need caching
        # check if bucket exists, if not create it
        if bucket not in current_buckets:
            print(
                "trying to write filename:{}, bucket:{} key:{}".format(
                    object_name, bucket, object_name
                )
            )
            s3.create_bucket(Bucket=bucket)

            s3.upload_file(Filename=object_name, Bucket=bucket, Key=object_name)
            print("fight page uploaded successfully")
        else:
            current_objects = s3.list_objects(Bucket=bucket)
            if object_name not in current_objects:
                s3.upload_file(Filename=object_name, Bucket=bucket, Key=object_name)
                print(
                    "trying to write filename:{}, bucket:{} key:{}".format(
                        object_name, bucket, object_name
                    )
                )
            else:
                s3.upload_file(Filename=object_name, Bucket=bucket, Key=object_name)
                print("bucket overwritten")
                raise Exception("trying to overwrite objects !!!")

            pass
        # if not create bucket, else push to that bucket
        # --- check if object exists
        # --- if yes throw error, else put object
    except Exception as e:
        # implement sns
        print(e)
        sys.exit(1)


__main__()
