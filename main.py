import os
from urllib import request, response
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import re
import json
import datetime


load_dotenv()
access_key_id = os.getenv("access_key_id")
secret_access_key_id = os.getenv("secret_access_key_id")


def __main__():
    print("starting script")
    stage_layer_1()


# we grab the latest raw data. We transform in another stage
def stage_layer_1():
    # dictionary of past cards with their dates
    card_urls_dic = get_card_urls_dic()

    # date is the key, url is the val
    for date, card_url in card_urls_dic.items():
        # list of fight urls for that card

        fight_urls_list = get_fight_url_list(card_url)
        # print(fight_urls_list)
        for f in fight_urls_list:
            print(f, date)
            card_page = create_fight_page(f, date)
        # print(card_page)
        # pushes card page to s3 with date added somewhere
        # push_card_page(card_page)


# fetch the urls of all past cards with date
def get_card_urls_dic():
    new_urls = {}
    endpoint = "http://ufcstats.com/statistics/events/completed?page=all"
    response = requests.get(endpoint)

    parser = BeautifulSoup(response.content, "html.parser")

    events = parser.find_all("i", class_="b-statistics__table-content")[0:2]
    for e in events:
        s = e.span.text.strip().replace(",", "").split()
        date = datetime.datetime(
            int(s[2]), datetime.datetime.strptime(s[0], "%B").month, int(s[1])
        )
        # get past events only
        if date < date.now():
            new_urls[str(date)] = e.find("a").get("href")

    return new_urls


# given a card url return all fight urls and the date of the card
def get_fight_url_list(card_url):
    fight_urls = []
    response = requests.get(card_url)
    parser = BeautifulSoup(response.text, "html.parser")
    fights = parser.find_all("tr", class_="b-fight-details__table-row")
    for f in fights[:2]:
        # he might move around the table structure
        if f.get("data-link"):
            fight_urls.append(f.get("data-link"))

    return fight_urls


# def create_fight_page(fight_url, date):
#     print(fight_url, date)
#     return date + "\n" + requests.get(fight_url).text


__main__()
