import os
from urllib import response
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests

load_dotenv()
access_key_id = os.getenv("access_key_id")
secret_access_key_id = os.getenv("secret_access_key_id")


def __main__():
    print("starting script")
    endpoint = "http://ufcstats.com/statistics/events/completed?page=all"

    response = requests.get(endpoint)

    parser = BeautifulSoup(response.content, "html.parser")
    for element in parser.find_all("a"):
        print(element.get("href"))


__main__()
