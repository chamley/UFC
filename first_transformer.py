# The first transformer parses a page and returns a new file for s3 where the data for a fight is
# parsed and formatted clearly
# pushes this new file back to S3

# TO-DO list:
#   Make the transformer run in parrallel, taking advantage of S3 pagination
#


from urllib import response
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os
import datetime
from pprint import pprint

load_dotenv()
ACCESS_KEY_ID = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID = os.getenv("secret_access_key_id")
DATE = datetime.date.today()
S3C = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)

BUCKET_NAME = "ufc-big-data"


def __main__():

    transformer()


def transformer():

    # max keys #remove prefix of volk fight
    response = S3C.list_objects_v2(
        Bucket=BUCKET_NAME, Prefix="fight-2022-04-09alexandervolkanovskichansungjung"
    )
    token = ""
    # tokens.append(response["ContinuationToken"])
    while True:
        if "Contents" in response:
            fight_list_shard = response["Contents"]
            for f in fight_list_shard:
                fight_page = fetch_fight(k=f["Key"])
                fight_object = parse_fight(fight_page)
                push_fight(fight_object)
                break  # for debugging purposes

        if "NextContinuationToken" in response:
            token = response["NextContinuationToken"]
            response = S3C.list_objects_v2(Bucket=BUCKET_NAME, ContinuationToken=token)
        else:
            break
        break  # for debugging purposes


def fetch_fight(k):
    o = S3C.get_object(Bucket=BUCKET_NAME, Key=k)
    fight_page = o["Body"].read()
    return fight_page


# turns a fight page into a json object
def parse_fight(fp):
    fight_object_small = {}
    fight_object_large = {}

    parser = BeautifulSoup(fp, "html.parser")
    print(parser.find_all("tr"))
    return {}


# pushes json object back to s3
def push_fight(fo):
    pass


__main__()
