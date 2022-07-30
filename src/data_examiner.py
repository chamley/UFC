import logging
from xml.dom.minidom import Element
import boto3
import os
import datetime
from importlib import reload
from dotenv import load_dotenv
import json

T = datetime.datetime.today()
load_dotenv()

# short on time, I dont know why im forced to do this.
logging.shutdown()
reload(logging)
# logging.basicConfig(
#     filename=f"logs/data-examiner-{T}.log", encoding="utf-8", level=logging.DEBUG
# )

STAGE_LAYER_TWO: str = "ufc-big-data-2"

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
Key_Vector = list[dict]
DEV: bool = True

if DEV:
    prefix_string = "fight-2006-12-13diegosanchezjoeriggs.txt-SL-2.json"
else:
    prefix_string = ""


# Tool to look at our json data and see what we're dealing with to check out assumptions. This will probably be refactored(/replaced)
# with Great Expectations however on first pass we're doing it quick and dirty: too many unknown unknowns and speed requirement is fairly high.

# 1. fetch the first page


def main():
    # res: dict = S3C.list_objects_v2(Bucket=STAGE_LAYER_TWO, Prefix=prefix_string)
    keys: Key_Vector = get_file_keys()
    for k in keys:
        object = S3R.Object(bucket_name=STAGE_LAYER_TWO, key=k["Key"]).get()
        fight_object = json.loads(object["Body"].read())

        dfs_print(fight_object)

        # print(json.dumps(fight_object, sort_keys=True, indent=4))


def dfs_print(d):
    if isinstance(d, dict):
        for element in d.keys():
            dfs_print(d[element])
    else:

        print(f"{d} is a {type(d)}")


def get_file_keys() -> Key_Vector:
    keys: Key_Vector = []
    res: dict = S3C.list_objects_v2(Bucket=STAGE_LAYER_TWO, Prefix=prefix_string)
    while True:
        items = res["Contents"]
        for i in items:
            keys.append(i)
        if not "NextContinuationToken" in res:
            break
        t = res["NextContinuationToken"]

        res = S3C.list_objects_v2(
            Bucket=STAGE_LAYER_TWO, Prefix=prefix_string, ContinuationToken=t
        )
    return keys


main()
