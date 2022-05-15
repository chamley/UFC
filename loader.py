"""
## ARGUMENTS: ##

1. (0 or 1) defaults to 1 --> Should we run the script in test mode?
2. (0 or 1) defaults to 1 --> Should we engage multiple cores?


"""

import sys
import logging
import boto3
import os
import datetime
from importlib import reload
from dotenv import load_dotenv
import json
import datetime
from dbhelper import DBHelper
import time
import multiprocessing as mp

# SETUP ENVIRONMENT
T = datetime.datetime.now()
load_dotenv()

# short on time, I dont know why im forced to do this.
logging.shutdown()
reload(logging)
logging.basicConfig(
    filename=f"logs/loader-{T}.log", encoding="utf-8", level=logging.DEBUG
)
db = DBHelper()
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
File_Vector = list[dict]


## ARGUMENT PARSING
devmode: bool = True
try:
    devmode = bool(int(sys.argv[1]))
except IndexError:
    pass

## ARGUMENT SETTING
if devmode:
    prefix_string = "fight-2006-12-13diegosanchezjoeriggs.txt-SL-2.json"
else:
    prefix_string = ""


def main():
    files: File_Vector = get_files()
    retry_list = []

    for f in files:
        print(f, files)

        with mp.Pool() as p:
            try:
                object = S3R.Object(bucket_name=STAGE_LAYER_TWO, key=f["Key"]).get()
                fight_object = json.loads(object["Body"].read())
                fight_object["nat_key"] = f["Key"]

                dirty_insert(fight_object)
            except Exception as e:
                print(f"error on {f}:  {e}")
                db.getConn().commit()  # close block and continue
                retry_list.append(f)

    logging.info(retry_list)


def dfs_print(d):
    if isinstance(d, dict):
        for element in d.keys():
            dfs_print(d[element])
    else:

        print(f"{d} is a {type(d)}")


def get_files() -> File_Vector:
    keys: File_Vector = []
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


def dirty_insert(fight_object: dict) -> None:

    colors = ["red", "blue"]
    for c in colors:
        for r in fight_object[c]:
            if not r.isalpha():
                fight_object[c][r]["rev"] = int(fight_object[c][r]["rev"])
                fight_object[c][r]["kd"] = int(fight_object[c][r]["kd"])
                x = fight_object[c][r]["ctrl"]
                fight_object[c][r]["ctrl"] = f"00:0{x}" if x != "--" else "11:11:11"

                for metric in fight_object[c][r].keys():
                    if "_" in metric:
                        fight_object[c][r][metric] = int(fight_object[c][r][metric])
                fight_object[c][r]["result"] = fight_object[c]["result"]
                fight_object[c][r]["color"] = 1 if c == "red" else 2
                fight_object[c][r]["fighter_name_nat"] = fight_object[c]["name"]
                fight_object[c][r]["round"] = int(r[1])
                fight_object[c][r]["fight_key_nat"] = fight_object["nat_key"]
                print(f"inserting  {fight_object[c][r]}")
                db.insert_into_dirty_round(fight_object[c][r])
    fight_object["metadata"]["fight_key_nat"] = fight_object["nat_key"]

    print(f"inserting  {fight_object['metadata']}")
    db.insert_into_dirty_fight(fight_object["metadata"])


if __name__ == "__main__":
    start = time.time()
    try:
        main()
    finally:
        db.closeDB()
    end = time.time()
    logging.info(f"{__file__} ran in {end-start} seconds")
    print(f"{__file__} ran in {end-start} seconds")
