from collections import defaultdict
import sys
from datetime import date, datetime
import boto3
import json
import os


# LOCAL runs require this: #
from dotenv import load_dotenv

load_dotenv()
ACCESS_KEY_ID = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID = os.getenv("secret_access_key_id")

sys.path.append(".")

from dbhelper_loader import DBHelper
from datetime import date
from configfile import (
    STAGE_LAYER_TWO,
    REGION_NAME,
    LOAD_MANIFEST_FOLDER,
    UFC_META_FILES_LOCATION,
    REDSHIFT_S3_READ_IAM_ROLE,
)


S3C = boto3.client(
    "s3",
    region_name=REGION_NAME,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)


# global args. configured at start then not modified.
STATE = {
    "PROD_MODE": True,
    "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
    "REGION_NAME": REGION_NAME,
    "START_DATE": None,  # becomes date object
    "END_DATE": None,  # becomes date object
}


# run as lambda
def main(event={}, context=None):
    global STATE  # should only be required here and nowhere else
    event = defaultdict(lambda: None, event)

    #### test #######################################################
    event = defaultdict(
        lambda: None, {"dates": {"start": "2020-09-08", "end": "2021-10-08"}}
    )
    #### test #######################################################

    STATE = prepstate(event, STATE)
    fight_manifest_file_name, round_manifest_file_name = createManifests()
    callCopy(fight_manifest_file_name, round_manifest_file_name)


# We want to pack as much data into a transaction block therefore pack all the dates together
# For data sanity sake we want no fights without rounds or rounds without fights,
# so put both COPY commands together in same transation block.
def callCopy(fight_manifest_file_name, round_manifest_file_name):
    db = DBHelper()

    the_rounds_query = f"""
                copy round_source
                from 's3://{UFC_META_FILES_LOCATION}/{LOAD_MANIFEST_FOLDER}/{round_manifest_file_name}'
                iam_role '{REDSHIFT_S3_READ_IAM_ROLE}';
            """

    the_fights_query = f"""
                copy fight_source
                from 's3://{UFC_META_FILES_LOCATION}/{LOAD_MANIFEST_FOLDER}/{fight_manifest_file_name}'
                iam_role '{REDSHIFT_S3_READ_IAM_ROLE}';
            """
    conn = db.getConn()
    cur = db.getCursor()
    cur.execute(the_rounds_query)
    cur.execute(the_fights_query)
    conn.commit()
    db.closeDB()


# Design Decisions:
# I like the idea of pushing a manifest to S3 and keeping them as potential logs.
# creates manifests and pushes them to s3 and returns us their location
def createManifests(STATE=STATE):
    """given input to Lambda build a manifest dict and also push it as a timestamped log to s3 meta files"""
    round_manifest = {
        "entries": []
    }  # entry example {"url":"s3://mybucket/custdata.1", "mandatory":true},
    fight_manifest = {
        "entries": []
    }  # entry example {"url":"s3://mybucket/custdata.1", "mandatory":true},

    # ARE WE DESIGNING OUR LAMBDA FUNCTIONS TO BE IDEMPOTENT ????? nope

    # build 2 lists (round/fights) of the keys of the objects to load to db

    # design: narrowing search space in lambda takes pressure off of datalake (stupid at this scale but whatever)
    years = [x for x in range(STATE["START_DATE"].year, STATE["END_DATE"].year + 1)]
    objects = []

    for y in years:
        objects.extend(get_files(f"fight-{y}"))

    # push to LOAD_MANIFEST_FOLDER_URI with timestamped + descriptive names for each manifest
    # return both manifest URI
    keys = [x["Key"] for x in objects]

    rounds = filter(
        lambda x: x[-3:] == "zip" and inside_bounds(x), keys
    )  # x[-3] == "zip"
    fights = filter(lambda x: x[-3:] == "csv" and inside_bounds(x), keys)

    # build manifest
    for x in fights:
        fight_manifest["entries"].append(
            {"url": f"s3://{STAGE_LAYER_TWO}/{x}", "mandatory": True}
        )

    for x in rounds:
        round_manifest["entries"].append(
            {"url": f"s3://{STAGE_LAYER_TWO}/{x}", "mandatory": True}
        )

    fight_manifest_file_name = f"fight-manifest-{datetime.now().isoformat(sep='-').replace(':','-').replace('.','-')}.json"
    round_manifest_file_name = f"round-manifest-{datetime.now().isoformat(sep='-').replace(':','-').replace('.','-')}.json"

    # Push our manifest log file
    S3C.put_object(
        ACL="private",
        Bucket=f"{UFC_META_FILES_LOCATION}",
        Key=f"{LOAD_MANIFEST_FOLDER}/{fight_manifest_file_name}",
        Body=json.dumps(fight_manifest),
    )
    S3C.put_object(
        ACL="private",
        Bucket=f"{UFC_META_FILES_LOCATION}",
        Key=f"{LOAD_MANIFEST_FOLDER}/{round_manifest_file_name}",
        Body=json.dumps(round_manifest),
    )
    return fight_manifest_file_name, round_manifest_file_name


# check date inside bounds
def inside_bounds(x, STATE=STATE):
    return (
        STATE["START_DATE"] <= date.fromisoformat(x[6:16])
        and date.fromisoformat(x[6:16]) <= STATE["END_DATE"]
    )


def get_files(prefix_string=""):
    keys = []
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


def prepstate(event, STATE):
    """any preprocessing before script occurs here"""
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
        raise ValueError("Invalid Dates")

    STATE["START_DATE"] = date.fromisoformat(event["dates"]["start"])
    STATE["END_DATE"] = date.fromisoformat(event["dates"]["end"])

    return STATE


# run as script
if __name__ == "__main__":
    main()
