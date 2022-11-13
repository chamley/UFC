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
        lambda: None, {"dates": {"start": "1999-01-01", "end": "2000-12-31"}}
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
                
                
                copy round_source(kd, ss_l, ss_a, ts_l, ts_a, td_l, td_a, sub_a, rev, ctrl, ss_l_h, ss_a_h, ss_l_b, ss_a_b, ss_l_l, ss_a_l, ss_l_dist, ss_a_dist, ss_l_cl, ss_a_cl, ss_l_gr, ss_a_gr, fighter_name, fighter_id, fight_key_nat, round_num)
                from 's3://{UFC_META_FILES_LOCATION}/{LOAD_MANIFEST_FOLDER}/{round_manifest_file_name}'
                -- iam_role '{REDSHIFT_S3_READ_IAM_ROLE}';
                access_key_id '{ACCESS_KEY_ID}'
                secret_access_key '{SECRET_ACCESS_KEY_ID}'
                csv
                emptyasnull
                IGNOREHEADER 1
                manifest
            """

    the_fights_query = f"""
                copy fight_source(fight_key_nat, red_fighter_name, red_fighter_id, blue_fighter_name, blue_fighter_id, winner_fighter_name, winner_fighter_id, details, final_round, final_round_duration, method, referee, round_format, weight_class, fight_date, is_title_fight, wmma, wc)
                from 's3://{UFC_META_FILES_LOCATION}/{LOAD_MANIFEST_FOLDER}/{fight_manifest_file_name}'
                -- iam_role '{REDSHIFT_S3_READ_IAM_ROLE}';
                access_key_id '{ACCESS_KEY_ID}'
                secret_access_key '{SECRET_ACCESS_KEY_ID}'
                csv
                emptyasnull
                IGNOREHEADER 1
                manifest
            """
    print(the_rounds_query)
    print(the_fights_query)

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
        lambda x: x[-10:] == "rounds.csv" and inside_bounds(x), keys
    )  # x[-3] == "zip"
    fights = filter(lambda x: x[-9:] == "fight.csv" and inside_bounds(x), keys)

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
    print(round_manifest)
    print(fight_manifest)
    print(f"manifests built: {round_manifest_file_name} AND {fight_manifest_file_name}")
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
