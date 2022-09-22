from collections import defaultdict
import sys
from datetime import date
import boto3

sys.path.append(".")


from datetime import date, timedelta
from configfile import STAGE_LAYER_TWO, REGION_NAME, LOAD_MANIFEST_FOLDER


S3C = boto3.client(
    "s3",
    region_name=REGION_NAME,
    access_key_id="AKIA4C4OXDDQP7NUC6VN",
    secret_access_key_id="DqXhI4gu0M3xD5mWwmO7QouneMRTJqdoUo5n/jI4",
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

    # test
    event = defaultdict(
        lambda: None, {"dates": {"start": "2020-09-08", "end": "2021-09-08"}}
    )

    STATE = prepstate(event, STATE)

    # build manifest file for query and push to s3
    manifest_fight, manifest_round = createManifests()

    # build query for fight_source
    # build query for round_source

    # We want to pack as much data into a transaction block therefore pack all the dates together
    # For data sanity sake we want no fights without rounds or rounds without fights, so put both COPY commands together in same transation block
    # Conclusion: pack it all together. Then commit.


def createManifests(STATE=STATE):
    manifest = {
        "entries": []
    }  # entry example {"url":"s3://mybucket/custdata.1", "mandatory":true},

    # create manifest

    # ARE WE DESIGNING OUR LAMBDA FUNCTIONS TO BE IDEMPOTENT ??????

    # linear scan of SL2 to find names of files that should be in there, add to manifest.
    # design: narrowing search space in lambda takes pressure off of datalake (stupid at this scale but whatever)
    years = [x for x in range(STATE["START_DATE"].year, STATE["END_DATE"].year + 1)]
    keys = []

    for y in years:
        keys.append(get_files(f"fight-{y}"))

    # push to LOAD_MANIFEST_FOLDER_URI with timestamped + descriptive names for each manifest
    # return both manifest URI
    print(keys)
    sys.exit()

    return "fight manifest uri", "round manifest uri"


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
