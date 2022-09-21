from collections import defaultdict
import sys
from datetime import date
import boto3

sys.path.append(".")


import argparse
from datetime import date
from configfile import STAGE_LAYER_TWO, REGION_NAME, LOAD_MANIFEST_BUCKET


S3C.

# global args. configured at start then not modified.
STATE = {
    "PROD_MODE": True,
    "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
    "REGION_NAME": REGION_NAME,
    "START_DATE": None,
    "END_DATE": None,
}


# run as lambda
def main(event={}, context=None):
    global STATE  # should only be required here and nowhere else
    event = defaultdict(lambda: None, event)

    STATE = prepstate(event, STATE)

    # build manifest file for query and push to s3
    manifest = createManifest()

    # build query for fight_source
    # build query for round_source

    # We want to pack as much data into a transaction block therefore pack all the dates together
    # For data sanity sake we want no fights without rounds or rounds without fights, so put both COPY commands together in same transation block
    # Conclusion: pack it all together. Then commit.


def createManifest(STATE=STATE):

    return ""


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
