from collections import defaultdict
import sys
from datetime import date

sys.path.append(".")


import argparse
from datetime import date

from configfile import STAGE_LAYER_TWO, REGION_NAME

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

    # formulate the two different ways to load
    #   - parquet to redshift
    #   - csv to df to redshift for )

    # execute (aws wrangler)


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
