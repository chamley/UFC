from collections import defaultdict
import sys


sys.path.append(".")


import argparse
from datetime import date

from configfile import STAGE_LAYER_TWO, REGION_NAME

# global args. configured at start then not modified.
STATE = {
    "PROD_MODE": False,
    "DEV_MODE": False,
    "DATE_SPECIFIED": False,
    "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
    "REGION_NAME": REGION_NAME,
    "START_DATE": None,
    "END_DATE": None,
}


# run as lambda
def main(event={}, context=None):
    global STATE  # should only be required here and nowhere else

    STATE = prepstate(event, STATE)

    event = defaultdict(lambda: None, event)


def prepstate(event, STATE):
    """any preprocessing before script occurs here"""
    return STATE


# run as script
if __name__ == "__main__":
    main()
