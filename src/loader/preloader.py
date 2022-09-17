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


def my_argument_parser():
    return parser


parser = argparse.ArgumentParser(
    description="this script is for a lambda that serves as a precursor to a load of fresh data. we STATEther all the metadata and spin up all the temp tables for the load"
)

arg_group = parser.add_mutually_exclusive_group()
arg_group.add_argument(
    "-dates",
    nargs=2,
    help="this is the main way of using this program. specify an interval of dates to use when scanning SL2 for files to upload",
)

arg_group.add_argument(
    "-dev",
    action="store_true",
    help="set DEV_MODE flag to help writing/debugging the program",
)
args = my_argument_parser().parse_args()

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
