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


def main(event={}, context=None):
    global STATE  # should only be required here and nowhere else

    event = defaultdict(lambda: None, event)

    # set global args
    STATE = prep_script(STATE, event, args)

    file_spec = fetch_file_names()
    # fetch the two sets of filenames (fight, round)

    build_tables()


def build_tables(STATE=STATE):
    # here we build all intermediary tables necesarry
    # use state + datetime.now() to name them and make logs clear
    pass


# given inputs to script fetch the set of prefixes to submit
# to redshift copy command
def fetch_file_names(STATE=STATE):
    x = {"fights": None, "rounds": None}
    return x


# mamma mia, el spaghetti!!
def prep_script(STATE, event, args):
    if event:
        STATE["PROD_MODE"] = True
        if event["dates"]:
            STATE["START_DATE"] = date.fromisoformat(event["dates"]["start"])
            STATE["END_DATE"] = date.fromisoformat(event["dates"]["end"])
            STATE["DATE_SPECIFIED"] = True
            if STATE["END_DATE"] < STATE["START_DATE"]:
                raise ValueError("invalid dates")

        elif event["dev"]:
            STATE["DEV_MODE"] = True
            print("DEV MODE ....")
        else:
            print("invalid event inputted")
            sys.exit()
    elif args.dates:
        STATE["START_DATE"] = date.fromisoformat(args.dates[0])
        STATE["END_DATE"] = date.fromisoformat(args.dates[1])
        STATE["DATE_SPECIFIED"] = True
        if STATE["END_DATE"] < STATE["START_DATE"]:
            raise ValueError("invalid dates")
    elif args.dev:
        STATE["DEV_MODE"] = True
        print("DEV MODE ....")
    else:
        raise ValueError("invalid input to script!")

    return STATE


if not STATE["PROD_MODE"] and __name__ == "__main__":
    main()
