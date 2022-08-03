from collections import defaultdict
import sys


sys.path.append(".")


import argparse
from datetime import date

from configfile import STAGE_LAYER_TWO, REGION_NAME

# global args. massaged around at start of script then left alone.
GA = {
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
    description="this script is for a lambda that serves as a precursor to a load of fresh data. we gather all the metadata and spin up all the temp tables for the load"
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
    global GA
    event = defaultdict(lambda: None, event)

    GA = prep_script(GA, event, args)


# mamma mia, el spaghetti!!
def prep_script(GA, event, args):
    if event:
        GA["PROD_MODE"] = True
        if event["dates"]:
            try:
                GA["START_DATE"] = date.fromisoformat(event["dates"]["start"])
                GA["END_DATE"] = date.fromisoformat(event["dates"]["end"])
                GA["DATE_SPECIFIED"] = True
                if GA["END_DATE"] < GA["START_DATE"]:
                    raise Exception
            except:
                print("invalid dates")
                sys.exit()
        elif event["dev"]:
            GA["DEV_MODE"] = True
            print("DEV MODE ....")
        else:
            print("invalid event inputted")
            sys.exit()
    elif args.dates:
        try:
            GA["START_DATE"] = date.fromisoformat(args.dates[0])
            GA["END_DATE"] = date.fromisoformat(args.dates[1])
            GA["DATE_SPECIFIED"] = True
            if GA["END_DATE"] < GA["START_DATE"]:
                raise Exception
        except:
            print("invalid dates")
            sys.exit()
    elif args.dev:
        GA["DEV_MODE"] = True
        print("DEV MODE ....")
    else:
        print("invalid input to script!")
        sys.exit()


if not GA["PROD_MODE"] and __name__ == "__main__":
    main()
