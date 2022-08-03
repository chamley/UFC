import sys

sys.path.append(".")


import argparse
from configfile import STAGE_LAYER_TWO, REGION_NAME

# global args
GA = {
    "PROD_MODE": "False",
    "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
    "REGION_NAME": REGION_NAME,
}


def main(event={}, context=None):
    my_args = my_argument_parser().parse_args()


def my_argument_parser():
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
    return parser
