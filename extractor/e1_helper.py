import argparse


def get_dates(s3_url, S3R):
    print(f"Using file: {s3_url}")
    dates_csv = (
        S3R.Object("ufc-meta-files", f"e1-dates/{s3_url}")
        .get()["Body"]
        .read()
        .decode("utf-8")
        .split(",")
    )
    clean_dates = list(filter(lambda x: x, dates_csv))
    print("found event dates:")

    for x in clean_dates:
        print(f"\t {x}")
    return clean_dates


def my_argument_parser():
    parser = argparse.ArgumentParser(description="Extract Data from ufcstats.com")
    arg_group = parser.add_mutually_exclusive_group(required=False)
    arg_group.add_argument(
        "-dates",
        nargs=2,
        help="This is the main way of using this program. Lets you specify a date range to cover. Takes 2 arguments (start and end) formatted like so: YYYY-MM-DD. ",
    )

    arg_group.add_argument(
        "-csv",
        help="provide the name of the csv file with fight prefixes to apply t1. csv should be located in folder: s3://ufc-meta/e1-dates",
    )
    arg_group.add_argument(
        "-dev",
        help="activates DEV_MODE flag you can put around the program to debug",
        action="store_true",
    )
    return parser
