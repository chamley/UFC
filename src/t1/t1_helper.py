import argparse


# arg parsers take arguments passed to a function via the command line and manages them so they
# can be leveraged effectively in the program


def my_arg_parser():
    # we specify a definition for our program
    parser = argparse.ArgumentParser(
        description="ingest a set of dates and apply transformation to all files in SL1, into SL2"
    )

    # Deprecated

    # # Our program can be specified in a number of ways, each mutually exclusive
    # arg_choices = parser.add_mutually_exclusive_group()

    # arg_choices.add_argument(
    #     "-dates",
    #     nargs=2,
    #     help="specify two dates x,y were x<=y formatted as YYYY-MM-DD",
    # )

    # arg_choices.add_argument(
    #     "-dev",
    #     help="activates a DEV_MODE flag which wraps the program and makes it easy",
    # )
    # return parser
