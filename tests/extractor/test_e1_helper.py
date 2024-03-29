import pytest
import sys
from moto import mock_s3
import boto3
from io import StringIO
import csv


### see issue here: https://github.com/spulec/moto/issues/1941

import os

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

###

sys.path.append("./src/extractor")

# from configfile import REGION_NAME, UFC_META_FILES_LOCATION, E1_CSV_OPT_DATE_FOLDER_PATH
from configfile import config_settings

STATE = {**config_settings}

from src.extractor.e1_helper import get_dates
from src.extractor.e1_helper import my_argument_parser


def create_csv(date_list):
    buffer = StringIO()
    writer = csv.writer(buffer, delimiter=",")
    writer.writerow(date_list)
    return buffer


@mock_s3
class TestGetDates(object):
    @pytest.mark.parametrize(
        "mock_s3_endpoint, mock_data, expected",
        [
            ("sebtest.csv", "2021-01-01".encode("utf-8"), ["2021-01-01"]),
            (
                "sebtest2.csv",
                "2021-01-01,2022-01-01".encode("utf-8"),
                ["2021-01-01", "2022-01-01"],
            ),
        ],
    )
    def test_normal_usage(self, mock_s3_endpoint, mock_data, expected):
        s3c = boto3.client("s3", STATE["REGION_NAME"])
        s3c.create_bucket(Bucket=STATE["UFC_META_FILES_LOCATION"])
        s3c.put_object(
            Bucket=STATE["UFC_META_FILES_LOCATION"],
            Key=f"{STATE['E1_CSV_OPT_DATE_FOLDER_PATH']}/{mock_s3_endpoint}",
            Body=mock_data,
        )
        actual = get_dates(mock_s3_endpoint, boto3.resource("s3", STATE["REGION_NAME"]))

        assert actual == expected, "fetches a csv from s3"


class TestMyArgumentParser(object):
    def test_returns_a_parser_with_a_parse_args_method(self):
        parser = my_argument_parser()
        assert bool(parser.parse_args()), "has the correct module used"
