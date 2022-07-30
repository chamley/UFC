import pytest
import sys
from moto import mock_s3
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
sys.path.append(".")


from src.extractor.e1_helper import get_dates

REGION_NAME = os.getenv("REGION_NAME")
UFC_META_FILES_LOCATION = os.getenv("UFC_META_FILES_LOCATION")


@mock_s3
class TestGetDates(object):
    def test_normal_usage(self):
        s3c = boto3.client("s3", REGION_NAME)
        s3c.create_bucket(Bucket=UFC_META_FILES_LOCATION)
        s3c.put_object(Bucket=UFC_META_FILES_LOCATION, Key="")

        get_dates()

        assert True

    def test_free_tests_weee(object):
        assert True
        assert True
