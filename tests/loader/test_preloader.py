import sys

sys.path.append(".")
sys.path.append("src/loader/")


from collections import defaultdict
from datetime import date

import json
import pytest
from configfile import config_settings

import random
import awswrangler as wr
import boto3
from dotenv import load_dotenv
import os

load_dotenv()


ACCESS_KEY_ID = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID = os.getenv("secret_access_key_id")

# Default state (what we start off with at the top of the script/load from config)
def return_default_state():
    return {
        **config_settings,
        "PROD_MODE": False,
        "START_DATE": None,
        "END_DATE": None,
        "PREFIX": "",
    }


my_session = boto3.session.Session(
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
    region_name="us-east-1",
)


class ArgsObject(object):
    def __init__(self, dates=None, dev=None):
        self.dates = dates
        self.dev = dev


from src.loader.preloader import prepstate


class TestPrepstate(object):
    @pytest.mark.parametrize(
        "STATE, event",
        [
            (return_default_state(), {}),
            (return_default_state(), {"asdf": "ass"}),
            (return_default_state(), {"dates": "hello"}),
            (return_default_state(), {"dates": {"start": "2021-09-08", "end": "asdf"}}),
            (return_default_state(), {"dates": {"end": "2021-09-08", "start": "asdf"}}),
            (
                return_default_state(),
                {"dates": {"start": "2021-09-08", "end": "2020-09-08"}},
            ),
        ],
    )
    def test_throws_value_error_for_bad_inputs(self, STATE, event):
        event = defaultdict(lambda: None, event)
        with pytest.raises(ValueError):
            prepstate(event, STATE)

    @pytest.mark.parametrize(
        "STATE, event, expected",
        [
            (
                return_default_state(),
                {"dates": {"start": "2020-09-08", "end": "2021-09-08"}},
                {
                    **return_default_state(),
                    "START_DATE": date.fromisoformat("2020-09-08"),
                    "END_DATE": date.fromisoformat("2021-09-08"),
                },
            ),
            (
                return_default_state(),
                {"dates": {"start": "2020-09-08", "end": "2020-09-08"}},
                {
                    **return_default_state(),
                    "START_DATE": date.fromisoformat("2020-09-08"),
                    "END_DATE": date.fromisoformat("2020-09-08"),
                },
            ),
        ],
    )
    def test_sets_dates_approriately(self, STATE, event, expected):
        event = defaultdict(lambda: None, event)
        actual = prepstate(event, STATE)
        assert actual == expected


from src.loader.preloader import inside_bounds


class TestInsideBounds(object):
    @pytest.mark.parametrize(
        "xtc, STATE, expected",
        [
            (
                "2001-01-01",
                {
                    "START_DATE": date.fromisoformat("1990-04-01"),
                    "END_DATE": date.fromisoformat("2001-01-01"),
                },
                True,
            ),
            (
                "1990-04-01",
                {
                    "START_DATE": date.fromisoformat("1990-04-01"),
                    "END_DATE": date.fromisoformat("2001-01-01"),
                },
                True,
            ),
            (
                "1989-04-01",
                {
                    "START_DATE": date.fromisoformat("1990-04-01"),
                    "END_DATE": date.fromisoformat("2001-01-01"),
                },
                False,
            ),
            (
                "2002-04-01",
                {
                    "START_DATE": date.fromisoformat("1990-04-01"),
                    "END_DATE": date.fromisoformat("2001-01-01"),
                },
                False,
            ),
        ],
    )
    def test_it_works_lol(self, xtc, STATE, expected):

        actual = inside_bounds(" " * 6 + xtc, STATE)
        assert actual == expected

    @pytest.mark.parametrize(
        "xt, STATE",
        [
            (
                "2001-01-01",
                {
                    "START_DATE": date.fromisoformat("1990-04-01"),
                    "END_DATE": date.fromisoformat("2001-01-01"),
                },
            ),
            (
                " " * 16,
                {
                    "START_DATE": date.fromisoformat("1990-04-01"),
                    "END_DATE": date.fromisoformat("2001-01-01"),
                },
            ),
            (
                "QUE PASA",
                {
                    "START_DATE": date.fromisoformat("1990-04-01"),
                    "END_DATE": date.fromisoformat("2001-01-01"),
                },
            ),
        ],
    )
    def test_it_throws_sane_errors(self, xt, STATE):
        with pytest.raises(ValueError):
            inside_bounds(xt, STATE)


# TEST CREATE MANIFESTS

from src.loader.preloader import createManifests


class TestCreateManifest(object):
    @pytest.mark.parametrize(
        "STATE",
        [
            (
                {
                    **return_default_state(),
                }
            ),
        ],
    )
    def test_raises_error_on_invalid_dates(self, STATE):

        with pytest.raises(AttributeError):
            createManifests(STATE)

    @pytest.mark.parametrize(
        "STATE",
        [
            ({}),
            ({"ohai": "quepasas"}),
            ({"invalid key": date(2001, 1, 1)}),
        ],
    )
    def test_raises_error_on_invalid_state(self, STATE):

        with pytest.raises(KeyError):
            createManifests(STATE)

    @pytest.mark.parametrize(
        "STATE, expected_path",
        [
            (
                {
                    **return_default_state(),
                    "START_DATE": date(1999, 1, 1),
                    "END_DATE": date(2000, 12, 31),
                },
                "tests/loader/mock_data/test-009709654492094155fight-manifest-2022-11-14-13-50-38-347682.json",
            ),
        ],
    )
    def test_works_gud(self, STATE, expected_path):
        test_key = f"test-{str(random.random())[2:]}"
        s3c = my_session.client("s3")
        s3r = my_session.resource("s3")
        STATE["PREFIX"] = test_key
        createManifests(STATE)

        ## get fight manifest and delete
        exact_key = s3c.list_objects_v2(
            Bucket=STATE["UFC_META_FILES_LOCATION"],
            Prefix=f"{STATE['LOAD_MANIFEST_FOLDER']}/{STATE['PREFIX']}fight-manifest-",
        )["Contents"][0]["Key"]
        actual_fight_manifest = json.loads(
            (
                s3r.Object(bucket_name=STATE["UFC_META_FILES_LOCATION"], key=exact_key)
                .get()["Body"]
                .read()
            )
        )
        s3r.Object(bucket_name=STATE["UFC_META_FILES_LOCATION"], key=exact_key).delete()

        # get round manifest and delete
        exact_key = s3c.list_objects_v2(
            Bucket=STATE["UFC_META_FILES_LOCATION"],
            Prefix=f"{STATE['LOAD_MANIFEST_FOLDER']}/{STATE['PREFIX']}round-manifest-",
        )["Contents"][0]["Key"]
        actual_round_manifest = json.loads(
            (
                s3r.Object(bucket_name=STATE["UFC_META_FILES_LOCATION"], key=exact_key)
                .get()["Body"]
                .read()
            )
        )
        s3r.Object(bucket_name=STATE["UFC_META_FILES_LOCATION"], key=exact_key).delete()

        with (open(expected_path) as f):
            expected_fight_manifest = json.loads(f.read())

            print(json.dumps(expected_fight_manifest, indent=2, sort_keys=True))
            print(json.dumps(actual_fight_manifest, indent=2, sort_keys=True))
            assert expected_fight_manifest == actual_fight_manifest


from src.loader.preloader import callCopy
from dbhelper import DBHelper


class TestCallCopy(object):
    def test_integration(self):
        STATE = return_default_state()
        db = DBHelper()
        conn = db.getConn()
        cur = db.getCursor()
        cur.execute(
            f""" 
            DROP TABLE IF EXISTS test_{STATE['UFCSTATS_FIGHT_SOURCE_TABLE_NAME']};
            select * into test_{STATE['UFCSTATS_FIGHT_SOURCE_TABLE_NAME']} from {STATE['UFCSTATS_FIGHT_SOURCE_TABLE_NAME']} where false"""
        )
        cur.execute(
            f"""
            DROP TABLE IF EXISTS test_{STATE['UFCSTATS_ROUND_SOURCE_TABLE_NAME']};
            select * into test_{STATE['UFCSTATS_ROUND_SOURCE_TABLE_NAME']} from {STATE['UFCSTATS_ROUND_SOURCE_TABLE_NAME']} where false"""
        )
        conn.commit()

        assert False

    # create two fake tables based on source table   (select into from {source table name})

    # create two fake pieces of data, and a fake manifest pointing to those

    # call callCopy and assert query new table to see if data looks the way it should

    # tear down infrastructure that you set up for the test.
