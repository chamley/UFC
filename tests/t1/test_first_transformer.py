# ToDo:
# 1. We should probably test somewhere for credentials in a meaningful way as it is a fairly probably source of error


import sys

sys.path.append(".")
sys.path.append("src/t1")


from collections import defaultdict
from datetime import date


import pytest
from configfile import STAGE_LAYER_ONE, STAGE_LAYER_TWO, REGION_NAME
from src.t1.ufcstats_t1 import prepstate, get_keys
from src.t1.t1_exceptions import InvalidDates

# Default state (what we start off with at the top of the script/load from config)


def return_default_state():
    return {
        "STAGE_LAYER_ONE": STAGE_LAYER_ONE,
        "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
        "REGION_NAME": REGION_NAME,
        "START_DATE": None,
        "END_DATE": None,
        "TODAY": date.today(),
        "PREFIX_STRING": "",
    }


class TestPrepstate(object):
    @pytest.mark.parametrize(
        "STATE, event",
        [
            (return_default_state(), {}),
            (return_default_state(), {"asdf": "asaas"}),
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


from mock_data.mock_data_get_keys import mock_keys


class TestGetKeys(object):
    @pytest.mark.parametrize(
        "STATE, expected",
        [
            (
                {
                    **return_default_state(),
                    "START_DATE": date(1990, 1, 1),
                    "END_DATE": date(2001, 1, 1),
                },
                mock_keys["keys_1"],
            ),
            (
                {
                    **return_default_state(),
                    "START_DATE": date(1985, 1, 1),
                    "END_DATE": date(1985, 1, 1),
                },
                [],
            ),
            (
                {
                    **return_default_state(),
                    "START_DATE": date(2050, 1, 1),
                    "END_DATE": date(2050, 1, 1),
                },
                [],
            ),
            (
                {
                    **return_default_state(),
                    "START_DATE": date(2019, 1, 1),
                    "END_DATE": date(2022, 1, 1),
                },
                mock_keys["keys_2"],
            ),
        ],
    )
    def test_proper_keys_retrieved(self, STATE, expected):
        actual = get_keys(STATE)
        assert actual == expected


from src.t1.ufcstats_t1 import sanity_check
from src.t1.t1_exceptions import InvalidHTMLTableDimensions


class TestSanityCheck(object):
    @pytest.mark.parametrize(
        "file, expected",
        [(open("tests/t1/mock_data/test-andrade-valid.html"), True)],
    )
    def validates_correctly(self, file, expected):
        actual = sanity_check("dummy_key", file)
        assert actual == expected
