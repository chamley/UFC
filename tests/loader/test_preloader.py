import sys
from collections import defaultdict
from datetime import date

sys.path.append(".")

import pytest
from configfile import STAGE_LAYER_TWO, REGION_NAME

from src.loader.preloader import prep_script

# Default General Args (what we start off with at the top of the script/load from config)
def return_defga():
    return {
        "PROD_MODE": False,
        "DEV_MODE": False,
        "DATE_SPECIFIED": False,
        "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
        "REGION_NAME": REGION_NAME,
        "START_DATE": None,
        "END_DATE": None,
    }


class ArgsObject(object):
    def __init__(self, dates=None, dev=None):
        self.dates = dates
        self.dev = dev


class TestPrepScript(object):
    # 3 tests here seen in code + 2 Value Errors from isoformsat errors
    @pytest.mark.parametrize(
        "GA, event, args",
        [
            (return_defga(), {}, ArgsObject(dates=["asdf", "111"])),
            (return_defga(), {"dates": {"start": "asdf", "end": "aaa"}}, None),
            (
                return_defga(),
                {"dates": {"start": "2022-01-01", "end": "2021-01-01"}},
                None,
            ),
            (return_defga(), {}, ArgsObject(dates=["2022-03-04", "2005-03-04"])),
            (return_defga(), {}, ArgsObject()),
        ],
    )
    def test_throws_value_error_for_incorrect_inputs(self, GA, event, args):
        with pytest.raises(ValueError):
            prep_script(GA, event, args)

    @pytest.mark.parametrize(
        "GA, event, args, expected",
        [
            (
                return_defga(),
                defaultdict(lambda: None, {"dev": "True"}),
                ArgsObject(),
                {**return_defga(), "DEV_MODE": True, "PROD_MODE": True},
            ),
            (
                return_defga(),
                defaultdict(
                    lambda: None,
                    {"dates": {"start": "2021-04-04", "end": "2021-05-05"}},
                ),
                ArgsObject(),
                {
                    **return_defga(),
                    "PROD_MODE": True,
                    "DATE_SPECIFIED": True,
                    "START_DATE": date.fromisoformat("2021-04-04"),
                    "END_DATE": date.fromisoformat("2021-05-05"),
                },
            ),
            (
                return_defga(),
                defaultdict(lambda: None, {}),
                ArgsObject(dates=["2015-04-04", "2016-06-05"]),
                {
                    **return_defga(),
                    "DATE_SPECIFIED": True,
                    "START_DATE": date.fromisoformat("2015-04-04"),
                    "END_DATE": date.fromisoformat("2016-06-05"),
                },
            ),
            (
                return_defga(),
                defaultdict(lambda: None, {}),
                ArgsObject(dev=True),
                {**return_defga(), "DEV_MODE": True},
            ),
        ],
    )
    def test_correct_end_state_for_valid_inputs(self, GA, event, args, expected):
        actual = prep_script(GA, event, args)
        assert expected == actual
