import sys

sys.path.append(".")

import pytest
from configfile import STAGE_LAYER_TWO, REGION_NAME

from src.loader.preloader import prep_script

# Default General Args
DEF_GA = {
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
            (DEF_GA, {}, ArgsObject(dates=["asdf", "111"])),
            (DEF_GA, {"dates": {"start": "asdf", "end": "aaa"}}, None),
            (DEF_GA, {"dates": {"start": "2022-01-01", "end": "2021-01-01"}}, None),
            (DEF_GA, {}, ArgsObject(dates=["2022-03-04", "2005-03-04"])),
            (DEF_GA, {}, ArgsObject()),
        ],
    )
    def test_throws_value_error_for_incorrect_inputs(self, GA, event, args):
        with pytest.raises(ValueError):
            prep_script(GA, event, args)

    # def test_correct_end_state_for_valid_inputs(self):
    #     pass

    def testhello(self):
        assert True
