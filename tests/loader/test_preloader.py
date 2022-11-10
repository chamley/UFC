import sys

sys.path.append(".")
sys.path.append("src/loader/")


from collections import defaultdict
from datetime import date


import pytest
from configfile import STAGE_LAYER_TWO, REGION_NAME
from src.loader.preloader import prepstate


# Default state (what we start off with at the top of the script/load from config)
def return_default_state():
    return {
        "PROD_MODE": False,
        "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
        "REGION_NAME": REGION_NAME,
        "START_DATE": None,
        "END_DATE": None,
    }


class ArgsObject(object):
    def __init__(self, dates=None, dev=None):
        self.dates = dates
        self.dev = dev


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


# class TestPrepScript(object):
#     # 3 tests here seen in code + 2 Value Errors from isoformsat errors
#     @pytest.mark.parametrize(
#         "STATE, event, args",
#         [
#             (return_default_state(), {}, ArgsObject(dates=["asdf", "111"])),
#             (return_default_state(), {"dates": {"start": "asdf", "end": "aaa"}}, None),
#             (
#                 return_default_state(),
#                 {"dates": {"start": "2022-01-01", "end": "2021-01-01"}},
#                 None,
#             ),
#             (
#                 return_default_state(),
#                 {},
#                 ArgsObject(dates=["2022-03-04", "2005-03-04"]),
#             ),
#             (return_default_state(), {}, ArgsObject()),
#         ],
#     )
#     def test_throws_value_error_for_incorrect_inputs(self, STATE, event, args):
#         with pytest.raises(ValueError):
#             prep_script(STATE, event, args)

# @pytest.mark.parametrize(
#     "STATE, event, args, expected",
#     [
#         (
#             return_default_state(),
#             defaultdict(lambda: None, {"dev": "True"}),
#             ArgsObject(),
#             {**return_default_state(), "DEV_MODE": True, "PROD_MODE": True},
#         ),
#         (
#             return_default_state(),
#             defaultdict(
#                 lambda: None,
#                 {"dates": {"start": "2021-04-04", "end": "2021-05-05"}},
#             ),
#             ArgsObject(),
#             {
#                 **return_default_state(),
#                 "PROD_MODE": True,
#                 "DATE_SPECIFIED": True,
#                 "START_DATE": date.fromisoformat("2021-04-04"),
#                 "END_DATE": date.fromisoformat("2021-05-05"),
#             },
#         ),
#         (
#             return_default_state(),
#             defaultdict(lambda: None, {}),
#             ArgsObject(dates=["2015-04-04", "2016-06-05"]),
#             {
#                 **return_default_state(),
#                 "DATE_SPECIFIED": True,
#                 "START_DATE": date.fromisoformat("2015-04-04"),
#                 "END_DATE": date.fromisoformat("2016-06-05"),
#             },
#         ),
#         (
#             return_default_state(),
#             defaultdict(lambda: None, {}),
#             ArgsObject(dev=True),
#             {**return_default_state(), "DEV_MODE": True},
#         ),
#     ],
# )
# def test_correct_end_state_for_valid_inputs(self, STATE, event, args, expected):
#     actual = prep_script(STATE, event, args)
#     assert expected == actual
