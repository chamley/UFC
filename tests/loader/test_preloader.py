import sys

sys.path.append(".")
sys.path.append("src/loader/")


from collections import defaultdict
from datetime import date


import pytest
from configfile import STAGE_LAYER_TWO, REGION_NAME
from src.loader.preloader import prepstate
import random

# Default state (what we start off with at the top of the script/load from config)
def return_default_state():
    return {
        "PROD_MODE": False,
        "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
        "REGION_NAME": REGION_NAME,
        "START_DATE": None,
        "END_DATE": None,
        "PREFIX": "",
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
        "STATE",
        [
            (
                {
                    **return_default_state(),
                    "START_DATE": date(1999, 1, 1),
                    "END_DATE": date(2000, 12, 31),
                }
            ),
        ],
    )
    def test_works_gud(self, STATE):
        test_key = f"test-{str(random.random())[2:]}"
        STATE["PREFIX"] = test_key

        createManifests(STATE)


# TEST CALL COPY
