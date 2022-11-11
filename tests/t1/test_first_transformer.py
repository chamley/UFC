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
    def test_validates_correctly(self, file, expected):
        actual = sanity_check("dummy_key", file)
        assert actual == expected


from src.t1.ufcstats_t1 import parse_fight


class TestParseFight(object):
    @pytest.mark.parametrize(
        "file, rounds, fight, key",
        [
            (
                open("tests/t1/mock_data/test-andrade-valid.html"),
                [
                    {
                        "kd": "0",
                        "ss_l": "3",
                        "ss_a": "9",
                        "ts_l": "3",
                        "ts_a": "9",
                        "td_l": "0",
                        "td_a": "1",
                        "sub_a": "1",
                        "rev": "0",
                        "ctrl": "0:38",
                        "ss_l_h": "0",
                        "ss_a_h": "3",
                        "ss_l_b": "1",
                        "ss_a_b": "1",
                        "ss_l_l": "2",
                        "ss_a_l": "5",
                        "ss_l_dist": "3",
                        "ss_a_dist": "9",
                        "ss_l_cl": "0",
                        "ss_a_cl": "0",
                        "ss_l_gr": "0",
                        "ss_a_gr": "0",
                        "fighter_name": "jessica andrade",
                        "fighter_id": "6a1901c62ab3870f",
                        "fight_key_nat": "fight-2022-04-23amandalemosjessicaandrade.txt",
                        "round": 1,
                    },
                    {
                        "kd": "0",
                        "ss_l": "7",
                        "ss_a": "17",
                        "ts_l": "7",
                        "ts_a": "17",
                        "td_l": "0",
                        "td_a": "0",
                        "sub_a": "0",
                        "rev": "0",
                        "ctrl": "0:00",
                        "ss_l_h": "4",
                        "ss_a_h": "12",
                        "ss_l_b": "0",
                        "ss_a_b": "2",
                        "ss_l_l": "3",
                        "ss_a_l": "3",
                        "ss_l_dist": "7",
                        "ss_a_dist": "17",
                        "ss_l_cl": "0",
                        "ss_a_cl": "0",
                        "ss_l_gr": "0",
                        "ss_a_gr": "0",
                        "fighter_name": "amanda lemos",
                        "fighter_id": "3df5493bb279226f",
                        "fight_key_nat": "fight-2022-04-23amandalemosjessicaandrade.txt",
                        "round": 1,
                    },
                ],
                {
                    "fight_key_nat": "fight-2022-04-23amandalemosjessicaandrade.txt",
                    "red_fighter_name": "amanda lemos",
                    "red_fighter_id": "3df5493bb279226f",
                    "blue_fighter_name": "jessica andrade",
                    "blue_fighter_id": "6a1901c62ab3870f",
                    "winner_fighter_name": "jessica andrade",
                    "winner_fighter_id": "6a1901c62ab3870f",
                    "details": "details:arm triangle standing",
                    "final_round": 1,
                    "final_round_duration": "3:13",
                    "method": "submission",
                    "referee": "keith peterson",
                    "round_format": "5 rnd (5-5-5-5-5)",
                    "weight_class": "women's strawweight bout",
                    "fight_date": "2022-04-23",
                    "is_title_fight": 0,
                    "wmma": 1,
                    "wc": "wsw",
                },
                "fight-2022-04-23amandalemosjessicaandrade.txt",
            )
        ],
    )

    # 'fight-2022-04-23amandalemosjessicaandrade.txt'

    def test_parses_correctly(self, file, rounds, fight, key):

        r, f, k = parse_fight(file, key)
        assert (r == rounds) and (f == fight) and (k == key)


class TestPushData(object):
    @pytest.mark.parametrize("ohai", [("ohai")])
    def test_notsure(self, ohai):
        assert True
