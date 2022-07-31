import sys


sys.path.append(".")
sys.path.append("./tests/extractor")

from src.extractor.extractor import (
    main,
    stage_layer_1,
    get_card_urls_dic,
    get_fight_url_list,
    create_fight_page,
    push_fight_page,
)
from configfile import STAGE_LAYER_ONE, REGION_NAME
import pytest
import boto3
from moto import mock_s3


class fakeRequest(object):
    def __init__(self, f) -> None:
        self.text = f


class TestCreateFightPage(object):
    @pytest.mark.parametrize(
        "data, date, expected",
        [
            (
                fakeRequest(
                    open("tests/extractor/mock_inputs/test-volk-data.html").read()
                ),
                "2022-07-02",
                [
                    open("tests/extractor/mock_inputs/test-volk-expected.html").read(),
                    "AlexanderVolkanovskiMaxHolloway",
                ],
            ),
            (
                fakeRequest(
                    open("tests/extractor/mock_inputs/test-andrade-data.html").read()
                ),
                "2022-04-23",
                [
                    open(
                        "tests/extractor/mock_inputs/test-andrade-expected.html"
                    ).read(),
                    "AmandaLemosJessicaAndrade",
                ],
            ),
        ],
    )
    def test_creates_page_properly(self, data, date, expected, mocker):
        mock_req_get = mocker.patch("src.extractor.extractor.requests")
        mock_req_get.get.return_value = data
        actual_page, actual_names = create_fight_page("", date)

        assert actual_page in expected[0] and expected[0] in actual_page
        assert actual_names == expected[1]


@mock_s3
class TestPushFightPage(object):
    def test_pushes_fight_page_correctly(self):
        s3c = boto3.client("s3", region=REGION_NAME)
        s3c.create_bucket(Bucket=STAGE_LAYER_ONE)
        push_fight_page(fight_page, object_name)

        assert True
