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

# need an object with a .text field to properly test function
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

        assert actual_page == expected[0]
        assert actual_names == expected[1]


@mock_s3
class TestPushFightPage(object):
    @pytest.mark.parametrize(
        "fight_page, object_name",
        [
            (
                open("tests/extractor/mock_inputs/test-volk-data.html").read(),
                "fight-2022-07-02-alexandervolkanovskimaxholloway.txt",
            ),
            (
                open("tests/extractor/mock_inputs/test-andrade-expected.html").read(),
                "fight-2022-04-23-amandalemosjessicaandrade.txt",
            ),
        ],
    )
    def test_pushes_fight_page_correctly(self, fight_page, object_name):
        s3c = boto3.client("s3", REGION_NAME)
        s3c.create_bucket(Bucket=STAGE_LAYER_ONE)
        push_fight_page(fight_page, object_name)

        actual = (
            s3c.get_object(Bucket=STAGE_LAYER_ONE, Key=object_name)["Body"]
            .read()
            .decode("utf-8")
        )

        assert actual == fight_page


class TestGetFightUrlList(object):
    # leaving url in for ref, even though not used
    @pytest.mark.parametrize(
        "url,data,expected",
        [
            (
                "http://ufcstats.com/event-details/b0a6124751a56bc4",
                fakeRequest(
                    open(
                        "tests/extractor/mock_inputs/test-get-fight-url-list-1.html"
                    ).read()
                ),
                [
                    "http://ufcstats.com/fight-details/72c3e5eacde4f0e5",
                    "http://ufcstats.com/fight-details/5a1ce88ec1f75160",
                    "http://ufcstats.com/fight-details/52f87252af59e23b",
                    "http://ufcstats.com/fight-details/96b62bd56b252bab",
                    "http://ufcstats.com/fight-details/058c2724dec1add1",
                    "http://ufcstats.com/fight-details/579b5770129b328a",
                    "http://ufcstats.com/fight-details/81a18de517a3336a",
                    "http://ufcstats.com/fight-details/5a2b86570110191b",
                    "http://ufcstats.com/fight-details/189b9af881b96de6",
                    "http://ufcstats.com/fight-details/e492dc60d8e44866",
                    "http://ufcstats.com/fight-details/0366e183296fea07",
                    "http://ufcstats.com/fight-details/1ed550f094ca7745",
                    "http://ufcstats.com/fight-details/66f7bb31d24b3b36",
                ],
            ),
            (
                "http://ufcstats.com/event-details/eb42d4febfafefd1",
                fakeRequest(
                    open(
                        "tests/extractor/mock_inputs/test-get-fight-url-list-2.html"
                    ).read()
                ),
                [
                    "http://ufcstats.com/fight-details/1f5f59924b59408b",
                    "http://ufcstats.com/fight-details/3c98739eb42f96bf",
                    "http://ufcstats.com/fight-details/c57c8e22a3e8dde2",
                    "http://ufcstats.com/fight-details/029a12d36a4687e1",
                    "http://ufcstats.com/fight-details/28bd070f7ab0a0b9",
                    "http://ufcstats.com/fight-details/3f38100fd8dae286",
                    "http://ufcstats.com/fight-details/28be3fbe5c377380",
                    "http://ufcstats.com/fight-details/21f79cd40513d075",
                    "http://ufcstats.com/fight-details/a457331c27bce6d5",
                    "http://ufcstats.com/fight-details/8cc43d79b61efc25",
                    "http://ufcstats.com/fight-details/24a9fc95a3112f51",
                    "http://ufcstats.com/fight-details/2c73bf152247f25a",
                ],
            ),
        ],
    )
    def test_that_none_are_missing_or_extra(self, url, data, expected, mocker):
        mock_req_get = mocker.patch("src.extractor.extractor.requests")
        mock_req_get.get.return_value = data

        actual = get_fight_url_list(url)
        assert actual == expected


# class TestGetCardUrlsDic(object):
#     def test_exhaustive_list_returned(self, mocker):
#         mocker_requets_get = mocker.patch("src.extractor.extractor.requests")
#         mocker.get.response_value = ""
