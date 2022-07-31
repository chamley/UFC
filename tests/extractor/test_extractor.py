import sys

sys.path.append(".")

from src.extractor.extractor import (
    main,
    stage_layer_1,
    get_card_urls_dic,
    get_fight_url_list,
    create_fight_page,
    push_fight_page,
)
import pytest
from moto import mock_s3


class TestCreateFightPage(object):
    def test_parses_page_properly(self):
        assert True

    def test_raises_error_if_bad_url(self):
        pass
