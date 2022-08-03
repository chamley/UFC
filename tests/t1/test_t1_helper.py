import sys

sys.path.append("./src/t1")

from src.t1.t1_helper import my_argument_parser


class TestMyArgumentParser(object):
    def test_arguments_are_parsed(self):
        parser = my_argument_parser()
        assert bool(parser.parse_args()), "has the correct module used"
