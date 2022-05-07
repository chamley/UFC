# The first transformer parses a page and returns a new file for s3 where the data for a fight is
# parsed and formatted clearly
# pushes this new file back to S3

# TO-DO list:
#   Make the transformer run in parrallel, see .md
#   Refactor all print to logging.
#   Refactor sanity checks to testing.

from pprint import pprint
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os
import datetime
import logging
import sys
from importlib import reload
import json
from collections import defaultdict

T = datetime.datetime.today()
load_dotenv()

# short on time, I dont know why im forced to do this.
logging.shutdown()
reload(logging)
logging.basicConfig(
    filename=f"logs/first_transformer-{T}.log", encoding="utf-8", level=logging.DEBUG
)


Key_Vector = list[dict]
ACCESS_KEY_ID: str = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID: str | None = os.getenv("secret_access_key_id")
DATE: datetime.date = datetime.date.today()
S3C = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)
S3R = boto3.resource(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)

# refactor this into an argparse + write argpaser.py to process argparsses from all python.
STAGE_LAYER_ONE: str = "ufc-big-data"
STAGE_LAYER_TWO: str = "ufc-big-data-2"

DEV_MODE: bool = False
prefix_string: str = ""
early_exit: bool = False
if DEV_MODE:
    prefix_string = "fight-2022-04-09alexandervolkanovskichansungjung"
    early_exit = True
else:
    prefix_string = ""


def main():
    print(
        "========================= Entering first transformer ======================="
    )
    keys: Key_Vector = get_file_keys()  # O(n)
    print(f"Found {len(keys)} files to transform")
    for item in keys:
        object = S3R.Object(bucket_name=STAGE_LAYER_ONE, key=item["Key"]).get()
        file = object["Body"].read()
        sanity_check(item["Key"], file)
        try:
            fight_data = parse_fight(file)
            push_fight(fight_data, item["Key"])
        except IndexError as e:
            print(f"Index error on {item['Key']}, skipping for now.")
            logging.info(f"Failed on {item['Key']}")
            print(e)
        except AttributeError as e:
            print(f"Attribute error on {item['Key']}, skipping for now.")
            logging.info(f"Failed on {item['Key']}")
            print(e)
    logging.info("Exiting first transformer")


# checks whether our program will make correct assumptions about the structure of the page
def sanity_check(key: str, file) -> None:
    parser = BeautifulSoup(file, "html.parser")

    num_rounds_according_to_page = (
        len(
            parser.find_all(
                class_="b-fight-details__table-row b-fight-details__table-row_type_head"
            )
        )
        / 2
    )
    num_rounds_according_to_table = int(
        parser.find_all("i", class_="b-fight-details__text-item")[0]
        .find("i")
        .next_sibling.strip()
    )

    con1 = num_rounds_according_to_table == num_rounds_according_to_page

    flag = con1

    if not flag:
        print(f"Table dim sanity check failed on {key}")
    else:
        print("Table dim Sanity check passed")


def parse_fight(file):
    # 1. Returns nested dicts.
    # 2. Ugly script that turns a fight page into a giant dict with the relevant data

    d = defaultdict(dict)
    d["red"], d["blue"], d["metadata"] = [
        defaultdict(dict),
        defaultdict(dict),
        defaultdict(dict),
    ]
    parser = BeautifulSoup(file, "html.parser")

    d["red"]["name"], d["blue"]["name"] = [
        x.text for x in parser.find_all(class_="b-link b-fight-details__person-link")
    ]
    d["metadata"]["weight class"] = parser.find(
        class_="b-fight-details__fight-title"
    ).text.strip()

    d["metadata"]["method"] = (
        parser.find(class_="b-fight-details__text-item_first")
        .find_all("i")[1]
        .text.strip()
    )
    d["red"]["result"], d["blue"]["result"] = [
        x.text.strip() for x in parser.find_all(class_="b-fight-details__person-status")
    ]

    (
        d["metadata"]["final_round"],
        d["metadata"]["final_round_duration"],
        d["metadata"]["round_format"],
        _,
    ) = [
        x.find("i").next_sibling.strip()
        for x in parser.find_all("i", class_="b-fight-details__text-item")
    ]
    d["metadata"]["referee"] = parser.find_all(class_="b-fight-details__text-item")[
        3
    ].span.text.strip()

    d["metadata"]["details"] = (
        parser.find_all(class_="b-fight-details__text")[1]
        .text.strip()
        .replace("\n", "")
        .replace("  ", "")
    )
    table_one = parser.find_all(class_="b-fight-details__table-body")[1]
    table_two = parser.find_all(class_="b-fight-details__table-body")[3]

    columns = table_one.find_all(class_="b-fight-details__table-col")
    columns_2 = table_two.find_all(class_="b-fight-details__table-col")

    num_rounds = int(
        len(
            parser.find_all(
                class_="b-fight-details__table-row b-fight-details__table-row_type_head"
            )
        )
        / 2
    )
    for r in range(1, num_rounds + 1):
        index_1 = r + 1 + (r - 1) * 10
        index_2 = r + 3 + (r - 1) * 9
        print(f"round {r}")
        print(f"index {index_1}")

        # index_1 = 2, 13, 24 ... + 11
        # index_2 = 4, 14, ...
        d["red"][f"r{r}"]["kd"], d["blue"][f"r{r}"]["kd"] = [
            x.text.strip()
            for x in columns[index_1].find_all(class_="b-fight-details__table-text")
        ]
        d["red"][f"r{r}"]["ss_l"], d["red"][f"r{r}"]["ss_a"] = clean(
            columns[index_1 + 1].find_all(class_="b-fight-details__table-text")[0].text
        )
        d["blue"][f"r{r}"]["ss_l"], d["blue"][f"r{r}"]["ss_a"] = clean(
            columns[index_1 + 1].find_all(class_="b-fight-details__table-text")[1].text
        )

        d["red"][f"r{r}"]["ts_l"], d["red"][f"r{r}"]["ts_a"] = clean(
            columns[index_1 + 3].find_all(class_="b-fight-details__table-text")[0].text
        )
        d["blue"][f"r{r}"]["ts_l"], d["blue"][f"r{r}"]["ts_a"] = clean(
            columns[index_1 + 3].find_all(class_="b-fight-details__table-text")[1].text
        )

        d["red"][f"r{r}"]["td_l"], d["red"][f"r{r}"]["td_a"] = clean(
            columns[index_1 + 4].find_all(class_="b-fight-details__table-text")[0].text
        )
        d["blue"][f"r{r}"]["td_l"], d["blue"][f"r{r}"]["td_a"] = clean(
            columns[index_1 + 4].find_all(class_="b-fight-details__table-text")[1].text
        )

        d["red"][f"r{r}"]["sub_a"], d["blue"][f"r{r}"]["sub_a"] = [
            x.text.strip()
            for x in columns[index_1 + 6].find_all(class_="b-fight-details__table-text")
        ]
        d["red"][f"r{r}"]["rev"], d["blue"][f"r{r}"]["rev"] = [
            x.text.strip()
            for x in columns[index_1 + 7].find_all(class_="b-fight-details__table-text")
        ]
        d["red"][f"r{r}"]["ctrl"], d["blue"][f"r{r}"]["ctrl"] = [
            x.text.strip()
            for x in columns[index_1 + 8].find_all(class_="b-fight-details__table-text")
        ]
        ### table 2
        (
            [d["red"][f"r{r}"]["ss_l_h"], d["red"][f"r{r}"]["ss_a_h"]],
            [d["blue"][f"r{r}"]["ss_l_h"], d["blue"][f"r{r}"]["ss_a_h"]],
        ) = [
            clean(x.text)
            for x in columns_2[index_2].find_all(class_="b-fight-details__table-text")
        ]

        (
            [d["red"][f"r{r}"]["ss_l_b"], d["red"][f"r{r}"]["ss_a_b"]],
            [d["blue"][f"r{r}"]["ss_l_b"], d["blue"][f"r{r}"]["ss_a_b"]],
        ) = [
            clean(x.text)
            for x in columns_2[index_2 + 1].find_all(
                class_="b-fight-details__table-text"
            )
        ]
        (
            [d["red"][f"r{r}"]["ss_l_l"], d["red"][f"r{r}"]["ss_a_l"]],
            [d["blue"][f"r{r}"]["ss_l_l"], d["blue"][f"r{r}"]["ss_a_l"]],
        ) = [
            clean(x.text)
            for x in columns_2[index_2 + 2].find_all(
                class_="b-fight-details__table-text"
            )
        ]
        (
            [d["red"][f"r{r}"]["ss_l_dist"], d["red"][f"r{r}"]["ss_a_dist"]],
            [d["blue"][f"r{r}"]["ss_l_dist"], d["blue"][f"r{r}"]["ss_a_dist"]],
        ) = [
            clean(x.text)
            for x in columns_2[index_2 + 3].find_all(
                class_="b-fight-details__table-text"
            )
        ]
        (
            [d["red"][f"r{r}"]["ss_l_cl"], d["red"][f"r{r}"]["ss_a_cl"]],
            [d["blue"][f"r{r}"]["ss_l_cl"], d["blue"][f"r{r}"]["ss_a_cl"]],
        ) = [
            clean(x.text)
            for x in columns_2[index_2 + 4].find_all(
                class_="b-fight-details__table-text"
            )
        ]
        (
            [d["red"][f"r{r}"]["ss_l_gr"], d["red"][f"r{r}"]["ss_a_gr"]],
            [d["blue"][f"r{r}"]["ss_l_gr"], d["blue"][f"r{r}"]["ss_a_gr"]],
        ) = [
            clean(x.text)
            for x in columns_2[index_2 + 5].find_all(
                class_="b-fight-details__table-text"
            )
        ]

    # n = columns_2[12].find_all(class_="b-fight-details__table-text")
    # print(n)

    # print(json.dumps(d, sort_keys=True, indent=4))

    return d


def clean(s):
    return [x.strip() for x in s.split("of")]


def get_file_keys() -> Key_Vector:
    keys: Key_Vector = []
    res: dict = S3C.list_objects_v2(Bucket=STAGE_LAYER_ONE, Prefix=prefix_string)
    while True:
        items = res["Contents"]
        for i in items:
            keys.append(i)
        if not "NextContinuationToken" in res:
            break
        t = res["NextContinuationToken"]

        res = S3C.list_objects_v2(
            Bucket=STAGE_LAYER_ONE, Prefix=prefix_string, ContinuationToken=t
        )
    return keys


# pushes json object back to s3
def push_fight(fight_data, key):
    file_name = f"{key}-SL-2.json"
    print(f"Trying to write: {file_name}   to: {STAGE_LAYER_TWO}   ...")
    S3C.put_object(Body=json.dumps(fight_data), Bucket=STAGE_LAYER_TWO, Key=file_name)
    print(f"Written: {file_name}   to: {STAGE_LAYER_TWO} successfully!")
    # with open(file_name, "w") as f:
    #     x = json.dumps(fight_data, indent=4)
    #     f.write(x)
    #     S3C.upload_file(file_name, STAGE_LAYER_TWO, file_name)


main()
