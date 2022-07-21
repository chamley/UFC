"""
Desired behavior:
    - backfill: look at all missing files in SL2 and try to backfill them with files in SL1, output any missing files.
    - daterange: look at only files in SL1 within a certain date range and process only those
    - read a csv of file names (in SL1) and process only those ones.


GLOBAL RULES:
    - never overwrite files, skip and output an error in logs

"""


# The first transformer parses a page and returns a new file for s3 where the data for a fight is
# parsed and formatted clearly
# pushes this new file back to S3
# TO-DO list:
#   Make the transformer run in parrallel, see .md
#   Refactor all print to logging.
#   Refactor sanity checks to testing.

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
import pandas as pd
import awswrangler as wr
from t1_argumentparser import my_argument_parser
from datetime import date


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

STAGE_LAYER_ONE: str = "ufc-big-data"  # grab files from this s3 bucket
STAGE_LAYER_TWO: str = "ufc-big-data-2"  # put files in this s3 bucket

DEV_MODE: bool = False
prefix_string: str = ""

args = my_argument_parser().parse_args()

if args.dev:
    DEV_MODE: bool = True
elif args.dates:
    try:
        START_DATE = date.fromisoformat(args.dates[0])
        END_DATE = date.fromisoformat(args.dates[1])
        if END_DATE < START_DATE:
            raise Exception
        print(f"transforming fights from {START_DATE} to {END_DATE}")
    except:
        print("invalid dates")
        sys.exit()
elif args.csv:
    print(f"fetching file in {args.csv}")
    pass  # do the stuff


if DEV_MODE:
    prefix_string = "fight-2022-04-09alexandervolkanovskichansungjung"  # "fight-2020-11-28anthonysmithdevinclark"  # "fight-2020-11-28ashleeevans-smithnormadumont"  #
else:
    prefix_string = ""


def main():
    print(
        "========================= Entering first transformer ======================="
    )
    keys: Key_Vector = get_file_keys()  # O(n)
    print(f"Found {len(keys)} files to transform")
    fights = []
    rounds = []
    for item in keys:
        object = S3R.Object(bucket_name=STAGE_LAYER_ONE, key=item["Key"]).get()
        file = object["Body"].read()
        sanity_check(item["Key"], file)
        try:
            fight_data = parse_fight(file)

            fix_data(
                fight_data, item["Key"][:-4]
            )  # easier than rewriting the scraper code, just takes the mess of json and puts it in clean csv
            # fights.append(fight_data)
            # [rounds.append(x) for x in fixed_round_data]
            # push_fight(fight_data, item["Key"])
        except IndexError as e:
            print(f"Index error on {item['Key']}, skipping for now.")
            logging.info(f"Failed on {item['Key']}")
            print(e)
        # except AttributeError as e:
        #     print(f"Attribute error on {item['Key']}, skipping for now.")
        #     logging.info(f"Failed on {item['Key']}")
        #     print(e)
    logging.info("Successfuly exiting first transformer")
    print("Successfuly exiting first transformer")


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
        print(f"Tabile dim sanity check failed on {key}")
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

    d["red"]["id"], d["blue"]["id"] = [
        x.get("href").split("/")[-1]
        for x in parser.find_all(class_="b-link b-fight-details__person-link")
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
    ## debug start

    lookahead = parser.find_all("i", class_="b-fight-details__text-item")
    if len(lookahead) == 7:
        (
            d["metadata"]["final_round"],
            d["metadata"]["final_round_duration"],
            d["metadata"]["round_format"],
            d["metadata"]["referee"],
            *details,
        ) = [x.text.strip().replace("\n", "").replace("  ", "") for x in lookahead]
        d["metadata"]["details"] = "#".join(details)
        print(json.dumps(d, sort_keys=True, indent=4))
    else:
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

    # entering loop

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

    return d


# returns an array of round (dict) and a single fight (dict)
def fix_data(d, k):
    rounds = []
    fight = {}

    if len(d["metadata"]["final_round"]):
        last_round = int(d["metadata"]["final_round"][-1])
    else:
        last_round = int(d["metadata"]["final_round"])

    for i in range(last_round):
        n = i + 1
        b = d["blue"][f"r{n}"]
        r = d["red"][f"r{n}"]

        b["fighter_id"] = d["blue"]["id"]
        r["fighter_id"] = d["red"]["id"]
        b["fight_key_nat"] = k
        r["fight_key_nat"] = k
        b["round"] = n
        r["round"] = n
        rounds.append(b)
        rounds.append(r)

    fight["fight_key_nat"] = k
    fight["red_fighter_name"] = d["red"]["name"].lower().strip()
    fight["red_fighter_id"] = d["red"]["id"]
    fight["blue_fighter_name"] = d["blue"]["name"].lower().strip()
    fight["blue_fighter_id"] = d["blue"]["id"]

    fight["winner_fighter_name"] = (
        d["blue"]["name"].lower().strip()
        if d["blue"]["result"] == "W"
        else d["blue"]["name"].lower().strip()
    )
    fight["winner_fighter_id"] = (
        d["blue"]["id"] if d["blue"]["result"] == "W" else d["blue"]["id"]
    )
    fight["details"] = d["metadata"]["details"].lower().strip()
    fight["final_round"] = last_round
    fight["final_round_duration"] = d["metadata"]["final_round_duration"].replace(
        "Time:", ""
    )
    fight["method"] = d["metadata"]["method"].lower().strip()
    fight["referee"] = d["metadata"]["referee"].replace("Referee:", "").lower().strip()
    fight["round_format"] = (
        d["metadata"]["round_format"].replace("Time format:", "").lower().strip()
    )
    fight["weight_class"] = d["metadata"]["weight class"].lower().strip()
    fight["fight_date"] = k[6:16]
    fight["is_title_fight"] = (
        1 if "title" in d["metadata"]["weight class"].lower() else 0
    )
    fight["wmma"] = 1 if "women" in d["metadata"]["weight class"].lower() else 0
    fight["wc"] = format_weight_class(
        d["metadata"]["weight class"].lower(), fight["wmma"]
    )

    wr.s3.to_parquet(
        pd.DataFrame(rounds),
        path=f"s3://ufc-big-data-2/{k}-rounds.parquet.gzip",
        compression="gzip",
    )
    wr.s3.to_csv(
        pd.DataFrame(fight, index=[0]), path=f"s3://ufc-big-data-2/{k}-fight.csv"
    )

    # pd.DataFrame(rounds).to_parquet(f"{k}-rounds.parquet.gzip", compression="gzip")
    # pd.DataFrame(fight, index=[0]).to_csv(f"{k}-fight.csv")

    # fightkey, fighterkey, round_key, fight_keynat, [.. stats]
    # fightkeynat,  red fighter key, winner_key, details, final round, final round duration, method, referee, round_format, weight class, fight date, is title fight  wmma, wc

    return 1


def format_weight_class(s, wmma):
    if not wmma:
        if "lightweight" in s:
            return "lw"
        elif "featherweight" in s:
            return "few"
        elif "bantamweight" in s:
            return "bw"
        elif "flyweight" in s:
            return "flw"
        elif "welterweight" in s:
            return "ww"
        elif "middleweight" in s:
            return "mw"
        elif "heavyweight" in s:
            return "hw"
        elif "light heavyweight" in s:
            return "lhw"
        else:
            return "catchweight"
    else:
        if "strawweight" in s:
            return "wsw"
        elif "bantamweight" in s:
            return "wbw"
        elif "featherweight" in s:
            return "wfew"
        elif "flyweight" in s:
            return "wflw"
        else:
            return "wcatchweight"


def clean(s):
    return [x.strip() for x in s.split("of")]


# use the args specified to
def get_file_keys() -> Key_Vector:
    keys: Key_Vector = []
    res: dict = S3C.list_objects_v2(Bucket=STAGE_LAYER_ONE, Prefix=prefix_string)

    clean_dates: list
    count_dic = defaultdict(int)
    if args.csv:
        # very important: at the end somewhere output
        # how many file were found to
        # be transformed for each date. eg: 4 fights on 2022-01-16, 0 fights for 2022-01-17 etc..

        dates_csv = (
            S3R.Object("ufc-meta-files", f"t1-dates/{args.csv}")
            .get()["Body"]
            .read()
            .decode("utf-8")
            .split(",")
        )
        # check for trailing comma
        clean_dates = list(filter(lambda x: x, dates_csv))

    while True:
        items = res["Contents"]
        for i in items:

            if args.dates:
                d = date.fromisoformat(i["Key"][6:16])
                if not (START_DATE <= d and d <= END_DATE):
                    continue
            if args.csv:
                d = i["Key"][6:16]
                print(len(d), len(clean_dates[0]))
                if d == clean_dates[0]:
                    print("trueeeeeeee $$$$")
                    count_dic[d] += 1
                else:
                    continue

            keys.append(i)
        if not "NextContinuationToken" in res:
            break
        t = res["NextContinuationToken"]

        res = S3C.list_objects_v2(
            Bucket=STAGE_LAYER_ONE, Prefix=prefix_string, ContinuationToken=t
        )

    if args.csv:
        for k, v in count_dic.items():
            print(f"Found {v} file(s) for date: {k}")
    return keys


if __name__ == "__main__":
    main()
