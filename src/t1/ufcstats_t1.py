import sys

sys.path.append(".")


from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
import os
import datetime
import sys
from importlib import reload
import json
from collections import defaultdict
import pandas as pd
import awswrangler as wr
from t1_helper import my_argument_parser
from t1_exceptions import InvalidDates, InvalidHTMLTableDimensions
from datetime import date
from configfile import STAGE_LAYER_ONE, STAGE_LAYER_TWO, REGION_NAME
import logging

load_dotenv()


ACCESS_KEY_ID = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID = os.getenv("secret_access_key_id")
DATE = date.today()
S3C = boto3.client(
    "s3",
    region_name=REGION_NAME,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)
S3R = boto3.resource(
    "s3",
    region_name=REGION_NAME,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID,
)

STATE = {
    "STAGE_LAYER_ONE": STAGE_LAYER_ONE,
    "STAGE_LAYER_TWO": STAGE_LAYER_TWO,
    "REGION_NAME": REGION_NAME,
    "START_DATE": None,
    "END_DATE": None,
    "TODAY": datetime.datetime.today(),
    "PREFIX_STRING": "",
}


# Nethod to Invoke
def main(event={}, context=None) -> None:
    # required here and nowhere else
    global STATE
    event = defaultdict(lambda: None, event)
    STATE = prepstate(event, STATE)

    # find all valid keys to transform
    keys = get_keys(STATE)
    logging.info(f"We found {len(keys)} elements to transform")
    # We assure idempotency on write as we may be missing a
    # fight file or a rounds file but not its partner

    for item in keys:
        object = S3R.Object(bucket_name=STATE["STAGE_LAYER_ONE"], key=item["Key"]).get()
        file = object["Body"].read()
        try:
            sanity_check(item["Key"], file)
        except:
            logging.error(f"HTML not parsable on {item['Key']}, skipping for now.")

        try:
            # I apologize for this
            # fight_data = parse_fight(file, item["Key"][:-4])
            # fix_data(fight_data, item["Key"][:-4])

            rounds, fight, k = parse_fight(file, item["Key"][:-4])
            push_data(rounds, fight, k)

        except InvalidHTMLTableDimensions as e:
            print(f"HTML not parsable on {item['Key']}, skipping for now.")

    logging.info("Successfully exiting first transformer")


def push_data(rounds, fight, k, STATE=STATE) -> None:
    wr.s3.to_csv(
        pd.DataFrame(rounds), path=f"s3://ufc-big-data-2/{k}-rounds.csv", index=False
    )
    wr.s3.to_csv(
        pd.DataFrame(fight, index=[0]),
        path=f"s3://ufc-big-data-2/{k}-fight.csv",
        index=False,
    )
    print("fight successfully written!")


# transform fix
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

    # two different storage formats, for the memes.
    boto3.setup_default_session(
        region_name="us-east-1",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY_ID,
    )

    # print(pd.DataFrame(rounds))
    # print(pd.DataFrame(fight, index=[0]))

    # sys.exit()

    return rounds, fight, k

    # wr.s3.to_csv(
    #     pd.DataFrame(rounds), path=f"s3://ufc-big-data-2/{k}-rounds.csv", index=False
    # )
    # wr.s3.to_csv(
    #     pd.DataFrame(fight, index=[0]),
    #     path=f"s3://ufc-big-data-2/{k}-fight.csv",
    #     index=False,
    # )
    # print("fight successfully written!")

    # pd.DataFrame(rounds).to_parquet(f"{k}-rounds.parquet.gzip", compression="gzip")
    # pd.DataFrame(fight, index=[0]).to_csv(f"{k}-fight.csv")

    # target table format reference:
    # fightkey, fighterkey, round_key, fight_keynat, [.. stats]
    # fightkeynat,  red fighter key, winner_key, details, final round, final round duration, method, referee, round_format, weight class, fight date, is title fight  wmma, wc

    return 1


# transform
def parse_fight(file, k):
    # 1. Returns nested dicts.
    # 2. Ugly script that turns a fight page into a giant dict with the relevant data

    def clean(s):
        return [x.strip() for x in s.split("of")]

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
        # print(json.dumps(d, sort_keys=True, indent=4))
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

    print("fight parsed.")
    rounds, fight, k = fix_data(d, k)
    return rounds, fight, k


def sanity_check(key: str, file) -> bool:
    try:
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
            logging.error(f"Table dim sanity check failed on {key}")
            raise InvalidHTMLTableDimensions
        else:
            logging.info(f"Table dim Sanity check passed on {key} ")
    except IndexError as e:
        logging.error(f"Table dim Sanity check failed on {key} due to {e}")
        raise InvalidHTMLTableDimensions
    except AttributeError as e:
        logging.error(f"Table dim Sanity check failed on {key} due to {e}")
        raise InvalidHTMLTableDimensions
    return True


def get_keys(STATE=STATE) -> list:

    # get keys in SL1 that fall in specified date range
    valid_sl1_keys = []
    start = STATE["START_DATE"].year
    end = STATE["END_DATE"].year
    years = []  # [2009,2010,2011 ...]

    while start <= end:
        years.append(start)
        start += 1

    for y in years:
        keys = S3C.list_objects_v2(
            Bucket=STATE["STAGE_LAYER_ONE"], Prefix=f"fight-{y}"
        ).get("Contents")
        if keys:
            for k in keys:
                valid_sl1_keys.append(k["Key"])
    return valid_sl1_keys


def prepstate(event, STATE=STATE) -> dict:
    try:
        if (
            not event
            or not event["dates"]
            or not event["dates"]["start"]
            or not event["dates"]["end"]
            or date.fromisoformat(event["dates"]["start"])
            > date.fromisoformat(event["dates"]["end"])
        ):
            raise ValueError
    except TypeError:
        raise ValueError("invalid dates")

    STATE["START_DATE"] = date.fromisoformat(event["dates"]["start"])
    STATE["END_DATE"] = date.fromisoformat(event["dates"]["end"])

    return STATE


# run as script
if __name__ == "__main__":
    main()
