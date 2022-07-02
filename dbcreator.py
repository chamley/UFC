from dbhelper import DBHelper
import logging
import datetime
import argparse
import os

DATE = datetime.datetime.now()
LOGFILE_NAME = f"logs/db_build {DATE}.log"
logging.basicConfig(filename=LOGFILE_NAME, encoding="utf-8", level=logging.DEBUG)


def main() -> None:
    """The purpose of this script is to fully fledge out version 1.0 of our database"""
    logging.info("-- Building Database --")
    db = DBHelper()
    try:
        # create_Date_dim(db)
        # create_Fighter_dim(db)
        # create_Round_dim(db)
        # create_Round_fact(db)
        # create_dirty_round_table(db)
        # create_dirty_fight_table(db)
        build_date_dim(db)
    finally:
        db.closeDB()
    logging.info("-- Building Database Finished --")


def build_date_dim(db: DBHelper) -> None:
    """the purpose of this function is to populate the date_dim. we tried doing this in sql but
    redshift gave us issues with the generate_series command"""

    if "date_table.csv" not in os.listdir(os.getcwd()):
        with open("date_table.csv", "w") as f:
            d = pd.DataFrame()
            d["timestamp"] = pd.date_range(
                start="1950-01-01", end="2030-01-01", freq="D"
            )
            d["timestamp"] = d["timestamp"].apply(lambda x: x.date())
            d["year"] = d["timestamp"].apply(lambda x: x.year)
            d["month"] = d["timestamp"].apply(lambda x: x.month)
            d["day"] = d["timestamp"].apply(lambda x: x.day)
            d["date_key"] = d["timestamp"].apply(lambda x: str(x).replace("-", ""))
            d.to_csv("date_table.csv")
    with open("date_table.csv", "r") as f:
        db.getCursor().copy_from(f, "date_dim", sep=",")


def set_foreign_keys(db: DBHelper) -> None:
    logging.info("setting foreign keys ...")
    query = """"""
    logging.info("foreign keys set successfully.")


def create_dirty_round_table(db: DBHelper) -> None:
    query = """
        drop table if exists dirty_round_table;
        create table dirty_round_table (
            color int,
            fighter_name_nat text,
            fight_key_nat text,
            round text,
            result text,
            "kd" int,
            "ss_l" int,
            "ss_a" int,
            "ts_l" int,
            "ts_a" int,
            "td_l" int,
            "td_a" int,
            "sub_a" int,
            "rev" int,
            "ctrl" text,
            "ss_l_h" int,
            "ss_a_h" int,
            "ss_l_b" int,
            "ss_a_b" int,
            "ss_l_l" int,
            "ss_a_l" int,
            "ss_l_dist" int,
            "ss_a_dist" int,
            "ss_l_cl" int,
            "ss_a_cl" int,
            "ss_l_gr" int,
            "ss_a_gr" int      
            )
    """
    db.getCursor().execute(query)
    db.getConn().commit()


def create_dirty_fight_table(db: DBHelper) -> None:
    query = """
        drop table if exists dirty_fight_table;
        create table dirty_fight_table (
        fight_key_nat text
        ,details text
        ,final_round text
        ,final_round_duration text
        ,method text
        ,referee text
        ,round_format text
        ,weight_class text
    )
    """
    db.getCursor().execute(query)
    db.getConn().commit()


def create_Round_fact(db: DBHelper) -> None:
    logging.info("building Round_fact ...")
    query = """
        drop table if exists Round_fact;
        create table Round_fact (
            ,fighter_key int
            ,round_key int
            ,knockdowns int
            ,ss_a int
            ,ss_l int
            ,ts_a int
            ,ts_l int
            ,td_a int
            ,td_l int
            ,sub_a int
            ,rev_l int
            ,ground_control_time time
            ,ss_a_head int
            ,ss_l_head int
            ,ss_a_body int
            ,ss_l_body int
            ,ss_a_leg int
            ,ss_l_leg int
            ,ss_a_dist int
            ,ss_l_dist int
            ,ss_a_clinch int
            ,ss_l_clinch int
            ,ss_a_ground int
            ,ss_l_ground int
            ,primary key(fighter_key, round_key)
        )
        """
    db.getCursor().execute(query)
    db.getConn().commit()
    logging.info("Round_fact successfully built.")


def create_Fight_dim(db: DBHelper) -> None:
    logging.info("building Fight_dim ...")
    query = """
            create table Fight_dim (
                fight_key serial primary key
                ,fight_date_key int not null
                ,red_fighter_key int not null
                ,blue_fighter_key int not null
                ,location varchar(20)
                ,referee varchar(20)
                ,weight_class varchar(20)
                ,winner_key int
                ,method varchar(20)
                ,round int
                ,time time

            )
    """
    db.getCursor().execute(query)
    db.getConn().commit()
    logging.info("Fight_dim successfully built.")


def create_Round_dim(db: DBHelper) -> None:
    query = """
        create table Round_dim (
            round_key int primary key
            ,fight_key int not null
            ,round_number smallint not null
            );"""
    db.getCursor().execute(query)
    db.getConn().commit()
    logging.info("Round_dim successfully built.")


def create_Fighter_dim(db: DBHelper) -> None:
    logging.info("building Fighter_dim ...")
    query = """
        create TABLE Fighter_dim (
            fighter_key serial primary key
            ,date_of_birth_key int
            ,first_name varchar(20) not null
            ,last_name varchar(20) not null
            ,height float
            ,reach float
            ,stance varchar(20)
        );
    """
    db.getCursor().execute(query)
    db.getConn().commit()
    logging.info("Fighter_dim successfully built.")


def create_Date_dim(db: DBHelper) -> None:
    logging.info("building Date_dim ...")
    query = """CREATE TABLE Date_dim (
                    date_key int primary key
                    ,year varchar(2) not null
                    ,month varchar(2) not null
                    ,day varchar(2) not null
                    ,date_timestamp date not null );
            """
    db.getCursor().execute(query)
    db.getConn().commit()
    logging.info("Date_dim successfully built.")


if __name__ == "__main__":
    main()
