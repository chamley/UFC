from dbhelper import DBHelper
import logging
from datetime import datetime

DATE = datetime.date.today()
LOGFILE_NAME = f"db_build {DATE}.log"
logging.basicConfig(filename=LOGFILE_NAME, encoding="utf-8", level=logging.DEBUG)


def main() -> None:
    """The purpose of this script is to fully fledge out version 1.0 of our database"""
    logging.info("-- Building Database --")
    db = DBHelper()
    # create_Date_dim(db)
    # create_Fighter_dim(db)
    # create_Round_dim(db)
    # create_Round_fact(db)
    # set_foreign_keys(db)
    db.closeDB()
    logging.info("-- Building Database Finished --")


def set_foreign_keys(db: DBHelper) -> None:
    logging.info("setting foreign keys ...")
    query = """"""
    logging.info("foreign keys set successfully.")


def create_Round_fact(db: DBHelper) -> None:
    logging.info("building Round_fact ...")
    query = """
        create table Round_fact (
            fight_date int
            ,figher_key int
            ,round_key int
            ,fight_key int
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
            ,primary key(fight_date, figher_key, round_key, fight_key)
        )
        """
    db.getCursor().execute(query)
    db.getConn().commit()
    logging.info("Round_fact successfully built.")


def create_Fight_dim(db: DBHelper) -> None:
    logging.info("building Fight_dim ...")
    query = """
            create table Fight_dim (
                fight_key int primary key
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
            fighter_key int primary key
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


main()
