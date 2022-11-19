import psycopg2
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_batch
import boto3
from configfile import config_settings
import json

load_dotenv()

# get our creds from the secrets manager:

STATE = {**config_settings}

secrets_manager_client = boto3.client(
    service_name="secretsmanager", region_name=STATE["REGION_NAME"]
)

db_config = json.loads(
    secrets_manager_client.get_secret_value(
        SecretId=STATE["redshift_db_login_SecretId"],
    )["SecretString"]
)

# local or deployed
DB_HOST = os.getenv("db_host") or db_config["host"]
DB_USERNAME = os.getenv("db_username") or db_config["username"]
DB_PASSWORD = os.getenv("db_password") or db_config["password"]
PORT = os.getenv("ufc_port") or db_config["port"]


## DESIGN DECISION - PARALLELISM:
## For THREAD SAFTEY each query must create its own cursor.
## The db connection itself however can be shared.
## HOWEVER, due to transaction blocks being per connection and
## not per cursor, i'd rather we keep things the way they are
## and just generate new DBHelper instances inside threads.
## else we propagate errors of buggy code into errors of non-buggy
## code and creating a log digging scenario.


class DBHelper:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=STATE["main_ufc_db_name"],
            user=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=PORT,
        )
        self.cur = self.conn.cursor()

    def getCursor(self):
        return self.cur

    def getConn(self):
        return self.conn

    def closeDB(self):
        self.conn.close()

    # def insert_into_dirty_round(self, round):
    #     query: str = """ insert into dirty_round_table
    #         values ( %(color)s,
    #             %(fighter_name_nat)s,
    #             %(fight_key_nat)s,
    #             %(round)s,
    #             %(result)s,
    #             %(kd)s,
    #             %(ss_l)s,
    #             %(ss_a)s,
    #             %(ts_l)s,
    #             %(ts_a)s,
    #             %(td_l)s,
    #             %(td_a)s,
    #             %(sub_a)s,
    #             %(rev)s,
    #             %(ctrl)s,
    #             %(ss_l_h)s,
    #             %(ss_a_h)s,
    #             %(ss_l_b)s,
    #             %(ss_a_b)s,
    #             %(ss_l_l)s,
    #             %(ss_a_l)s,
    #             %(ss_l_dist)s,
    #             %(ss_a_dist)s,
    #             %(ss_l_cl)s,
    #             %(ss_a_cl)s,
    #             %(ss_l_gr)s,
    #             %(ss_a_gr)s
    #     )
    #     """
    #     self.cur.execute(query, round)
    #     self.conn.commit()

    # def insert_into_dirty_fight(self, fight_meta):

    #     query: str = """insert into dirty_fight_table values (
    #         %(fight_key_nat)s
    #         ,%(details)s
    #         ,%(final_round)s
    #         ,%(final_round_duration)s
    #         ,%(method)s
    #         ,%(referee)s
    #         ,%(round_format)s
    #         ,%(weight class)s)
    #         """
    #     self.cur.execute(query, fight_meta)

    # def batch_insert_into_dirty_round(self, rounds: list):
    #     query = """ insert into dirty_round_table
    #         values ( %(color)s,
    #             %(fighter_name_nat)s,
    #             %(fight_key_nat)s,
    #             %(round)s,
    #             %(result)s,
    #             %(kd)s,
    #             %(ss_l)s,
    #             %(ss_a)s,
    #             %(ts_l)s,
    #             %(ts_a)s,
    #             %(td_l)s,
    #             %(td_a)s,
    #             %(sub_a)s,
    #             %(rev)s,
    #             %(ctrl)s,
    #             %(ss_l_h)s,
    #             %(ss_a_h)s,
    #             %(ss_l_b)s,
    #             %(ss_a_b)s,
    #             %(ss_l_l)s,
    #             %(ss_a_l)s,
    #             %(ss_l_dist)s,
    #             %(ss_a_dist)s,
    #             %(ss_l_cl)s,
    #             %(ss_a_cl)s,
    #             %(ss_l_gr)s,
    #             %(ss_a_gr)s
    #     )
    #     """
    #     execute_batch(self.cur, query, rounds)
