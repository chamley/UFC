from sqlite3 import connect
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("db_host")
DB_USERNAME = os.getenv("db_username")
DB_PASSWORD = os.getenv("db_password")


class DBHelper:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="dev",
            user=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port="5439",
        )
        self.cur = self.conn.cursor()

    def getCursor(self):
        return self.cur

    def getConn(self):
        return self.conn

    def closeDB(self):
        self.conn.close()

    def insert_into_dirty_round(self, round):
        query = """ insert into dirty_round_table
            values ( %(color)s,
                %(fighter_name_nat)s,
                %(fight_key_nat)s,
                %(round)s,
                %(result)s,
                %(kd)s,
                %(ss_l)s,
                %(ss_a)s,
                %(ts_l)s,
                %(ts_a)s,
                %(td_l)s,
                %(td_a)s,
                %(sub_a)s,
                %(rev)s,
                %(ctrl)s,
                %(ss_l_h)s,
                %(ss_a_h)s,
                %(ss_l_b)s,
                %(ss_a_b)s,
                %(ss_l_l)s,
                %(ss_a_l)s,
                %(ss_l_dist)s,
                %(ss_a_dist)s,
                %(ss_l_cl)s,
                %(ss_a_cl)s,
                %(ss_l_gr)s,
                %(ss_a_gr)s
        )
        """
        self.cur.execute(query, round)
        self.conn.commit()

    def insert_into_dirty_fight(self, fight_meta):
        query = """insert into dirty_fight_table values (
            %(fight_key_nat)s
            ,%(details)s
            ,%(final_round)s
            ,%(final_round_duration)s
            ,%(method)s
            ,%(referee)s
            ,%(round_format)s
            ,%(weight class)s)
            """
        self.cur.execute(query, fight_meta)
        self.conn.commit()
