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
        pass

    def query1(self, params):
        # return self.cur.execute("some sql stuff").fetchall()
        pass

    def query2(self, params):
        pass

    pass
