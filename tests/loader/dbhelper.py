import psycopg2
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_batch


load_dotenv()

DB_HOST = (
    os.getenv("db_host")
    if os.getenv("db_host")
    else "ufc-main.c54pcx5oxf8d.us-east-1.redshift.amazonaws.com"
)

DB_USERNAME = os.getenv("db_username")
DB_PASSWORD = os.getenv("db_password")

## PARALLELISM:
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
