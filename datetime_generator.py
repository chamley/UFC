from datetime import datetime
import csv
import pandas as pd


def main():
    with open("date_table.csv", "w") as f:
        d = pd.DataFrame()
        d["timestamp"] = pd.date_range(start="1950-01-01", end="2030-01-01", freq="D")
        d["timestamp"] = d["timestamp"].apply(lambda x: x.date())
        d["year"] = d["timestamp"].apply(lambda x: x.year)
        d["month"] = d["timestamp"].apply(lambda x: x.month)
        d["day"] = d["timestamp"].apply(lambda x: x.day)
        d["date_key"] = d["timestamp"].apply(lambda x: str(x).replace("-", ""))
        d.to_csv("date_table.csv")


main()
