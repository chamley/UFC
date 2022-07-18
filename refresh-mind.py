import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

from first_transformer import STAGE_LAYER_TWO

# PRESCRIPT DETAILS
load_dotenv()
ACCESS_KEY_ID: str = os.getenv("access_key_id")
SECRET_ACCESS_KEY_ID: str | None = os.getenv("secret_access_key_id")
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
STAGE_LAYER_ONE: str = "ufc-big-data"

STAGE_LAYER_TWO: str = "ufc-big-data-2"


def main():
    objects = S3C.list_objects_v2(
        Bucket=STAGE_LAYER_TWO,
        Prefix=f"fight-{datetime.today().year}",  # hack, if we assume we at most have a year to backfill
    )
    x = objects["Contents"][-1:][0]["Key"][6:16]
    print(datetime.strptime(x, "%Y-%m-%d").date())


if __name__ == "__main__":
    main()
