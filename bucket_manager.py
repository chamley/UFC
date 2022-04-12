import os
from dotenv import load_dotenv
import boto3

load_dotenv()
access_key_id = os.getenv("access_key_id")
secret_access_key_id = os.getenv("secret_access_key_id")


def __main__():
    print("starting bucket manager script ..\n#\n#\n#\n#\n#\n#\n#")
    bucket_manager()


# we grab the latest raw data. We transform in another stage
def bucket_manager():
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key_id,
    )

    my_buckets = s3.list_buckets()["Buckets"]
    for e in my_buckets[:4]:
        object_dic = s3.list_objects(Bucket=e["Name"])
        for key, val in object_dic.items():

            print("key: ", key, "   val: ", val)


__main__()
