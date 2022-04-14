import os
from dotenv import load_dotenv
import boto3
from pprint import pprint

# get all buckets in our s3
# x = s3r.buckets.all()

# getting objects in one fell swoop is not a thing, we can
# however get a list of ObjectSummary types that have the description
# of each object, which can then with the Bucket object be deleted
# x = sebs_bucket.objects.all()

# Each ObjectSummary o will contain two attributes we need:
# o.key and o.bucket_name

load_dotenv()
access_key_id = os.getenv("access_key_id")
secret_access_key_id = os.getenv("secret_access_key_id")


def __main__():
    print("starting bucket manager script ..\n#\n#\n#\n#\n#\n#\n#")
    sns = boto3.client(
        "sns",
        region_name="eu-west-3",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key_id,
    )
    response = sns.publish(PhoneNumber="+33667938716", Message="ohai")
    print(response)


# we grab the latest raw data. We transform in another stage
def bucket_manager():
    s3client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key_id,
    )
    s3resource = boto3.resource(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key_id,
    )

    # for bucket in s3resource.buckets.all():
    #     for o in bucket.objects.all():
    #         print(o)
    #         response = s3resource.Object(bucket.name, o.key).delete()
    #         print(response)


__main__()
