import pickle
from connector import connector


conf = dict(
    service_name = "s3",
    aws_access_key_id = "AWS_ACCESS_KEY_ID",
    aws_secret_access_key = "AWS_SECRET_ACCESS_KEY",
    region_name = "REGION_NAME",
    )        

# ---------- S3 Utils ----------

@connector(**conf)
def get_pickle(client, bucket=None, key=None):
    if "Contents" in client.list_objects(Bucket=bucket, Prefix=key):
        obj = client.get_object(Bucket=bucket, Key=key)
        data = pickle.loads(obj["Body"].read())
        return data
    
@connector(**conf)
def put_pickle(client, data, bucket=None, key=None):
    body = pickle.dumps(data)
    response = client.put_object(Body=body, Bucket=bucket, Key=key)
    return response
