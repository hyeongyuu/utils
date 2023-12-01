import pandas as pd
from connector import connector


conf = dict(
    service_name = "dynamodb",
    aws_access_key_id = "AWS_ACCESS_KEY_ID",
    aws_secret_access_key = "AWS_SECRET_ACCESS_KEY",
    region_name = "REGION_NAME",
    )        

# ---------- DynamoDB Utils ----------

@connector(**conf)
def dynamodb_query(client, **kwargs):
    response = client.query(**kwargs)
    items = response["Items"]
    while "LastEvaluatedKey" in response:
        response = client.query(ExclusiveStartKey=response["LastEvaluatedKey"], **kwargs)
        items.extend(response["Items"])
    return items

def dynamodb_keycondition(start, end, **kwargs):
    condition = dict()
    for args in kwargs:
        condition.update({f"{args}":{"ComparisonOperator":f"{kwargs[args][0]}", "AttributeValueList":[{"S":kwargs[args][1]}]}})
    _start, _end = (pd.to_datetime(t).strftime("%Y-%m-%d %H:%M:%S") for t in (start, end))
    time_condition = {"timestamp":{"ComparisonOperator":"BETWEEN", "AttributeValueList":[{"S":_start}, {"S":_end}]}}
    condition.update(time_condition)
    return condition
