import boto3


# ---------- Connector ----------

def connector(**conf):
    def wrapper(func):
        def with_connection(*args, **kwargs):
            client = boto3.client(**conf)
            out = func(client, *args, **kwargs)
            client.close()
            return out
        return with_connection
    return wrapper

