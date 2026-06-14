from databricks.connect import DatabricksSession

def get_databricks_session(host,token):
    return DatabricksSession.builder.host(host).token(token).serverless().getOrCreate()
