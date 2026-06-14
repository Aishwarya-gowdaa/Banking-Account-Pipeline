import os
from lib.utils import get_databricks_session

if __name__ == '__main__':
    token = os.getenv("DATABRICKS_TOKEN")
    host = os.getenv("DATABRICKS_HOST")
    spark = get_databricks_session(host,token)

    df = spark.read.table("dev.spark_db.diamonds")
    df.show(5)