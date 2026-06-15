import os
from lib.utils import get_databricks_session
from lib import transformations as tr
from lib import data_loader as dl
from lib import config_loader as cf
from pyspark.sql.functions import to_json,col,struct

if __name__ == '__main__':
    token = os.getenv("DATABRICKS_TOKEN")
    host = os.getenv("DATABRICKS_HOST")
    spark = get_databricks_session(host, token)
    #reading data from accounts table
    accounts_df = dl.get_accounts(spark)
    trans_accounts = tr.get_trans_accounts(accounts_df)

    #reading data from parties table
    parties_df = dl.get_parties(spark)
    trans_parties = tr.get_trans_parties(parties_df)

    #reading data from party_address table
    address_df = dl.get_address(spark)
    trans_address = tr.get_trans_address(address_df)

    #joining parties and party_address table
    party_address_join = tr.get_join_parties(trans_parties, trans_address)

    #joining accounts with party_address_join
    final_join_df = tr.get_join_address(trans_accounts,party_address_join)

    #final df with clean format
    final_df = tr.get_final_df(final_join_df)

    #creating key and value for kafka
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").cast("string").alias("key"),
                                  to_json(struct("*")).alias("value"))
    kafka_kv_df.show(1, truncate=False)
    conf = cf.get_config()
    api_key = conf["kafka.api_key"]
    api_secret = conf["kafka.api_secret"]

    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .save()