import pytest
import os
from lib import utils as ut
from lib import data_loader as dl
from lib import transformations as tr
from pyspark.sql.types import StructType,ArrayType

@pytest.fixture(scope="session")
def spark():
    token = os.getenv("DATABRICKS_TOKEN")
    host = os.getenv("DATABRICKS_HOST")
    return ut.get_databricks_session(host,token)

def test_accounts(spark):
    acc_cnt = dl.get_accounts(spark).count()
    assert acc_cnt == 9

def test_parties(spark):
    par_df = dl.get_parties(spark)
    par_cnt = par_df.count()
    par_tcnt = tr.get_trans_parties(par_df).count()
    assert par_cnt == par_tcnt

def test_party_address(spark):
    adds_df = dl.get_address(spark)
    tadds_cnt  = tr.get_trans_address(adds_df).count()
    assert tadds_cnt == 9

def test_final_df(spark):
    accounts_df = dl.get_accounts(spark)
    trans_accounts = tr.get_trans_accounts(accounts_df)
    acc_cnt = trans_accounts.count()
    parties_df = dl.get_parties(spark)
    trans_parties = tr.get_trans_parties(parties_df)
    address_df = dl.get_address(spark)
    trans_address = tr.get_trans_address(address_df)
    party_address_join = tr.get_join_parties(trans_parties, trans_address)
    final_join_df = tr.get_join_address(trans_accounts, party_address_join)
    final_df = tr.get_final_df(final_join_df)
    fin_df_count = final_df.count()

    assert acc_cnt == fin_df_count
    #assert "eventHeader" in final_df.columns
    #assert "keys" in final_df.columns
    #assert "payload" in final_df.columns
    assert set(final_df.columns) == {"eventHeader", "keys", "payload"}
    row = final_df.first()
    assert row["eventHeader"] is not None
    assert row["keys"] is not None
    assert row["payload"] is not None
    assert isinstance(final_df.schema["eventHeader"].dataType, StructType)
    assert isinstance(final_df.schema["keys"].dataType, ArrayType)
    assert isinstance(final_df.schema["payload"].dataType, StructType)
