import pytest
from pyspark.sql import SparkSession
from config.config import Config
from business.logic import BusinessLogic

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_process(spark):
    config = Config()
    logic = BusinessLogic(spark, config)

    pedidos_data = [("1", "SP", "cartao", 100.0, "2025-05-01")]
    pagamentos_data = [("1", False, False)]

    pedidos_df = spark.createDataFrame(pedidos_data, schema=config.pedidos_schema)
    pagamentos_df = spark.createDataFrame(pagamentos_data, schema=config.pagamentos_schema)

    result_df = logic.process(pedidos_df, pagamentos_df)
    assert result_df.count() == 1
