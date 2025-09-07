# tests/test_logic.py
import pytest
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, BooleanType
)
from business.logic import BusinessLogic
from config.config import Config

# ---- FIXTURE SPARK (no próprio arquivo) ----
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_process(spark):
    config = Config()
    logic = BusinessLogic(spark, config)

    # Pedidos: calcula valor_total_pedido, data_criacao -> data_pedido, uf -> estado
    pedidos_schema = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("valor_unitario", DoubleType(), True),
        StructField("quantidade", IntegerType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("uf", StringType(), True),
    ])
    pedidos_data = [("1", 50.0, 2, datetime(2025, 5, 1), "SP")]

    pedidos_df = spark.createDataFrame(pedidos_data, schema=pedidos_schema)

    # Pagamentos: forma_pagamento + fraude dentro de avaliacao_fraude
    fraude_schema = StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score", DoubleType(), True),
    ])
    pagamentos_schema = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("status", BooleanType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("avaliacao_fraude", fraude_schema, True),
    ])
    pagamentos_data = [("1", False, "cartao", Row(fraude=False, score=0.12))]

    pagamentos_df = spark.createDataFrame(pagamentos_data, schema=pagamentos_schema)

    # Executa lógica
    out_rows = logic.process(pedidos_df, pagamentos_df).collect()
    assert len(out_rows) == 1
    row = out_rows[0].asDict()

    # Asserts
    assert row["id_pedido"] == "1"
    assert row["estado"] == "SP"
    assert row["forma_pagamento"] == "cartao"
    assert abs(row["valor_total_pedido"] - 100.0) < 1e-9  # 50 * 2
    assert row["data_pedido"].year == 2025
