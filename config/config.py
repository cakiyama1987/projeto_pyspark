class Config:
    def __init__(self):
        # Caminhos dos datasets
        self.pedidos_path = "data/pedidos/*.csv"
        self.pagamentos_path = "data/pagamentos/*.json"
        self.output_path = "output/relatorio_parquet"

        # Schemas explícitos
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, DateType

        self.pedidos_schema = StructType([
            StructField("id_pedido", StringType(), False),
            StructField("estado", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("valor_total_pedido", DoubleType(), True),
            StructField("data_pedido", DateType(), True)
        ])

        self.pagamentos_schema = StructType([
            StructField("id_pedido", StringType(), False),
            StructField("status", BooleanType(), True),
            StructField("fraude", BooleanType(), True)
        ])
