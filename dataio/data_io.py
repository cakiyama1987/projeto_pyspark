import logging

class DataIO:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def read_pedidos(self):
        return self.spark.read.csv(self.config.pedidos_path, header=True, schema=self.config.pedidos_schema)

    def read_pagamentos(self):
        return self.spark.read.json(self.config.pagamentos_path, schema=self.config.pagamentos_schema)

    def write_parquet(self, df):
        df.write.mode("overwrite").parquet(self.config.output_path)
        logging.info("Relatório gravado em Parquet.")
