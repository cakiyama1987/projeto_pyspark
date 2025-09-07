import glob
import logging

class DataIO:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def _expand_or_fail(self, pattern):
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado para: {pattern}")
        logging.info(f"{pattern} -> {len(files)} arquivos")
        return files

    def read_pedidos(self):
        files = self._expand_or_fail(self.config.pedidos_glob)
        return (
            self.spark.read
            .option("header", True)
            .option("sep", ";")      # CSV usa ';'
            .option("inferSchema", True)
            .csv(files)
        )

    def read_pagamentos(self):
        files = self._expand_or_fail(self.config.pagamentos_glob)
        return (
            self.spark.read
            .option("inferSchema", True)
            .json(files)
        )

    def write_parquet(self, df):
        df.write.mode("overwrite").parquet(self.config.output_path)
        logging.info(f"Gravado em: {self.config.output_path}")
