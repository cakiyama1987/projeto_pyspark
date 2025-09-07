import logging

class Pipeline:
    def __init__(self, data_io, logic, config):
        self.data_io = data_io
        self.logic = logic
        self.config = config

    def run(self):
        logging.info("Pipeline iniciado.")

        pedidos_df = self.data_io.read_pedidos()
        pagamentos_df = self.data_io.read_pagamentos()

        result_df = self.logic.process(pedidos_df, pagamentos_df)
        self.data_io.write_parquet(result_df)

        logging.info("Pipeline finalizado.")
