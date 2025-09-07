import logging
from pyspark.sql.functions import year

class BusinessLogic:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def process(self, pedidos_df, pagamentos_df):
        try:
            logging.info("Iniciando processamento...")

            # Join
            df = pedidos_df.join(pagamentos_df, "id_pedido")

            # Filtros
            df = df.filter(
                (year(df.data_pedido) == 2025) &
                (df.status == False) &
                (df.fraude == False)
            )

            # Seleção e ordenação
            df = df.select("id_pedido", "estado", "forma_pagamento", "valor_total_pedido", "data_pedido")                    .orderBy("estado", "forma_pagamento", "data_pedido")

            logging.info("Processamento concluído.")
            return df

        except Exception as e:
            logging.error(f"Erro no processamento: {e}")
            raise e
