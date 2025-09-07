# business/logic.py
import logging
from pyspark.sql.functions import col, year

class BusinessLogic:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def process(self, pedidos_df, pagamentos_df):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

        # normaliza nomes p/ minúsculo
        pedidos = pedidos_df.toDF(*[c.lower() for c in pedidos_df.columns])
        pagos   = pagamentos_df.toDF(*[c.lower() for c in pagamentos_df.columns])

        # pedidos reais: id_pedido, produto, valor_unitario, quantidade, data_criacao, uf, id_cliente
        # cria valor_total_pedido e renomeia campos para o esperado pela saída
        pedidos = (
            pedidos
            .withColumn("valor_total_pedido", col("valor_unitario") * col("quantidade"))
            .withColumnRenamed("data_criacao", "data_pedido")
            .withColumnRenamed("uf", "estado")
        )

        # pagamentos reais: id_pedido, status, forma_pagamento, valor_pagamento, avaliacao_fraude.fraude, ...
        pagos = pagos.withColumn("fraude", col("avaliacao_fraude.fraude"))

        # join
        df = pedidos.join(pagos, "id_pedido", "inner")

        # filtros do trabalho
        df = df.filter(
            (year(col("data_pedido")) == 2025) &
            (col("status") == False) &
            (col("fraude") == False)
        )

        # seleção e ordenação finais
        return df.select(
            "id_pedido", "estado", "forma_pagamento", "valor_total_pedido", "data_pedido"
        ).orderBy("estado", "forma_pagamento", "data_pedido")
