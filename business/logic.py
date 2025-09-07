
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year

logger = logging.getLogger("business.logic")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

def _safe_count(df: DataFrame, name: str):
    try:
        n = df.count()
        logger.info("%s.count=%s", name, n)
        return n
    except Exception:
        logger.exception("Falha ao contar %s", name)
        return None

def _log_cols(df: DataFrame, name: str):
    logger.info("%s.columns=%s", name, df.columns)

class BusinessLogic:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def process(self, pedidos_df, pagamentos_df):
        logger.info("BusinessLogic.process: START")
        _log_cols(pedidos_df, "pedidos_df(in)")
        _log_cols(pagamentos_df, "pagamentos_df(in)")
        _safe_count(pedidos_df, "pedidos_df(in)")
        _safe_count(pagamentos_df, "pagamentos_df(in)")

        try:
            # 1) normaliza nomes p/ minúsculo
            pedidos = pedidos_df.toDF(*[c.lower() for c in pedidos_df.columns])
            pagos   = pagamentos_df.toDF(*[c.lower() for c in pagamentos_df.columns])
            _log_cols(pedidos, "pedidos(norm)")
            _log_cols(pagos, "pagamentos(norm)")

            # 2) derivados/renomeios (PEDIDOS)
            missing = [c for c in ["valor_unitario", "quantidade", "data_criacao", "uf"] if c not in pedidos.columns]
            if missing:
                logger.warning("Colunas esperadas ausentes em PEDIDOS: %s", missing)

            pedidos = (
                pedidos
                .withColumn("valor_total_pedido", col("valor_unitario") * col("quantidade"))
                .withColumnRenamed("data_criacao", "data_pedido")
                .withColumnRenamed("uf", "estado")
            )
            _log_cols(pedidos, "pedidos(derivados)")

            # 3) flatten fraude (PAGAMENTOS)
            if "avaliacao_fraude" in pagos.columns:
                pagos = pagos.withColumn("fraude", col("avaliacao_fraude.fraude"))
                logger.info("Extraída coluna 'fraude' de avaliacao_fraude.fraude")
            elif "fraude" in pagos.columns:
                logger.info("Coluna 'fraude' já existe em pagamentos")
            else:
                logger.warning("Nenhuma coluna de fraude encontrada em PAGAMENTOS")
            _log_cols(pagos, "pagamentos(flat)")

            before_join = (_safe_count(pedidos, "pedidos"), _safe_count(pagos, "pagamentos"))
            df = pedidos.join(pagos, "id_pedido", "inner")
            joined = _safe_count(df, "df(join)")
            logger.info("Join concluído (inner) em 'id_pedido'; antes=%s depois=%s", before_join, joined)

            df = df.filter(
                (year(col("data_pedido")) == 2025) &
                (col("status") == False) &
                (col("fraude") == False)
            )
            _safe_count(df, "df(filtrado)")
            logger.info("Filtros aplicados: year(data_pedido)=2025, status=False, fraude=False")

            df = df.select(
                "id_pedido", "estado", "forma_pagamento", "valor_total_pedido", "data_pedido"
            ).orderBy("estado", "forma_pagamento", "data_pedido")
            _log_cols(df, "df(final)")

            logger.info("BusinessLogic.process: END")
            return df

        except Exception:
            logger.exception("Erro no processamento")
            raise
