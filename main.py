# main.py
import os
import sys
import logging

from config.config import Config
from session.spark_session import SparkSessionManager
from dataio.data_io import DataIO
from business.logic import BusinessLogic
from orchestration.pipeline import Pipeline

# root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("main")

def main():
    try:
        logger.info("==== Início do pipeline ====")

        # Configuração
        config = Config()
        logger.info("Paths: pedidos=%s | pagamentos=%s | output=%s",
                    getattr(config, "pedidos_path", None),
                    getattr(config, "pagamentos_path", None),
                    getattr(config, "output_path", None))

        # Sessão Spark
        spark_manager = SparkSessionManager(config)
        spark = spark_manager.get_session()
        # reduzir verborragia do Spark
        try:
            spark.sparkContext.setLogLevel("WARN")
        except Exception:
            pass

        logger.info("Spark version=%s | Python=%s | JAVA_HOME=%s",
                    spark.version, sys.version.split()[0], os.environ.get("JAVA_HOME"))

        # I/O e Lógica
        data_io = DataIO(spark, config)
        logic = BusinessLogic(spark, config)

        # Orquestração
        pipeline = Pipeline(data_io, logic, config)

        logger.info("Executando pipeline.run() ...")
        pipeline.run()
        logger.info("Pipeline finalizado com sucesso.")

        logger.info("Saída Parquet: %s", getattr(config, "output_path", None))
        logger.info("==== Fim do pipeline ====")

    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário.")
        sys.exit(130)
    except Exception:
        logger.exception("Falha na execução do main")
        sys.exit(1)

if __name__ == "__main__":
    main()
