from pyspark.sql import SparkSession
import os

class SparkSessionManager:
    def __init__(self, config):
        self.config = config

    def get_session(self):
        # Diagnóstico explícito
        print("JAVA_HOME:", os.environ.get("JAVA_HOME"))
        print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))
        print("TEMP:", os.environ.get("TEMP"))
        print("TMP:", os.environ.get("TMP"))

        builder = (
            SparkSession.builder
            .appName("Trabalho Final PySpark")
            # Garante diretório local válido no Windows
            .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIRS", "C:/spark_tmp"))
        )

        # Se HADOOP_HOME estiver setado e inválido, remova do processo
        if os.environ.get("HADOOP_HOME") and not os.path.exists(os.environ["HADOOP_HOME"]):
            os.environ.pop("HADOOP_HOME", None)

        spark = builder.getOrCreate()
        return spark
