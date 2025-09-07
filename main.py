from config.config import Config
from session.spark_session import SparkSessionManager
from dataio.data_io import DataIO
from business.logic import BusinessLogic
from orchestration.pipeline import Pipeline

def main():
    # Configuração
    config = Config()
    
    # Sessão Spark
    spark_manager = SparkSessionManager(config)
    spark = spark_manager.get_session()
    
    # I/O
    data_io = DataIO(spark, config)
    
    # Lógica de negócios
    logic = BusinessLogic(spark, config)
    
    # Orquestração
    pipeline = Pipeline(data_io, logic, config)
    pipeline.run()

if __name__ == "__main__":
    main()
