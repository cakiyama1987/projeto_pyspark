from pathlib import Path

class Config:
    def __init__(self):
        base = Path(__file__).resolve().parents[1]  # /workspaces/projeto_pyspark
        self.pedidos_glob    = str(base / "data" / "pedidos" / "pedidos-*.csv.gz")
        self.pagamentos_glob = str(base / "data" / "pagamentos" / "pagamentos-*.json.gz")
        self.output_path = str(base / "output" / "relatorio_parquet")
