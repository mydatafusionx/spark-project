from pyspark.sql import SparkSession
import sys

def main():
    # Inicializa a sessão Spark
    spark = SparkSession.builder \
        .appName("CheckProcessedData") \
        .getOrCreate()
    
    # Caminho para os dados processados
    base_path = "data/processed/"
    
    # Lista de tabelas dimensionais e de fatos
    tables = ["dim_clientes", "dim_produtos", "dim_tempo", "fato_vendas"]
    
    # Para cada tabela, mostra as primeiras 5 linhas
    for table in tables:
        try:
            print(f"\n=== {table.upper()} ===")
            path = f"{base_path}{table}"
            df = spark.read.parquet(path)
            df.show(5, truncate=False)
            print(f"Total de registros: {df.count()}")
        except Exception as e:
            print(f"Erro ao ler {table}: {str(e)}")
    
    # Encerra a sessão Spark
    spark.stop()

if __name__ == "__main__":
    main()
