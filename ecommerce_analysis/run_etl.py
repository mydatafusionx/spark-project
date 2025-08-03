#!/usr/bin/env python3
"""
Script para executar o pipeline ETL do projeto de análise de e-commerce.
"""
import os
import sys
from pyspark.sql import SparkSession
from src.etl import ETLProcessor
from src.utils import create_spark_session

def main():
    """Função principal para executar o pipeline ETL."""
    try:
        # Configurações
        app_name = "EcommerceETL"
        input_dir = os.path.join(os.path.dirname(__file__), "data/raw")
        output_dir = os.path.join(os.path.dirname(__file__), "data/processed")
        
        print(f"Iniciando a sessão Spark: {app_name}")
        print(f"Diretório de entrada: {input_dir}")
        print(f"Diretório de saída: {output_dir}")
        
        # Criar sessão Spark
        spark = create_spark_session(app_name)
        
        # Inicializar e executar o ETL
        print("Iniciando o processamento ETL...")
        etl = ETLProcessor(spark=spark, input_dir=input_dir, output_dir=output_dir)
        etl.run()
        
        print("Processamento ETL concluído com sucesso!")
        return 0
        
    except Exception as e:
        print(f"Erro durante a execução do ETL: {str(e)}", file=sys.stderr)
        return 1
    finally:
        # Encerrar a sessão Spark ao final
        if 'spark' in locals():
            spark.stop()
            print("Sessão Spark encerrada.")

if __name__ == "__main__":
    sys.exit(main())
