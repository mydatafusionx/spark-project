import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lit, sum as _sum, count as _count, max as _max, min as _min, avg as _avg, date_format
import logging

from utils import create_spark_session, load_data, get_schema

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLProcessor:
    """
    Classe responsável pelo processamento ETL dos dados de e-commerce
    """
    
    def __init__(self, spark, input_dir="../data/raw", output_dir="../data/processed"):
        """
        Inicializa o processador ETL
        
        Args:
            spark: Sessão Spark
            input_dir: Diretório de entrada dos dados brutos
            output_dir: Diretório de saída dos dados processados
        """
        self.spark = spark
        self.input_dir = input_dir
        self.output_dir = output_dir
        
        # Criar diretórios se não existirem
        os.makedirs(self.output_dir, exist_ok=True)
    
    def extract(self):
        """
        Extrai os dados das fontes de origem
        """
        logger.info("Iniciando extração dos dados...")
        
        try:
            # Carregar dados brutos
            self.clientes_df = load_data(self.spark, "clientes", f"{self.input_dir}/clientes")
            self.produtos_df = load_data(self.spark, "produtos", f"{self.input_dir}/produtos")
            self.pedidos_df = load_data(self.spark, "pedidos", f"{self.input_dir}/pedidos")
            self.itens_pedido_df = load_data(self.spark, "itens_pedido", f"{self.input_dir}/itens_pedido")
            
            logger.info("Dados extraídos com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"Erro na extração dos dados: {str(e)}")
            raise
    
    def transform(self):
        """
        Transforma os dados conforme as regras de negócio
        """
        logger.info("Iniciando transformação dos dados...")
        
        try:
            # 1. Preparar dados de clientes
            # Usando data_cadastro para criar uma faixa de tempo como cliente
            self.clientes_df = self.clientes_df.withColumn("meses_cadastro", 
                F.datediff(F.current_date(), F.to_date("data_cadastro")) / 30
            )
            
            self.clientes_df = self.clientes_df.withColumn(
                "faixa_etaria",
                F.when(F.col("meses_cadastro").isNull(), "Não informado")
                .when(F.col("meses_cadastro") <= 6, "0-6 meses")
                .when((F.col("meses_cadastro") > 6) & (F.col("meses_cadastro") <= 12), "7-12 meses")
                .when((F.col("meses_cadastro") > 12) & (F.col("meses_cadastro") <= 24), "1-2 anos")
                .otherwise("Mais de 2 anos")
            )
            
            # Remover a coluna temporária
            self.clientes_df = self.clientes_df.drop("meses_cadastro")
            
            # 2. Preparar dados de produtos
            self.produtos_df = self.produtos_df.withColumn(
                "faixa_preco",
                when(col("preco") < 50, "Até R$50")
                .when((col("preco") >= 50) & (col("preco") < 100), "R$50 a R$100")
                .when((col("preco") >= 100) & (col("preco") < 200), "R$100 a R$200")
                .when((col("preco") >= 200) & (col("preco") < 500), "R$200 a R$500")
                .otherwise("Acima de R$500")
            )
            
            # 3. Preparar dados de pedidos
            # Converter data_pedido para date para facilitar as análises
            self.pedidos_df = self.pedidos_df.withColumn(
                "data_pedido_date", 
                F.to_date("data_pedido")
            )
            
            # Adicionar coluna de mês/ano do pedido
            self.pedidos_df = self.pedidos_df.withColumn(
                "mes_ano_pedido",
                date_format("data_pedido", "yyyy-MM")
            )
            
            # 4. Criar fatos e dimensões
            self._create_fato_vendas()
            self._create_dimensao_clientes()
            self._create_dimensao_produtos()
            self._create_dimensao_tempo()
            
            logger.info("Transformação dos dados concluída com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"Erro na transformação dos dados: {str(e)}")
            raise
    
    def _create_fato_vendas(self):
        """
        Cria a tabela de fatos de vendas
        """
        # Juntar pedidos com itens de pedido
        self.fato_vendas = self.pedidos_df.join(
            self.itens_pedido_df,
            "pedido_id",
            "inner"
        )
        
        # Adicionar chaves estrangeiras
        self.fato_vendas = self.fato_vendas.join(
            self.clientes_df.select("cliente_id", "cidade", "estado"),
            "cliente_id",
            "left"
        )
        
        self.fato_vendas = self.fato_vendas.join(
            self.produtos_df.select("produto_id", "categoria", "faixa_preco"),
            "produto_id",
            "left"
        )
        
        # Calcular métricas adicionais
        self.fato_vendas = self.fato_vendas.withColumn(
            "valor_total_item",
            col("quantidade") * col("preco_unitario")
        )
    
    def _create_dimensao_clientes(self):
        """
        Cria a dimensão de clientes
        """
        self.dim_clientes = self.clientes_df.select(
            "cliente_id",
            "nome",
            "email",
            "cidade",
            "estado",
            "faixa_etaria",
            "data_cadastro"
        )
    
    def _create_dimensao_produtos(self):
        """
        Cria a dimensão de produtos
        """
        self.dim_produtos = self.produtos_df.select(
            "produto_id",
            "nome",
            "categoria",
            "preco",
            "faixa_preco",
            "estoque"
        )
    
    def _create_dimensao_tempo(self):
        """
        Cria a dimensão de tempo a partir dos pedidos
        """
        # Extrair datas distintas dos pedidos
        datas_distintas = self.pedidos_df.select(
            "data_pedido_date"
        ).distinct()
        
        # Criar dimensão de tempo
        self.dim_tempo = datas_distintas.select(
            col("data_pedido_date").alias("data"),
            F.dayofmonth("data_pedido_date").alias("dia"),
            F.dayofweek("data_pedido_date").alias("dia_semana"),
            F.dayofyear("data_pedido_date").alias("dia_ano"),
            F.weekofyear("data_pedido_date").alias("semana_ano"),
            F.month("data_pedido_date").alias("mes"),
            F.quarter("data_pedido_date").alias("trimestre"),
            F.year("data_pedido_date").alias("ano"),
            (F.quarter("data_pedido_date") <= 2).cast("int").alias("primeiro_semestre"),
            (F.quarter("data_pedido_date") > 2).cast("int").alias("segundo_semestre"),
            F.last_day("data_pedido_date").alias("ultimo_dia_mes")
        )
    
    def load(self):
        """
        Carrega os dados processados para o destino final
        """
        logger.info("Iniciando carregamento dos dados processados...")
        
        try:
            # Salvar dados processados
            self.fato_vendas.write.mode("overwrite").parquet(f"{self.output_dir}/fato_vendas")
            self.dim_clientes.write.mode("overwrite").parquet(f"{self.output_dir}/dim_clientes")
            self.dim_produtos.write.mode("overwrite").parquet(f"{self.output_dir}/dim_produtos")
            self.dim_tempo.write.mode("overwrite").parquet(f"{self.output_dir}/dim_tempo")
            
            logger.info(f"Dados processados salvos em {self.output_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados processados: {str(e)}")
            raise
    
    def run(self):
        """
        Executa o pipeline ETL completo
        """
        try:
            self.extract()
            self.transform()
            self.load()
            logger.info("Pipeline ETL executado com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao executar pipeline ETL: {str(e)}")
            raise

def main():
    """
    Função principal para execução do ETL
    """
    try:
        # Criar sessão Spark
        spark = create_spark_session("ETLEcommerce")
        
        # Executar pipeline ETL
        etl = ETLProcessor(spark)
        etl.run()
        
        # Encerrar sessão Spark
        spark.stop()
        
    except Exception as e:
        logger.error(f"Erro na execução do ETL: {str(e)}")
        raise

if __name__ == "__main__":
    main()