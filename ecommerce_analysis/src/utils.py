from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import random
from datetime import datetime, timedelta

# Configurações de logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="EcommerceAnalysis"):
    """
    Cria e retorna uma sessão Spark configurada
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        logger.info("Spark session criada com sucesso!")
        return spark
    except Exception as e:
        logger.error(f"Erro ao criar sessão Spark: {str(e)}")
        raise

def get_schema(data_type):
    """
    Retorna o schema apropriado para cada tipo de dado
    """
    schemas = {
        "clientes": StructType([
            StructField("cliente_id", IntegerType(), False),
            StructField("nome", StringType(), False),
            StructField("email", StringType(), False),
            StructField("cidade", StringType()),
            StructField("estado", StringType()),
            StructField("data_cadastro", DateType())
        ]),
        "produtos": StructType([
            StructField("produto_id", IntegerType(), False),
            StructField("nome", StringType(), False),
            StructField("categoria", StringType()),
            StructField("preco", DoubleType(), False),
            StructField("estoque", IntegerType())
        ]),
        "pedidos": StructType([
            StructField("pedido_id", IntegerType(), False),
            StructField("cliente_id", IntegerType(), False),
            StructField("data_pedido", TimestampType(), False),
            StructField("valor_total", DoubleType()),
            StructField("status", StringType())
        ]),
        "itens_pedido": StructType([
            StructField("item_id", IntegerType(), False),
            StructField("pedido_id", IntegerType(), False),
            StructField("produto_id", IntegerType(), False),
            StructField("quantidade", IntegerType()),
            StructField("preco_unitario", DoubleType())
        ])
    }
    
    return schemas.get(data_type.lower(), None)

def generate_sample_data(spark, output_dir="../data/raw", n_clients=100, n_products=50, n_orders=1000):
    """
    Gera dados de exemplo para o projeto
    """
    try:
        # Criar diretório se não existir
        os.makedirs(output_dir, exist_ok=True)
        
        # Gerar dados de clientes
        clientes_data = []
        estados = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE"]
        cidades = {
            "SP": ["São Paulo", "Campinas", "São José dos Campos"],
            "RJ": ["Rio de Janeiro", "Niterói", "São Gonçalo"],
            "MG": ["Belo Horizonte", "Uberlândia", "Contagem"],
            "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas"],
            "PR": ["Curitiba", "Londrina", "Maringá"],
            "SC": ["Florianópolis", "Joinville", "Blumenau"],
            "BA": ["Salvador", "Feira de Santana", "Vitória da Conquista"],
            "DF": ["Brasília"],
            "GO": ["Goiânia", "Aparecida de Goiânia", "Anápolis"],
            "PE": ["Recife", "Jaboatão dos Guararapes", "Olinda"]
        }
        
        for i in range(1, n_clients + 1):
            estado = random.choice(estados)
            cidade = random.choice(cidades[estado])
            data_cadastro = datetime.now() - timedelta(days=random.randint(1, 365))
            
            clientes_data.append( (
                i,  # cliente_id
                f"Cliente {i}",  # nome
                f"cliente{i}@email.com",  # email
                cidade,
                estado,
                data_cadastro.date()
            ) )
        
        # Criar DataFrame de clientes
        clientes_df = spark.createDataFrame(clientes_data, schema=get_schema("clientes"))
        
        # Salvar dados
        clientes_df.write.mode("overwrite").parquet(f"{output_dir}/clientes")
        logger.info(f"Dados de clientes gerados e salvos em {output_dir}/clientes")
        
        # Gerar dados de produtos
        produtos_data = []
        categorias = ["Eletrônicos", "Roupas", "Alimentos", "Livros", "Casa", "Esportes"]
        
        for i in range(1, n_products + 1):
            categoria = random.choice(categorias)
            preco = round(random.uniform(10, 2000), 2)
            
            produtos_data.append( (
                i,  # produto_id
                f"Produto {i} - {categoria}",  # nome
                categoria,
                preco,
                random.randint(0, 100)  # estoque
            ) )
        
        # Criar DataFrame de produtos
        produtos_df = spark.createDataFrame(produtos_data, schema=get_schema("produtos"))
        
        # Salvar dados
        produtos_df.write.mode("overwrite").parquet(f"{output_dir}/produtos")
        logger.info(f"Dados de produtos gerados e salvos em {output_dir}/produtos")
        
        # Gerar dados de pedidos
        pedidos_data = []
        status_options = ["Pendente", "Pago", "Enviado", "Entregue", "Cancelado"]
        
        for i in range(1, n_orders + 1):
            cliente_id = random.randint(1, n_clients)
            data_pedido = datetime.now() - timedelta(days=random.randint(1, 30))
            status = random.choices(
                status_options, 
                weights=[0.1, 0.3, 0.2, 0.35, 0.05],  # pesos para cada status
                k=1
            )[0]
            
            pedidos_data.append( (
                i,  # pedido_id
                cliente_id,
                data_pedido,
                None,  # valor_total será calculado posteriormente
                status
            ) )
        
        # Criar DataFrame de pedidos
        pedidos_df = spark.createDataFrame(pedidos_data, schema=get_schema("pedidos"))
        
        # Gerar itens de pedido e calcular valor total
        itens_pedido_data = []
        
        for pedido in pedidos_data:
            pedido_id = pedido[0]
            n_itens = random.randint(1, 5)
            produtos_pedido = random.sample(range(1, n_products + 1), n_itens)
            
            valor_total_pedido = 0
            
            for produto_id in produtos_pedido:
                produto = produtos_df.filter(produtos_df.produto_id == produto_id).first()
                if produto:
                    quantidade = random.randint(1, 3)
                    preco_unitario = produto.preco
                    valor_total_item = quantidade * preco_unitario
                    valor_total_pedido += valor_total_item
                    
                    itens_pedido_data.append( (
                        len(itens_pedido_data) + 1,  # item_id
                        pedido_id,
                        produto_id,
                        quantidade,
                        float(preco_unitario)
                    ) )
            
            # Atualizar valor total do pedido
            pedidos_df = pedidos_df.withColumn(
                "valor_total",
                when(col("pedido_id") == pedido_id, valor_total_pedido)
                .otherwise(col("valor_total"))
            )
        
        # Criar DataFrame de itens de pedido
        itens_pedido_df = spark.createDataFrame(
            itens_pedido_data, 
            schema=get_schema("itens_pedido")
        )
        
        # Salvar dados
        pedidos_df.write.mode("overwrite").parquet(f"{output_dir}/pedidos")
        itens_pedido_df.write.mode("overwrite").parquet(f"{output_dir}/itens_pedido")
        
        logger.info(f"Dados de pedidos gerados e salvos em {output_dir}/pedidos")
        logger.info(f"Dados de itens de pedido gerados e salvos em {output_dir}/itens_pedido")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao gerar dados de exemplo: {str(e)}")
        raise

def load_data(spark, data_type, path):
    """
    Carrega dados de um arquivo CSV
    """
    try:
        schema = get_schema(data_type)
        if not schema:
            raise ValueError(f"Tipo de dados inválido: {data_type}")
            
        # Verifica se o caminho é um diretório ou arquivo
        if os.path.isdir(path):
            # Se for diretório, procura por arquivos CSV dentro dele
            csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]
            if not csv_files:
                raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {path}")
            path = os.path.join(path, csv_files[0])
            
        # Lê o arquivo CSV com o schema definido
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(path)
            
        logger.info(f"Dados carregados com sucesso: {path}")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao carregar dados de {path}: {str(e)}")
        raise

if __name__ == "__main__":
    spark = create_spark_session("DataGenerator")
    generate_sample_data(spark)
    spark.stop()