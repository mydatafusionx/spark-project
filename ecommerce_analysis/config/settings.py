"""
Arquivo de configuração do projeto E-commerce Analysis

Este arquivo contém as configurações e parâmetros utilizados em todo o projeto.
"""
import os
from pathlib import Path

# Diretórios base
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')
MODELS_DIR = os.path.join(BASE_DIR, 'models')

# Configurações do Spark
SPARK_CONFIG = {
    'app_name': 'EcommerceAnalysis',
    'master': 'local[*]',  # Usa todos os núcleos disponíveis
    'config': {
        'spark.driver.memory': '4g',
        'spark.executor.memory': '4g',
        'spark.sql.shuffle.partitions': '4',
        'spark.sql.autoBroadcastJoinThreshold': '10485760',  # 10MB
        'spark.sql.execution.arrow.pyspark.enabled': 'true',
        'spark.sql.execution.arrow.pyspark.fallback.enabled': 'true',
        'spark.driver.extraJavaOptions': '-Duser.timezone=GMT',
        'spark.executor.extraJavaOptions': '-Duser.timezone=GMT',
        'spark.sql.session.timeZone': 'UTC'
    }
}

# Configurações do projeto
PROJECT_CONFIG = {
    'sample_data': {
        'n_clients': 1000,      # Número de clientes de exemplo
        'n_products': 100,      # Número de produtos de exemplo
        'n_orders': 5000,       # Número de pedidos de exemplo
        'start_date': '2023-01-01',  # Data inicial para geração de dados
        'end_date': '2023-12-31'     # Data final para geração de dados
    },
    'data_processing': {
        'date_format': 'yyyy-MM-dd',
        'datetime_format': 'yyyy-MM-dd HH:mm:ss',
        'decimal_precision': 2
    },
    'analysis': {
        'top_n_products': 10,    # Número de produtos principais para análise
        'rfm_segments': 5,       # Número de segmentos para análise RFM
        'min_support': 0.05,     # Suporte mínimo para análise de cesta de compras
        'min_confidence': 0.2    # Confiança mínima para análise de cesta de compras
    }
}

# Configurações de logging
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.FileHandler',
            'formatter': 'standard',
            'level': 'DEBUG',
            'filename': os.path.join(BASE_DIR, 'logs', 'ecommerce_analysis.log'),
            'mode': 'a',
            'encoding': 'utf-8'
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': True
        },
        'pyspark': {
            'handlers': ['console'],
            'level': 'WARN',  # Reduzir verbosidade do PySpark
            'propagate': False
        },
        'py4j': {
            'handlers': ['console'],
            'level': 'WARN',  # Reduzir verbosidade do Py4J
            'propagate': False
        }
    }
}

# Configurações de visualização
PLOT_CONFIG = {
    'style': 'seaborn',
    'palette': 'viridis',
    'figure.figsize': (12, 6),
    'font.size': 12,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.format': 'png'
}

# Configurações de exportação
EXPORT_CONFIG = {
    'csv': {
        'sep': ';',
        'decimal': ',',
        'encoding': 'utf-8',
        'index': False,
        'date_format': '%Y-%m-%d',
        'datetime_format': '%Y-%m-%d %H:%M:%S'
    },
    'excel': {
        'sheet_name': 'Dados',
        'encoding': 'utf-8',
        'index': False,
        'date_format': 'yyyy-mm-dd',
        'datetime_format': 'yyyy-mm-dd hh:mm:ss'
    },
    'parquet': {
        'compression': 'snappy',
        'coalesce': 1
    }
}

# Categorias de produtos
PRODUCT_CATEGORIES = [
    'Eletrônicos', 'Informática', 'Celulares', 'TV e Vídeo', 'Eletrodomésticos',
    'Móveis', 'Cama, Mesa e Banho', 'Decoração', 'Cozinha', 'Bebê',
    'Brinquedos', 'Games', 'Livros', 'Papelaria', 'Instrumentos Musicais',
    'Esporte', 'Automotivo', 'Ferramentas', 'Construção', 'Casa e Jardim',
    'Pet Shop', 'Beleza', 'Saúde', 'Bebidas', 'Alimentos', 'Biscoitos',
    'Chocolates', 'Sorvetes', 'Carnes', 'Peixes', 'Aves', 'Frios', 'Laticínios',
    'Hortifruti', 'Padaria', 'Congelados', 'Massas', 'Temperos', 'Molhos',
    'Enlatados', 'Grãos', 'Cereais', 'Farinhas', 'Açúcar e Adoçantes',
    'Óleos e Azeites', 'Achocolatados', 'Cafés', 'Chás', 'Sucos', 'Refrigerantes',
    'Cervejas', 'Vinhos', 'Destilados', 'Águas', 'Outros'
]

# Estados brasileiros
BRAZILIAN_STATES = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
    'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
    'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
]

def ensure_directories():
    """Garante que todos os diretórios necessários existam."""
    directories = [
        DATA_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        REPORTS_DIR,
        MODELS_DIR,
        os.path.join(BASE_DIR, 'logs')
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

# Garantir que os diretórios existam ao importar o módulo
ensure_directories()

if __name__ == "__main__":
    print(f"Diretório base: {BASE_DIR}")
    print(f"Diretório de dados: {DATA_DIR}")
    print(f"Diretório de relatórios: {REPORTS_DIR}")
