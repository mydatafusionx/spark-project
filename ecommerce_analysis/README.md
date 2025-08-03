# Projeto de Análise de Dados de E-commerce com PySpark

Este projeto demonstra o uso do PySpark para processamento e análise de dados de e-commerce, incluindo um pipeline ETL completo e análises avançadas.

## Estrutura do Projeto

```
ecommerce_analysis/
├── config/               # Arquivos de configuração
├── data/                 # Dados brutos e processados
│   ├── raw/             # Dados brutos
│   └── processed/       # Dados processados
├── notebooks/           # Jupyter notebooks para análise exploratória
├── reports/             # Relatórios e visualizações
├── src/                 # Código-fonte
│   ├── __init__.py
│   ├── utils.py        # Funções auxiliares
│   ├── etl.py          # Pipeline ETL
│   └── analysis.py     # Análises e visualizações
├── requirements.txt    # Dependências do projeto
└── README.md           # Este arquivo
```

## Configuração do Ambiente

1. Crie um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # No Linux/Mac
   # ou
   .\venv\Scripts\activate  # No Windows
   ```

2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## Como Usar

1. Gere dados de exemplo (opcional):
   ```bash
   python -m src.utils
   ```

2. Execute o pipeline ETL:
   ```bash
   python -m src.etl
   ```

3. Execute as análises:
   ```bash
   python -m src.analysis
   ```

## Recursos

- Processamento distribuído com PySpark
- Pipeline ETL completo
- Análise exploratória de dados
- Visualizações com Matplotlib e Seaborn
- Dados de exemplo gerados automaticamente

## Requisitos

- Python 3.8+
- Java 8 ou superior
- PySpark 3.4.0+
- Bibliotecas listadas em requirements.txt