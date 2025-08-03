# Guia de Início Rápido

Este guia irá ajudá-lo a configurar e executar o projeto de análise de e-commerce com PySpark.

## Pré-requisitos

- Python 3.8 ou superior
- Java 8 ou 11 (necessário para o PySpark)
- Git
- Docker e Docker Compose (opcional, para execução em contêineres)

## Configuração do Ambiente

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/ecommerce-analysis.git
cd ecommerce-analysis
```

### 2. Crie um ambiente virtual (recomendado)

```bash
python -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate
```

### 3. Instale as dependências

```bash
pip install -e ".[dev]"  # Instala o pacote em modo de desenvolvimento com dependências extras
pre-commit install  # Configura os hooks do pre-commit
```

## Estrutura do Projeto

```
ecommerce-analysis/
├── config/               # Configurações do projeto
├── data/                 # Dados brutos e processados
│   ├── raw/              # Dados brutos
│   └── processed/        # Dados processados
├── docs/                 # Documentação
├── logs/                 # Arquivos de log
├── models/               # Modelos de machine learning
├── notebooks/            # Jupyter notebooks para análise exploratória
├── reports/              # Relatórios e visualizações gerados
├── src/                  # Código-fonte
│   ├── analysis/         # Módulos de análise
│   ├── etl/              # Módulos de ETL
│   ├── models/           # Modelos de dados
│   ├── utils/            # Utilitários
│   ├── __init__.py
│   ├── analysis.py       # Script principal de análise
│   ├── etl.py            # Script principal de ETL
│   └── utils.py          # Utilitários comuns
├── tests/                # Testes automatizados
├── .env.example          # Exemplo de variáveis de ambiente
├── .gitignore
├── .pre-commit-config.yaml
├── docker-compose.yml    # Configuração do Docker Compose
├── Dockerfile           # Configuração do Docker
├── Makefile             # Comandos úteis
├── pyproject.toml       # Configuração do projeto e dependências
└── README.md            # Documentação principal
```

## Uso Básico

### 1. Gerar Dados de Exemplo

```bash
make generate-data
```

### 2. Executar o Pipeline ETL

```bash
make etl
```

### 3. Executar Análises

```bash
make analysis
```

### 4. Iniciar Jupyter Lab para Análise Interativa

```bash
make jupyter-lab
```

## Executando com Docker

### 1. Construa a imagem

```bash
docker-compose build
```

### 2. Inicie os serviços

```bash
docker-compose up -d
```

### 3. Acesse as ferramentas

- Jupyter Lab: http://localhost:8888
- Spark UI: http://localhost:4040
- Metabase: http://localhost:3000
- Grafana: http://localhost:3001
- Prometheus: http://localhost:9090

## Desenvolvimento

### Executando Testes

```bash
make test
```

### Verificando Qualidade do Código

```bash
make lint
```

### Formatando o Código

```bash
make format
```

## Solução de Problemas

### Erro de Memória do Spark

Se encontrar erros de memória, você pode ajustar as configurações de memória:

1. No arquivo `config/settings.py`, modifique:
   ```python
   'spark.driver.memory': '4g',
   'spark.executor.memory': '4g',
   ```

2. Ou defina variáveis de ambiente:
   ```bash
   export SPARK_DRIVER_MEMORY=4g
   export SPARK_EXECUTOR_MEMORY=4g
   ```

### Problemas com Dependências

Se encontrar problemas com dependências, tente:

```bash
pip install -U pip setuptools wheel
pip install -e ".[dev]" --no-cache-dir
```

## Contribuindo

1. Faça um fork do repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Faça commit das suas alterações (`git commit -am 'Adiciona nova feature'`)
4. Faça push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo [LICENSE](LICENSE) para mais detalhes.

## Contato

Seu Nome - [@seu_usuario](https://twitter.com/seu_usuario)

Link do Projeto: [https://github.com/seu-usuario/ecommerce-analysis](https://github.com/seu-usuario/ecommerce-analysis)
