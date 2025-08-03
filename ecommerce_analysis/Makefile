.PHONY: help install install-dev setup test lint format clean

# Cor para formatação
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

# Nome do ambiente virtual
VENV_NAME ?= venv
VENV_ACTIVATE = . $(VENV_NAME)/bin/activate
PYTHON = $(VENV_NAME)/bin/python3
PIP = $(VENV_NAME)/bin/pip

# Comandos
SPARK_SUBMIT = spark-submit --master local[*] --driver-memory 4g --executor-memory 4g
PYTHONPATH = PYTHONPATH=$(PWD)

# Ajuda
help:
	@echo ''
	@echo 'Comandos disponíveis:'
	@echo ''
	@echo '  ${YELLOW}make help${RESET}           Mostra esta mensagem de ajuda' 
	@echo '  ${YELLOW}make setup${RESET}          Configura o ambiente de desenvolvimento' 
	@echo '  ${YELLOW}make install${RESET}        Instala o pacote no modo de desenvolvimento' 
	@echo '  ${YELLOW}make install-dev${RESET}    Instala as dependências de desenvolvimento' 
	@echo '  ${YELLOW}make generate-data${RESET}  Gera dados de exemplo' 
	@echo '  ${YELLOW}make etl${RESET}            Executa o pipeline ETL' 
	@echo '  ${YELLOW}make analysis${RESET}       Executa as análises' 
	@echo '  ${YELLOW}make test${RESET}           Executa os testes' 
	@echo '  ${YELLOW}make lint${RESET}           Verifica a qualidade do código' 
	@echo '  ${YELLOW}make format${RESET}         Formata o código automaticamente' 
	@echo '  ${YELLOW}make clean${RESET}          Limpa arquivos gerados' 
	@echo ''

# Configuração do ambiente
setup: venv install install-dev

# Cria ambiente virtual
venv:
	@echo "${GREEN}>>> Criando ambiente virtual...${RESET}"
	python3 -m venv $(VENV_NAME)

# Instala o pacote no modo de desenvolvimento
install: venv
	@echo "${GREEN}>>> Instalando o pacote...${RESET}"
	$(PIP) install -e .

# Instala dependências de desenvolvimento
install-dev: venv
	@echo "${GREEN}>>> Instalando dependências de desenvolvimento...${RESET}"
	$(PIP) install -e ".[dev]"

# Gera dados de exemplo
generate-data: venv
	@echo "${GREEN}>>> Gerando dados de exemplo...${RESET}"
	$(PYTHONPATH) $(PYTHON) -m src.utils

# Executa o pipeline ETL
etl: venv
	@echo "${GREEN}>>> Executando o pipeline ETL...${RESET}"
	$(PYTHONPATH) $(PYTHON) -m src.etl

# Executa as análises
analysis: venv
	@echo "${GREEN}>>> Executando análises...${RESET}"
	$(PYTHONPATH) $(PYTHON) -m src.analysis

# Executa os testes
test: venv
	@echo "${GREEN}>>> Executando testes...${RESET}"
	$(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing

# Verifica a qualidade do código
lint: venv
	@echo "${GREEN}>>> Verificando estilo do código...${RESET}"
	$(PYTHON) -m flake8 src/
	$(PYTHON) -m black --check src/
	$(PYTHON) -m isort --check-only src/
	$(PYTHON) -m mypy src/

# Formata o código automaticamente
format: venv
	@echo "${GREEN}>>> Formatando o código...${RESET}"
	$(PYTHON) -m black src/
	$(PYTHON) -m isort src/

# Limpa arquivos gerados
clean:
	@echo "${GREEN}>>> Limpando arquivos gerados...${RESET}"
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".ipynb_checkpoints" -exec rm -r {} +
	find . -type f -name "*.py[co]" -delete
	find . -type f -name "*~" -delete
	find . -type f -name "*.bak" -delete

dist-clean: clean
	@echo "${GREEN}>>> Removendo ambiente virtual e arquivos de build...${RESET}"
	rm -rf $(VENV_NAME)
	rm -rf build/ dist/ *.egg-info/
	rm -f .coverage .coverage.*
	rm -rf htmlcov/

# Comandos auxiliares para desenvolvimento
notebook: venv
	@echo "${GREEN}>>> Iniciando Jupyter Notebook...${RESET}"
	$(PYTHONPATH) jupyter notebook

jupyter-lab: venv
	@echo "${GREEN}>>> Iniciando Jupyter Lab...${RESET}"
	$(PYTHONPATH) jupyter lab

pyspark: venv
	@echo "${GREEN}>>> Iniciando PySpark shell...${RESET}"
	$(PYTHONPATH) pyspark --master local[*] --driver-memory 4g --executor-memory 4g

# Ajuda para comandos específicos
help-generate-data:
	@echo "${GREEN}Uso: make generate-data [OPÇÕES]${RESET}"
	@echo ""
	@echo "Gera dados de exemplo para o projeto."
	@echo ""
	@echo "Opções:"
	@echo "  NUM_CLIENTS=N   Número de clientes a serem gerados (padrão: 1000)"
	@echo "  NUM_PRODUCTS=N  Número de produtos a serem gerados (padrão: 100)"
	@echo "  NUM_ORDERS=N    Número de pedidos a serem gerados (padrão: 5000)"
	@echo ""
	@echo "Exemplo:"
	@echo "  make generate-data NUM_CLIENTS=2000 NUM_PRODUCTS=150 NUM_ORDERS=10000"
