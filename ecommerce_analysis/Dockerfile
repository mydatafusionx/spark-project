# Use a imagem base do Python
FROM python:3.9-slim

# Define variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.4.2 \
    SPARK_VERSION=3.4.0 \
    HADOOP_VERSION=3 \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH="${PATH}:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    curl \
    wget \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Instala o Spark
RUN wget -qO- https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar xvz -C /opt/ \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Configura o diretório de trabalho
WORKDIR /app

# Copia os arquivos necessários
COPY pyproject.toml poetry.lock* ./
COPY README.md ./
COPY setup.py ./
COPY src/ ./src/
COPY config/ ./config/
COPY notebooks/ ./notebooks/

# Instala o Poetry e as dependências do projeto
RUN pip install "poetry==$POETRY_VERSION" \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# Cria diretórios necessários
RUN mkdir -p /app/data/raw \
    && mkdir -p /app/data/processed \
    && mkdir -p /app/reports \
    && mkdir -p /app/logs

# Expõe portas necessárias
EXPOSE 4040 8888

# Comando padrão
CMD ["bash"]
