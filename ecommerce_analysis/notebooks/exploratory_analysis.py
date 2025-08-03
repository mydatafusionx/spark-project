"""
Análise Exploratória de Dados de E-commerce

Este script contém uma análise exploratória dos dados de e-commerce processados pelo pipeline ETL.
Pode ser convertido em um notebook Jupyter usando ferramentas como `jupytext`.
"""

# %% [markdown]
# Análise Exploratória de Dados de E-commerce
# 
# Este notebook contém uma análise exploratória dos dados de e-commerce processados pelo pipeline ETL.

# %%
# Configuração inicial
import os
import sys
import findspark

# Adicionar o diretório raiz ao path para importar módulos locais
sys.path.append(os.path.abspath(os.path.join('..')))

# Inicializar o findspark
findspark.init()

# Configurações de exibição
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.float_format', '{:,.2f}'.format)

# Importações
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Importar módulos locais
from src.utils import create_spark_session, load_data

# Configuração de estilo para os gráficos
plt.style.use('seaborn')
sns.set_palette("viridis")

# %% [markdown]
# 1. Configuração Inicial
# 
# Vamos começar inicializando a sessão Spark e carregando os dados processados.

# %%
# Inicializar a sessão Spark
spark = create_spark_session("EcommerceAnalysisNotebook")

# %%
# Carregar dados processados
fato_vendas = spark.read.parquet("../data/processed/fato_vendas")
dim_clientes = spark.read.parquet("../data/processed/dim_clientes")
dim_produtos = spark.read.parquet("../data/processed/dim_produtos")
dim_tempo = spark.read.parquet("../data/processed/dim_tempo")

print("Dados carregados com sucesso!")
print(f"Total de registros em fato_vendas: {fato_vendas.count():,}")
print(f"Total de clientes: {dim_clientes.count():,}")
print(f"Total de produtos: {dim_produtos.count():,}")

# %% [markdown]
# 2. Análise de Vendas por Categoria

# %%
# Vendas por categoria
vendas_por_categoria = fato_vendas.groupBy("categoria").agg(
    F.sum("valor_total_item").alias("faturamento_total"),
    F.sum("quantidade").alias("quantidade_vendida"),
    F.countDistinct("pedido_id").alias("total_pedidos"),
    F.avg("valor_total_item").alias("ticket_medio_por_item")
).orderBy("faturamento_total", ascending=False)

# Exibir resultados
vendas_por_categoria.show(truncate=False)

# %%
# Visualização de faturamento por categoria
vendas_pd = vendas_por_categoria.toPandas()

plt.figure(figsize=(12, 6))
ax = sns.barplot(
    x="categoria", 
    y="faturamento_total", 
    data=vendas_pd,
    estimator=sum,
    ci=None
)

# Adicionar valores nas barras
for p in ax.patches:
    ax.annotate(
        f'R$ {p.get_height():.2f}',
        (p.get_x() + p.get_width() / 2., p.get_height()),
        ha='center',
        va='center',
        xytext=(0, 10),
        textcoords='offset points'
    )

plt.title("Faturamento por Categoria de Produto")
plt.xlabel("Categoria")
plt.ylabel("Faturamento Total (R$)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("../reports/faturamento_por_categoria_notebook.png", dpi=300, bbox_inches='tight')
plt.show()

# %% [markdown]
# 3. Análise de Tendências de Vendas ao Longo do Tempo

# %%
# Vendas ao longo do tempo
vendas_por_mes = fato_vendas.groupBy("mes_ano_pedido").agg(
    F.sum("valor_total_item").alias("faturamento_total"),
    F.countDistinct("pedido_id").alias("total_pedidos"),
    F.avg("valor_total").alias("ticket_medio_por_pedido")
).orderBy("mes_ano_pedido")

# Exibir resultados
vendas_por_mes.show(truncate=False)

# %%
# Visualização de tendência de vendas
vendas_por_mes_pd = vendas_por_mes.toPandas()

plt.figure(figsize=(14, 6))
ax = sns.lineplot(
    x="mes_ano_pedido", 
    y="faturamento_total", 
    data=vendas_por_mes_pd,
    marker="o"
)

# Adicionar valores nos pontos
for i, row in vendas_por_mes_pd.iterrows():
    ax.text(
        i,
        row['faturamento_total'],
        f'R$ {row["faturamento_total"]:,.2f}',
        ha='center',
        va='bottom'
    )

plt.title("Tendência de Faturamento por Mês")
plt.xlabel("Mês/Ano")
plt.ylabel("Faturamento Total (R$)")
plt.xticks(rotation=45)
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig("../reports/tendencia_faturamento_notebook.png", dpi=300, bbox_inches='tight')
plt.show()

# %% [markdown]
# 4. Análise de Comportamento dos Clientes

# %%
# Comportamento dos clientes
comportamento_clientes = fato_vendas.groupBy("cliente_id").agg(
    F.sum("valor_total_item").alias("valor_total_gasto"),
    F.countDistinct("pedido_id").alias("total_pedidos"),
    F.sum("quantidade").alias("total_itens_comprados"),
    F.avg("valor_total_item").alias("valor_medio_por_item"),
    F.max("data_pedido").alias("ultima_compra")
)

# Juntar com dados demográficos dos clientes
comportamento_clientes = comportamento_clientes.join(
    dim_clientes,
    "cliente_id",
    "left"
)

# Análise por faixa etária
analise_faixa_etaria = comportamento_clientes.groupBy("faixa_etaria").agg(
    F.count("cliente_id").alias("total_clientes"),
    F.avg("valor_total_gasto").alias("gasto_medio"),
    F.avg("total_pedidos").alias("pedidos_medio")
).orderBy("faixa_etaria")

# Exibir resultados
analise_faixa_etaria.show(truncate=False)

# %%
# Visualização de análise por faixa etária
analise_pd = analise_faixa_etaria.toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Gráfico de barras para gasto médio por faixa etária
sns.barplot(
    x="faixa_etaria", 
    y="gasto_medio", 
    data=analise_pd,
    ax=ax1
)
ax1.set_title("Gasto Médio por Faixa Etária")
ax1.set_xlabel("Faixa Etária")
ax1.set_ylabel("Gasto Médio (R$)")

# Gráfico de barras para pedidos médios por faixa etária
sns.barplot(
    x="faixa_etaria", 
    y="pedidos_medio", 
    data=analise_pd,
    ax=ax2
)
ax2.set_title("Média de Pedidos por Faixa Etária")
ax2.set_xlabel("Faixa Etária")
ax2.set_ylabel("Média de Pedidos")

plt.tight_layout()
plt.savefig("../reports/analise_faixa_etaria_notebook.png", dpi=300, bbox_inches='tight')
plt.show()

# %% [markdown]
# 5. Análise de Produtos Mais Vendidos

# %%
# Produtos mais vendidos
produtos_mais_vendidos = fato_vendas.groupBy("produto_id").agg(
    F.first("nome").alias("nome_produto"),
    F.first("categoria").alias("categoria"),
    F.sum("quantidade").alias("quantidade_vendida"),
    F.sum("valor_total_item").alias("faturamento_total"),
    F.avg("preco_unitario").alias("preco_medio")
).orderBy("quantidade_vendida", ascending=False).limit(10)

# Exibir resultados
produtos_mais_vendidos.show(truncate=False)

# %%
# Visualização dos produtos mais vendidos
produtos_pd = produtos_mais_vendidos.toPandas()

plt.figure(figsize=(14, 6))
ax = sns.barplot(
    x="nome_produto", 
    y="quantidade_vendida", 
    data=produtos_pd,
    hue="categoria",
    dodge=False
)

plt.title("Top 10 Produtos Mais Vendidos por Quantidade")
plt.xlabel("Produto")
plt.ylabel("Quantidade Vendida")
plt.xticks(rotation=45, ha='right')
plt.legend(title="Categoria", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig("../reports/top_produtos_vendidos.png", dpi=300, bbox_inches='tight')
plt.show()

# %% [markdown]
# 6. Análise de RFM (Recência, Frequência, Valor Monetário)

# %%
# Cálculo do RFM
from pyspark.sql.functions import datediff, current_date, col

# Data de referência (última data de compra + 1 dia)
data_referencia = fato_vendas.agg(F.max("data_pedido")).collect()[0][0]

# Calcular métricas RFM
rfm = fato_vendas.groupBy("cliente_id").agg(
    F.datediff(F.lit(data_referencia), F.max("data_pedido")).alias("recencia"),
    F.countDistinct("pedido_id").alias("frequencia"),
    F.sum("valor_total_item").alias("valor_monetario")
)

# Exibir métricas RFM
rfm.show(5)

# %%
# Criar segmentos RFM
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile, col

# Definir o número de segmentos
num_segmentos = 5

# Criar segmentos para cada métrica RFM
rfm_segmentos = rfm.withColumn(
    "r_segmento",
    ntile(num_segmentos).over(Window.orderBy(col("recencia").asc()))
).withColumn(
    "f_segmento",
    ntile(num_segmentos).over(Window.orderBy(col("frequencia").desc()))
).withColumn(
    "m_segmento",
    ntile(num_segmentos).over(Window.orderBy(col("valor_monetario").desc()))
)

# Calcular pontuação RFM (soma dos segmentos)
rfm_resultado = rfm_segmentos.withColumn(
    "rfm_score",
    (col("r_segmento") * 0.3 + 
     col("f_segmento") * 0.3 + 
     col("m_segmento") * 0.4)
)

# Classificar clientes com base no score RFM
rfm_resultado = rfm_resultado.withColumn(
    "segmento_rfm",
    F.when(col("rfm_score") >= 4.5, "Campeões")
    .when((col("rfm_score") >= 4.0) & (col("rfm_score") < 4.5), "Clientes Leais")
    .when((col("rfm_score") >= 3.5) & (col("rfm_score") < 4.0), "Clientes em Crescimento")
    .when((col("rfm_score") >= 3.0) & (col("rfm_score") < 3.5), "Clientes Promissores")
    .when((col("rfm_score") >= 2.0) & (col("rfm_score") < 3.0), "Clientes Ocasionais")
    .otherwise("Clientes em Risco")
)

# Juntar com dados dos clientes para análise
rfm_completo = rfm_resultado.join(
    dim_clientes,
    "cliente_id",
    "left"
)

# Exibir contagem por segmento
rfm_completo.groupBy("segmento_rfm").count().orderBy("count", ascending=False).show()

# %%
# Visualização da distribuição dos segmentos RFM
segmentos_pd = rfm_completo.groupBy("segmento_rfm").count().orderBy("count", ascending=False).toPandas()

plt.figure(figsize=(12, 6))
ax = sns.barplot(
    x="segmento_rfm",
    y="count",
    data=segmentos_pd
)

plt.title("Distribuição de Clientes por Segmento RFM")
plt.xlabel("Segmento RFM")
plt.ylabel("Número de Clientes")
plt.xticks(rotation=45, ha='right')

# Adicionar contagens nas barras
for p in ax.patches:
    ax.annotate(
        f'{int(p.get_height())}',
        (p.get_x() + p.get_width() / 2., p.get_height()),
        ha='center',
        va='center',
        xytext=(0, 10),
        textcoords='offset points'
    )

plt.tight_layout()
plt.savefig("../reports/segmentacao_rfm.png", dpi=300, bbox_inches='tight')
plt.show()

# %% [markdown]
# 7. Análise de Cestas de Compras (Market Basket Analysis)

# %%
# Pré-processamento para análise de cesta de compras
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import collect_list, array_distinct

# Criar um DataFrame com arrays de itens por pedido
itens_por_pedido = fato_vendas.groupBy("pedido_id").agg(
    array_distinct(collect_list("nome")).alias("itens")
)

# Inicializar o algoritmo FPGrowth
fpGrowth = FPGrowth(itemsCol="itens", minSupport=0.05, minConfidence=0.2)

# Treinar o modelo
model = fpGrowth.fit(itens_por_pedido)

# Exibir os itens frequentes (frequent itemsets)
print("Itens frequentes (minSupport = 0.05):")
model.freqItemsets.show(10, truncate=False)

# Exibir as regras de associação
print("\nRegras de associação (minConfidence = 0.2):")
model.associationRules.show(10, truncate=False)

# Gerar previsões (consequentes com base nos antecedentes)
print("\nPrevisões (itens frequentemente comprados juntos):")
model.transform(itens_por_pedido).show(5, truncate=False)

# %% [markdown]
# 8. Análise de Sazonalidade

# %%
# Análise de sazonalidade por dia da semana
vendas_por_dia_semana = fato_vendas.join(
    dim_tempo,
    fato_vendas["data_pedido_date"] == dim_tempo["data"],
    "inner"
).groupBy("dia_semana").agg(
    F.sum("valor_total_item").alias("faturamento_total"),
    F.countDistinct("pedido_id").alias("total_pedidos")
).orderBy("dia_semana")

# Mapear números para nomes dos dias
from pyspark.sql.functions import when

vendas_por_dia_semana = vendas_por_dia_semana.withColumn(
    "dia_semana_nome",
    when(col("dia_semana") == 1, "Domingo")
    .when(col("dia_semana") == 2, "Segunda")
    .when(col("dia_semana") == 3, "Terça")
    .when(col("dia_semana") == 4, "Quarta")
    .when(col("dia_semana") == 5, "Quinta")
    .when(col("dia_semana") == 6, "Sexta")
    .when(col("dia_semana") == 7, "Sábado")
)

# Exibir resultados
vendas_por_dia_semana.show()

# %%
# Visualização de vendas por dia da semana
vendas_dia_semana_pd = vendas_por_dia_semana.toPandas()

plt.figure(figsize=(12, 6))
ax = sns.barplot(
    x="dia_semana_nome",
    y="faturamento_total",
    data=vendas_dia_semana_pd,
    order=["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
)

plt.title("Faturamento por Dia da Semana")
plt.xlabel("Dia da Semana")
plt.ylabel("Faturamento Total (R$)")

# Adicionar valores nas barras
for p in ax.patches:
    ax.annotate(
        f'R$ {p.get_height():,.2f}',
        (p.get_x() + p.get_width() / 2., p.get_height()),
        ha='center',
        va='center',
        xytext=(0, 10),
        textcoords='offset points'
    )

plt.tight_layout()
plt.savefig("../reports/faturamento_por_dia_semana.png", dpi=300, bbox_inches='tight')
plt.show()

# %% [markdown]
# 9. Análise de Valor de Vida do Cliente (CLV - Customer Lifetime Value)

# %%
# Cálculo do CLV (Customer Lifetime Value)
clv = fato_vendas.groupBy("cliente_id").agg(
    F.first("nome").alias("nome_cliente"),
    F.first("cidade").alias("cidade"),
    F.first("estado").alias("estado"),
    F.datediff(F.max("data_pedido"), F.min("data_pedido")).alias("dias_entre_primeira_ultima_compra"),
    F.countDistinct("pedido_id").alias("total_pedidos"),
    F.sum("valor_total_item").alias("valor_total_gasto"),
    F.avg("valor_total").alias("ticket_medio")
)

# Calcular métricas adicionais para CLV
clv = clv.withColumn(
    "frequencia_compra",
    when(col("dias_entre_primeira_ultima_compra") > 0,
        col("total_pedidos") / (col("dias_entre_primeira_ultima_compra") / 30.0)  # Pedidos por mês
    ).otherwise(col("total_pedidos"))
).withColumn(
    "valor_medio_por_mes",
    col("valor_total_gasto") / (col("dias_entre_primeira_ultima_compra") / 30.0)  # Valor médio por mês
)

# Ordenar por valor total gasto (maiores clientes primeiro)
clv_ordenado = clv.orderBy("valor_total_gasto", ascending=False)

# Exibir os 10 melhores clientes
print("Top 10 Clientes por Valor Total Gasto:")
clv_ordenado.select(
    "nome_cliente", "cidade", "estado", "total_pedidos", 
    "valor_total_gasto", "ticket_medio", "valor_medio_por_mes"
).show(10, truncate=False)

# %%
# Visualização da distribuição do valor total gasto pelos clientes
clv_pd = clv_ordenado.limit(50).toPandas()  # Limitar a 50 clientes para visualização

plt.figure(figsize=(14, 6))
sns.scatterplot(
    x="total_pedidos",
    y="valor_total_gasto",
    data=clv_pd,
    hue="estado",
    size="valor_medio_por_mes",
    sizes=(50, 500),
    alpha=0.7
)

plt.title("Relação entre Número de Pedidos e Valor Total Gasto por Cliente")
plt.xlabel("Número Total de Pedidos")
plt.ylabel("Valor Total Gasto (R$)")
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig("../reports/analise_clv.png", dpi=300, bbox_inches='tight')
plt.show()

# %% [markdown]
# 10. Conclusões e Próximos Passos
# 
# Nesta análise exploratória, examinamos diversos aspectos dos dados de e-commerce, incluindo:
# 
# 1. **Vendas por Categoria**: Identificamos as categorias de produtos mais lucrativas.
# 2. **Tendências Temporais**: Analisamos como as vendas variam ao longo do tempo.
# 3. **Comportamento do Cliente**: Examinamos padrões de compra por faixa etária.
# 4. **Produtos Mais Vendidos**: Identificamos os produtos mais populares.
# 5. **Segmentação RFM**: Classificamos os clientes com base em Recência, Frequência e Valor Monetário.
# 6. **Análise de Cestas de Compras**: Identificamos produtos frequentemente comprados juntos.
# 7. **Sazonalidade**: Analisamos padrões de vendas por dia da semana.
# 8. **Valor de Vida do Cliente (CLV)**: Calculamos métricas para entender o valor dos clientes ao longo do tempo.
# 
# ### Próximos Passos Sugeridos:
# 
# 1. **Análise de Churn**: Identificar clientes inativos e desenvolver estratégias de retenção.
# 2. **Previsão de Demanda**: Implementar modelos para prever vendas futuras.
# 3. **Recomendação de Produtos**: Desenvolver um sistema de recomendação baseado nas análises de cesta.
# 4. **Segmentação Avançada**: Aplicar técnicas de clustering para identificar grupos de clientes com comportamentos semelhantes.
# 5. **Análise de Lucratividade**: Incorporar dados de custos para calcular a lucratividade por produto/cliente.
# 6. **Testes A/B**: Implementar testes controlados para otimizar preços, promoções e layout do site.

# %%
# Encerrar a sessão Spark
spark.stop()
print("Análise concluída e sessão Spark encerrada.")

# %% [markdown]
# ---
# 
# **Nota**: Este notebook foi projetado para ser executado em um ambiente com PySpark configurado. Certifique-se de que todas as dependências estejam instaladas e que os caminhos dos arquivos de dados estejam corretos para o seu ambiente.
