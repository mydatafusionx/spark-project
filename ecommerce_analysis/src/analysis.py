import os
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as _sum, count as _count, max as _max, min as _min, avg as _avg

from utils import create_spark_session, load_data

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração de estilo para os gráficos
plt.style.use('seaborn')
sns.set_palette("viridis")

class EcommerceAnalyzer:
    """
    Classe responsável por realizar análises e gerar visualizações dos dados de e-commerce
    """
    
    def __init__(self, spark, processed_dir="../data/processed", reports_dir="../reports"):
        """
        Inicializa o analisador de dados
        
        Args:
            spark: Sessão Spark
            processed_dir: Diretório com os dados processados
            reports_dir: Diretório para salvar os relatórios
        """
        self.spark = spark
        self.processed_dir = processed_dir
        self.reports_dir = reports_dir
        
        # Criar diretório de relatórios se não existir
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # Carregar dados processados
        self._load_processed_data()
    
    def _load_processed_data(self):
        """
        Carrega os dados processados para análise
        """
        try:
            logger.info("Carregando dados processados...")
            
            # Carregar dados processados
            self.fato_vendas = self.spark.read.parquet(f"{self.processed_dir}/fato_vendas")
            self.dim_clientes = self.spark.read.parquet(f"{self.processed_dir}/dim_clientes")
            self.dim_produtos = self.spark.read.parquet(f"{self.processed_dir}/dim_produtos")
            self.dim_tempo = self.spark.read.parquet(f"{self.processed_dir}/dim_tempo")
            
            logger.info("Dados processados carregados com sucesso!")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados processados: {str(e)}")
            raise
    
    def analyze_sales_by_category(self):
        """
        Analisa as vendas por categoria de produto
        """
        logger.info("Analisando vendas por categoria...")
        
        try:
            # Agrupar por categoria e calcular métricas
            vendas_por_categoria = self.fato_vendas.groupBy("categoria").agg(
                _sum("valor_total_item").alias("faturamento_total"),
                _sum("quantidade").alias("quantidade_vendida"),
                _countDistinct("pedido_id").alias("total_pedidos"),
                _avg("valor_total_item").alias("ticket_medio_por_item")
            ).orderBy("faturamento_total", ascending=False)
            
            # Converter para Pandas para visualização
            vendas_pd = vendas_por_categoria.toPandas()
            
            # Criar visualização
            plt.figure(figsize=(12, 6))
            
            # Gráfico de barras para faturamento por categoria
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
            
            # Salvar gráfico
            output_path = f"{self.reports_dir}/faturamento_por_categoria.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Análise de vendas por categoria concluída. Gráfico salvo em: {output_path}")
            return vendas_por_categoria
            
        except Exception as e:
            logger.error(f"Erro na análise de vendas por categoria: {str(e)}")
            raise
    
    def analyze_sales_trends(self):
        """
        Analisa as tendências de vendas ao longo do tempo
        """
        logger.info("Analisando tendências de vendas...")
        
        try:
            # Juntar fatos com dimensão de tempo
            vendas_com_tempo = self.fato_vendas.join(
                self.dim_tempo,
                self.fato_vendas["data_pedido_date"] == self.dim_tempo["data"],
                "inner"
            )
            
            # Agrupar por mês/ano e calcular métricas
            tendencias = vendas_com_tempo.groupBy("mes_ano_pedido").agg(
                _sum("valor_total_item").alias("faturamento_total"),
                _countDistinct("pedido_id").alias("total_pedidos"),
                _avg("valor_total").alias("ticket_medio_por_pedido")
            ).orderBy("mes_ano_pedido")
            
            # Converter para Pandas para visualização
            tendencias_pd = tendencias.toPandas()
            
            # Criar visualização
            plt.figure(figsize=(14, 6))
            
            # Gráfico de linha para tendência de faturamento
            ax = sns.lineplot(
                x="mes_ano_pedido", 
                y="faturamento_total", 
                data=tendencias_pd,
                marker="o"
            )
            
            # Adicionar valores nos pontos
            for i, row in tendencias_pd.iterrows():
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
            
            # Salvar gráfico
            output_path = f"{self.reports_dir}/tendencia_faturamento.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Análise de tendências de vendas concluída. Gráfico salvo em: {output_path}")
            return tendencias
            
        except Exception as e:
            logger.error(f"Erro na análise de tendências de vendas: {str(e)}")
            raise
    
    def analyze_customer_behavior(self):
        """
        Analisa o comportamento dos clientes
        """
        logger.info("Analisando comportamento dos clientes...")
        
        try:
            # Calcular métricas por cliente
            comportamento_clientes = self.fato_vendas.groupBy("cliente_id").agg(
                _sum("valor_total_item").alias("valor_total_gasto"),
                _countDistinct("pedido_id").alias("total_pedidos"),
                _sum("quantidade").alias("total_itens_comprados"),
                _avg("valor_total_item").alias("valor_medio_por_item"),
                _max("data_pedido").alias("ultima_compra")
            )
            
            # Juntar com dados demográficos dos clientes
            comportamento_clientes = comportamento_clientes.join(
                self.dim_clientes,
                "cliente_id",
                "left"
            )
            
            # Análise por faixa etária
            analise_faixa_etaria = comportamento_clientes.groupBy("faixa_etaria").agg(
                _count("cliente_id").alias("total_clientes"),
                _avg("valor_total_gasto").alias("gasto_medio"),
                _avg("total_pedidos").alias("pedidos_medio")
            ).orderBy("faixa_etaria")
            
            # Converter para Pandas para visualização
            analise_pd = analise_faixa_etaria.toPandas()
            
            # Criar visualização
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
            
            # Salvar gráfico
            output_path = f"{self.reports_dir}/analise_faixa_etaria.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Análise de comportamento dos clientes concluída. Gráficos salvos em: {output_path}")
            return analise_faixa_etaria
            
        except Exception as e:
            logger.error(f"Erro na análise de comportamento dos clientes: {str(e)}")
            raise
    
    def generate_sales_report(self):
        """
        Gera um relatório completo de vendas
        """
        logger.info("Gerando relatório completo de vendas...")
        
        try:
            # Executar análises
            vendas_por_categoria = self.analyze_sales_by_category()
            tendencias = self.analyze_sales_trends()
            comportamento_clientes = self.analyze_customer_behavior()
            
            # Calcular métricas gerais
            total_vendas = self.fato_vendas.agg(_sum("valor_total_item").alias("total")).collect()[0]["total"]
            total_pedidos = self.fato_vendas.select("pedido_id").distinct().count()
            ticket_medio = total_vendas / total_pedidos if total_pedidos > 0 else 0
            
            # Criar relatório em HTML
            report_content = f"""
            <html>
            <head>
                <title>Relatório de Vendas - E-commerce</title>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                    .container {{ width: 90%; margin: 0 auto; }}
                    .header {{ text-align: center; margin-bottom: 30px; }}
                    .section {{ margin-bottom: 30px; }}
                    .metric-card {{ 
                        background-color: #f8f9fa; 
                        border-radius: 5px; 
                        padding: 15px; 
                        margin-bottom: 20px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }}
                    .metrics-container {{ 
                        display: flex; 
                        justify-content: space-between; 
                        flex-wrap: wrap; 
                    }}
                    .metric {{ 
                        flex: 1; 
                        min-width: 200px; 
                        margin: 10px; 
                    }}
                    .metric-value {{ 
                        font-size: 24px; 
                        font-weight: bold; 
                        color: #2c3e50;
                    }}
                    .metric-label {{ 
                        font-size: 14px; 
                        color: #7f8c8d;
                    }}
                    img {{ 
                        max-width: 100%; 
                        height: auto; 
                        margin-bottom: 20px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>Relatório de Vendas - E-commerce</h1>
                        <p>Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                    </div>
                    
                    <div class="section">
                        <h2>Métricas Principais</h2>
                        <div class="metrics-container">
                            <div class="metric">
                                <div class="metric-value">R$ {total_vendas:,.2f}</div>
                                <div class="metric-label">Faturamento Total</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value">{total_pedidos:,}</div>
                                <div class="metric-label">Total de Pedidos</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value">R$ {ticket_medio:,.2f}</div>
                                <div class="metric-label">Ticket Médio</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="section">
                        <h2>Análise por Categoria</h2>
                        <img src="faturamento_por_categoria.png" alt="Faturamento por Categoria">
                    </div>
                    
                    <div class="section">
                        <h2>Tendência de Vendas</h2>
                        <img src="tendencia_faturamento.png" alt="Tendência de Faturamento">
                    </div>
                    
                    <div class="section">
                        <h2>Análise por Faixa Etária</h2>
                        <img src="analise_faixa_etaria.png" alt="Análise por Faixa Etária">
                    </div>
                </div>
            </body>
            </html>
            """
            
            # Salvar relatório
            output_path = f"{self.reports_dir}/relatorio_vendas.html"
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            logger.info(f"Relatório de vendas gerado com sucesso em: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de vendas: {str(e)}")
            raise

def main():
    """
    Função principal para execução das análises
    """
    try:
        # Criar sessão Spark
        spark = create_spark_session("EcommerceAnalysis")
        
        # Executar análises
        analyzer = EcommerceAnalyzer(spark)
        analyzer.generate_sales_report()
        
        # Encerrar sessão Spark
        spark.stop()
        
    except Exception as e:
        logger.error(f"Erro na execução das análises: {str(e)}")
        raise

if __name__ == "__main__":
    main()