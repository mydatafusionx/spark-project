import os
import random
import pandas as pd
from datetime import datetime, timedelta

# Configurações
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/raw')
random.seed(42)  # Para resultados reproduzíveis

# Criar diretórios se não existirem
os.makedirs(os.path.join(DATA_DIR, 'clientes'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'produtos'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'pedidos'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'itens_pedido'), exist_ok=True)

def generate_clientes(n=100):
    """Gera dados de clientes"""
    cidades = ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Porto Alegre', 'Curitiba', 'Salvador', 'Recife', 'Fortaleza', 'Brasília', 'Manaus']
    estados = ['SP', 'RJ', 'MG', 'RS', 'PR', 'BA', 'PE', 'CE', 'DF', 'AM']
    
    data = []
    for i in range(1, n+1):
        data.append({
            'cliente_id': i,
            'nome': f'Cliente {i}',
            'email': f'cliente{i}@email.com',
            'data_nascimento': (datetime.now() - timedelta(days=random.randint(18*365, 80*365))).strftime('%Y-%m-%d'),
            'cidade': random.choice(cidades),
            'estado': random.choice(estados),
            'data_cadastro': (datetime.now() - timedelta(days=random.randint(1, 365*3))).strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(DATA_DIR, 'clientes', 'clientes.csv'), index=False)
    print(f"Arquivo de clientes gerado com {len(df)} registros")

def generate_produtos(n=50):
    """Gera dados de produtos"""
    categorias = ['Eletrônicos', 'Roupas', 'Acessórios', 'Casa', 'Esportes', 'Beleza', 'Alimentos', 'Livros', 'Brinquedos', 'Informática']
    
    data = []
    for i in range(1, n+1):
        preco = round(random.uniform(10, 2000), 2)
        data.append({
            'produto_id': i,
            'nome': f'Produto {i}',
            'categoria': random.choice(categorias),
            'preco': preco,
            'custo': round(preco * random.uniform(0.3, 0.7), 2),
            'quantidade_estoque': random.randint(0, 1000)
        })
    
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(DATA_DIR, 'produtos', 'produtos.csv'), index=False)
    print(f"Arquivo de produtos gerado com {len(df)} registros")

def generate_pedidos(n=1000):
    """Gera dados de pedidos"""
    status = ['Pendente', 'Processando', 'Enviado', 'Entregue', 'Cancelado']
    data = []
    
    for i in range(1, n+1):
        data_pedido = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d %H:%M:%S')
        data_entrega = (datetime.strptime(data_pedido, '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d %H:%M:%S')
        
        data.append({
            'pedido_id': i,
            'cliente_id': random.randint(1, 100),  # Assumindo 100 clientes
            'data_pedido': data_pedido,
            'data_entrega': data_entrega if random.random() > 0.1 else None,  # 10% de chance de não ter data de entrega
            'status': random.choice(status),
            'valor_total': 0,  # Será calculado posteriormente
            'forma_pagamento': random.choice(['Cartão de Crédito', 'Boleto', 'PIX', 'Transferência'])
        })
    
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(DATA_DIR, 'pedidos', 'pedidos.csv'), index=False)
    print(f"Arquivo de pedidos gerado com {len(df)} registros")
    return df

def generate_itens_pedido(pedidos_df, max_itens_por_pedido=5):
    """Gera itens de pedido e atualiza o valor total dos pedidos"""
    data = []
    valor_total_pedidos = {}
    
    for _, pedido in pedidos_df.iterrows():
        pedido_id = pedido['pedido_id']
        num_itens = random.randint(1, max_itens_por_pedido)
        valor_total = 0
        
        for _ in range(num_itens):
            produto_id = random.randint(1, 50)  # Assumindo 50 produtos
            quantidade = random.randint(1, 5)
            preco_unitario = round(random.uniform(10, 2000), 2)
            valor_total_item = round(preco_unitario * quantidade, 2)
            valor_total += valor_total_item
            
            data.append({
                'item_id': len(data) + 1,
                'pedido_id': pedido_id,
                'produto_id': produto_id,
                'quantidade': quantidade,
                'preco_unitario': preco_unitario,
                'valor_total': valor_total_item
            })
        
        valor_total_pedidos[pedido_id] = round(valor_total, 2)
    
    # Atualizar valor total dos pedidos
    pedidos_df = pd.read_csv(os.path.join(DATA_DIR, 'pedidos', 'pedidos.csv'))
    pedidos_df['valor_total'] = pedidos_df['pedido_id'].map(valor_total_pedidos)
    pedidos_df.to_csv(os.path.join(DATA_DIR, 'pedidos', 'pedidos.csv'), index=False)
    
    # Salvar itens de pedido
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(DATA_DIR, 'itens_pedido', 'itens_pedido.csv'), index=False)
    print(f"Arquivo de itens de pedido gerado com {len(df)} registros")

if __name__ == "__main__":
    print("Iniciando geração de dados de exemplo...")
    generate_clientes()
    generate_produtos()
    pedidos_df = generate_pedidos()
    generate_itens_pedido(pedidos_df)
    print("Dados gerados com sucesso!")
