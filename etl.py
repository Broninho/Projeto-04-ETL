import os
import requests
import pandas as pd
import duckdb
from datetime import datetime
from dotenv import load_dotenv
import time

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração de diretórios
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)

def get_api_token():
    """Obtém o token da API de variáveis de ambiente ou arquivo .env"""
    token = os.getenv("BRASIL_IO_TOKEN")
    if not token:
        raise ValueError(
            "Token da API Brasil.IO não encontrado.\n"
            "Por favor, crie um arquivo .env na raiz do projeto com:\n"
            "BRASIL_IO_TOKEN=Token seu_token_aqui\n\n"
            "Você pode obter um token em: https://brasil.io/auth/tokens/"
        )
    if not token.startswith("Token "):
        token = f"Token {token}"
    return token

def download_data():
    """Baixa os dados da API Brasil.IO com autenticação"""
    token = get_api_token()
    base_url = "https://api.brasil.io/v1"
    dataset = "gastos-deputados"
    table_name = "cota_parlamentar"
    url = f"{base_url}/dataset/{dataset}/{table_name}/data/"
    
    headers = {
        "Authorization": token,
        "User-Agent": "Python-ETL-Deputados/1.0"
    }
    
    print("Iniciando download dos dados da API Brasil.IO...")
    all_data = []
    next_page = url
    page_count = 0
    
    try:
        while next_page and page_count < 5:  # Limite de páginas para teste
            print(f"Baixando página {page_count + 1}...")
            response = requests.get(next_page, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data['results'])
            next_page = data.get('next')
            page_count += 1
            if next_page:
                time.sleep(1)
    except requests.exceptions.HTTPError as http_err:
        print(f"\nErro HTTP {http_err.response.status_code} ao acessar a API:")
        print(f"URL: {http_err.request.url}")
        print(f"Resposta: {http_err.response.text[:200]}...")
        raise
    except Exception as err:
        print(f"\nErro inesperado ao acessar API: {str(err)}")
        raise

    if not all_data:
        raise ValueError("Nenhum dado foi baixado da API. Verifique seu token e conexão.")
    
    print(f"\nDownload concluído! {len(all_data)} registros baixados.")
    df = pd.DataFrame(all_data)

    # Renomear colunas da API para os nomes esperados no pipeline
    df.rename(columns={
        "numano": "ano",
        "nummes": "mes",
        "txnomeparlamentar": "nome_parlamentar",
        "vlrdocumento": "valor_documento",
        "sgpartido": "sigla_partido",
        "sguf": "sigla_uf",
        "txtfornecedor": "nome_fornecedor",
        "txtcnpjcpf": "cnpj_cpf",
        "txtdescricao": "tipo_despesa",
        "datemissao": "data_emissao",
        "txtnumero": "documento"
    }, inplace=True)

    required_columns = ['ano', 'mes', 'nome_parlamentar', 'valor_documento']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas essenciais faltando nos dados: {missing_columns}")
    
    bronze_path = os.path.join(BRONZE_DIR, "gastos_deputados_raw.parquet")
    df.to_parquet(bronze_path, index=False)
    print(f"Dados brutos salvos em {bronze_path}")
    
    return df

def transform_to_silver(bronze_df):
    """Transforma os dados para a camada Silver"""
    print("\nIniciando transformação para Silver...")
    
    silver_df = bronze_df.copy()
    
    expected_columns = [
        'ano', 'mes', 'documento', 'tipo_despesa', 'data_emissao',
        'nome_parlamentar', 'sigla_partido', 'sigla_uf', 'valor_documento',
        'nome_fornecedor', 'cnpj_cpf'
    ]
    
    for col in expected_columns:
        if col not in silver_df.columns:
            print(f"Aviso: Coluna '{col}' não encontrada - será criada com valores nulos")
            silver_df[col] = None
    
    silver_df = silver_df[expected_columns]
    
    silver_df['data_emissao'] = pd.to_datetime(silver_df['data_emissao'], errors='coerce')
    silver_df['valor_documento'] = pd.to_numeric(silver_df['valor_documento'], errors='coerce')

    for col in ['ano', 'mes']:
        try:
            silver_df[col] = pd.to_numeric(silver_df[col], errors='coerce').astype('Int64')
        except Exception as e:
            print(f"Erro ao converter coluna {col}: {str(e)}")
            silver_df[col] = None
    
    string_cols = ['nome_parlamentar', 'sigla_partido', 'sigla_uf', 'nome_fornecedor', 'tipo_despesa']
    for col in string_cols:
        if col in silver_df.columns:
            silver_df[col] = silver_df[col].astype(str).str.strip().str.upper()
    
    silver_df['cnpj_cpf'] = silver_df['cnpj_cpf'].fillna('NAO_INFORMADO')
    silver_df['nome_fornecedor'] = silver_df['nome_fornecedor'].fillna('NAO_INFORMADO')

    # Remover linhas com valores críticos nulos (sem excluir por data_emissao)
    required_cols = ['valor_documento', 'nome_parlamentar']
    silver_df = silver_df.dropna(subset=[col for col in required_cols if col in silver_df.columns])
    
    silver_path = os.path.join(SILVER_DIR, "gastos_deputados_silver.parquet")
    silver_df.to_parquet(silver_path, index=False)
    print(f"Dados tratados salvos em {silver_path}")
    
    return silver_df

def create_gold_layer(silver_df):
    """Cria as tabelas analíticas na camada Gold"""
    print("\nCriando camada Gold...")
    
    if silver_df.empty:
        raise ValueError("DataFrame vazio - não é possível criar camada Gold")
    
    gastos_por_parlamentar = silver_df.groupby(
        ['ano', 'mes', 'nome_parlamentar', 'sigla_partido', 'sigla_uf']
    )['valor_documento'].sum().reset_index()
    
    gold_path1 = os.path.join(GOLD_DIR, "gastos_por_parlamentar.parquet")
    gastos_por_parlamentar.to_parquet(gold_path1, index=False)
    print(f"Tabela Gold 1 salva em {gold_path1}")
    
    gastos_por_categoria_partido = silver_df.groupby(
        ['ano', 'tipo_despesa', 'sigla_partido']
    )['valor_documento'].sum().reset_index()
    
    gold_path2 = os.path.join(GOLD_DIR, "gastos_por_categoria_partido.parquet")
    gastos_por_categoria_partido.to_parquet(gold_path2, index=False)
    print(f"Tabela Gold 2 salva em {gold_path2}")
    
    top_fornecedores_partido = silver_df.groupby(
        ['sigla_partido', 'nome_fornecedor']
    )['valor_documento'].sum().reset_index()
    top_fornecedores_partido = top_fornecedores_partido.sort_values(
        ['sigla_partido', 'valor_documento'], ascending=[True, False]
    )
    
    gold_path3 = os.path.join(GOLD_DIR, "top_fornecedores_partido.parquet")
    top_fornecedores_partido.to_parquet(gold_path3, index=False)
    print(f"Tabela Gold 3 salva em {gold_path3}")

def main():
    try:
        print("Iniciando pipeline ETL...")
        bronze_df = download_data()
        silver_df = transform_to_silver(bronze_df)
        create_gold_layer(silver_df)
        print("\nPipeline ETL concluído com sucesso!")
    except Exception as e:
        print(f"\nErro durante a execução do pipeline: {str(e)}")
        print("Verifique:")
        print("- Seu token de API no arquivo .env (formato: 'Token seu_token')")
        print("- Sua conexão com a internet")
        print("- Os logs acima para detalhes do erro")

if __name__ == "__main__":
    main()
