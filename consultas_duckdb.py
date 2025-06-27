import duckdb
import pandas as pd
from pathlib import Path

# Diretórios base e arquivos
BASE_DIR = Path(__file__).resolve().parent
SILVER_PATH = BASE_DIR / "data" / "silver" / "gastos_deputados_silver.parquet"
GOLD_PATH1 = BASE_DIR / "data" / "gold" / "gastos_por_parlamentar.parquet"
GOLD_PATH2 = BASE_DIR / "data" / "gold" / "gastos_por_categoria_partido.parquet"
GOLD_PATH3 = BASE_DIR / "data" / "gold" / "top_fornecedores_partido.parquet"

def verificar_arquivos():
    """Verifica se todos os arquivos Parquet existem"""
    paths = [SILVER_PATH, GOLD_PATH1, GOLD_PATH2, GOLD_PATH3]
    faltantes = [p for p in paths if not p.exists()]
    if faltantes:
        for p in faltantes:
            print(f"Aviso: Arquivo não encontrado -> {p}")
        return False
    return True

def executar_consulta(titulo, query, conn):
    """Executa e imprime uma consulta com título formatado"""
    print(f"\n{titulo}")
    print("-" * len(titulo))
    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            print("Nenhum dado encontrado.")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"Erro na consulta: {str(e)}")

def run_queries():
    print("\nConsultas analíticas sobre os gastos dos deputados")
    print("=" * 80)

    if not verificar_arquivos():
        print("\nErro: Arquivos necessários não encontrados.")
        print("Execute o script ETL para gerar os dados.")
        return

    conn = duckdb.connect()

    # Consulta 1: Top 10 parlamentares com maiores gastos
    executar_consulta("1. Top 10 parlamentares com maiores gastos", f"""
        SELECT 
            nome_parlamentar,
            sigla_partido,
            sigla_uf,
            SUM(valor_documento) AS total_gasto,
            COUNT(*) AS quantidade_documentos
        FROM '{SILVER_PATH}'
        GROUP BY nome_parlamentar, sigla_partido, sigla_uf
        ORDER BY total_gasto DESC
        LIMIT 10;
    """, conn)

    # Consulta 2: Categorias com maiores gastos por partido
    executar_consulta("2. Categorias com maiores gastos por partido", f"""
        SELECT 
            sigla_partido,
            tipo_despesa,
            SUM(valor_documento) AS total_gasto,
            ROUND(SUM(valor_documento) * 100.0 / SUM(SUM(valor_documento)) OVER (PARTITION BY sigla_partido), 2) AS percentual_partido
        FROM '{SILVER_PATH}'
        GROUP BY sigla_partido, tipo_despesa
        ORDER BY sigla_partido, total_gasto DESC;
    """, conn)

    # Consulta 3: Evolução mensal de gastos
    executar_consulta("3. Evolução mensal de gastos", f"""
        SELECT 
            ano,
            mes,
            SUM(valor_documento) AS total_gasto,
            COUNT(DISTINCT nome_parlamentar) AS deputados_ativos,
            ROUND(SUM(valor_documento) / COUNT(DISTINCT nome_parlamentar), 2) AS media_por_deputado
        FROM '{SILVER_PATH}'
        GROUP BY ano, mes
        ORDER BY ano, mes;
    """, conn)

    # Consulta 4: Top 10 fornecedores que mais receberam
    executar_consulta("4. Top 10 fornecedores que mais receberam", f"""
        SELECT 
            nome_fornecedor,
            SUM(valor_documento) AS total_recebido,
            COUNT(DISTINCT nome_parlamentar) AS deputados_atendidos
        FROM '{SILVER_PATH}'
        GROUP BY nome_fornecedor
        ORDER BY total_recebido DESC
        LIMIT 10;
    """, conn)

    # Consulta 5: Comparativo de gastos entre partidos
    executar_consulta("5. Comparativo de gastos entre partidos", f"""
        SELECT 
            sigla_partido,
            SUM(valor_documento) AS total_gasto,
            COUNT(DISTINCT nome_parlamentar) AS qtd_deputados,
            ROUND(SUM(valor_documento) / COUNT(DISTINCT nome_parlamentar), 2) AS media_por_deputado
        FROM '{GOLD_PATH1}'
        GROUP BY sigla_partido
        ORDER BY total_gasto DESC;
    """, conn)

    print("\n" + "=" * 80)
    conn.close()

if __name__ == "__main__":
    run_queries()
