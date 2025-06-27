# 📊 Projeto 4 — ETL com Bronze, Silver, Gold e DuckDB

Este projeto faz ETL de dados de gastos de deputados, cria camadas Bronze, Silver, Gold e realiza análises com DuckDB e visualizações com Python.

---

## ✅ **Estrutura do Projeto**

projeto4_etl/
- ├── data/
- │   ├── bronze/                        # Dados brutos
- │   ├── silver/                        # Dados tratados
- │   └── gold/                          # Dados agregados
- ├── etl.py                             # Script ETL completo
- ├── consultas_duckdb.py                # Consultas analíticas
- ├── visualizacao.ipynb                 # Notebook para gráficos
- ├── requirements.txt                   # Dependências do projeto
- ├── README.md                          # Explicação do projeto
- └── .env                               # Token da API 

---

## ✅ **Objetivo**
- Coletar dados da API [Brasil.IO](https://brasil.io/dataset/gastos-deputados/cota_parlamentar/)
- Criar as camadas:
  - Bronze: dados brutos
  - Silver: dados limpos e normalizados
  - Gold: dados agregados para análise
- Fazer consultas SQL com DuckDB
- Criar gráficos e insights no notebook

---

## 🛠️ **Tecnologias usadas**
- Python 3.x
- pandas
- requests
- duckdb
- python-dotenv
- matplotlib & seaborn

---

## 📦 **Como executar (resumido)**
1. Clone o projeto:
```bash
git clone https://github.com/Broninho/projeto4_etl.git
cd projeto4_etl

```
2. Instale as dependências:
``` bash
pip install -r requirements.txt

```
3. Configure o token da API Brasil.IO:
- Crie um arquivo .env na raiz do projeto: 
``` ini
BRASIL_IO_TOKEN=seu_token_aqui

```

4. Execute o ETL para gerar Bronze, Silver e Gold:
``` bash
python etl.py

```
5. Faça consultas SQL usando DuckDB: 
``` bash
python consultas_duckdb.py

```

6. Visualize gráficos e dashboards:
``` bash
jupyter notebook visualizacao.ipynb

```
---

📦 O que cada script faz

Script|Função
-------------------------------------------------------------------------------------------
etl.py -> Extrai dados da API, trata e salva nas camadas Bronze, Silver e Gold

consultas_duckdb.py	-> Executa consultas SQL diretamente sobre arquivos Parquet

visualizacao.ipynb	-> Gera gráficos e análises visuais

---

🧪 Consultas implementadas

-✅ Top 10 parlamentares que mais gastaram
-✅ Categorias com maiores gastos por partido
-✅ Evolução mensal dos gastos
-✅ Top 10 fornecedores que mais receberam
-✅ Comparativo de gastos entre partidos

---

📊 Exemplos de visualizações

-Evolução mensal dos gastos parlamentares (gráfico de linha)
-Top 10 parlamentares que mais gastaram (gráfico de barras)
-Top fornecedores por valor recebido
-Distribuição de gastos por tipo de despesa
