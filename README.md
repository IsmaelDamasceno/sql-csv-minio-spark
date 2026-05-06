# Projeto Apache Spark + Delta Lake + PostgreSQL + MinIO

Projeto desenvolvido para o curso de Engenharia de Dados com Spark e Delta Lake, lendo dados de um banco PostgreSQL e processando no MinIO em duas camadas: **landing-zone** (CSV) e **bronze** (Delta Lake).

1. **Extração** de tabelas do PostgreSQL via JDBC
2. **Carga** no MinIO (Object Storage compatível com S3) no formato CSV — camada `landing-zone`
3. **Conversão** para Delta Lake — camada `bronze`
4. **Manipulação** dos dados com operações DML (INSERT, UPDATE, DELETE, MERGE) usando a DataFrame API do Spark

## Arquitetura

```
┌─────────────────┐     ┌──────────────────┐     ┌───────────────────┐
│   PostgreSQL    │────▶│   MinIO (S3)     │────▶│   MinIO (S3)      │
│                 │     │   landing-zone/  │     │   bronze/         │
│   BikeStores    │     │   (CSVs)         │     │   (Delta Tables)  │
│   9 tabelas     │     │                  │     │                   │
│                 │     │   1 CSV/tabela   │     │   INSERT/UPDATE   │
│                 │     │                  │     │   DELETE/MERGE    │
└─────────────────┘     └──────────────────┘     └───────────────────┘
                             Notebook 01              Notebooks 02/03
                             (Extração)               (Delta + DML)
```

## Pré-requisitos

- **Linux** (Ubuntu 24.04 ou WSL do Windows 11)
- **Docker** e **Docker Compose** v2+
- **Python 3.11** (PySpark 3.5 requer Python ≤ 3.12)
- **Java 11** (OpenJDK)
- **UV** (gerenciador de pacotes Python) — [instalação](https://github.com/astral-sh/uv)

## Setup do Ambiente

### 1. Subir os Containers (PostgreSQL + MinIO)

```bash
docker compose up -d
```

**Containers criados:**

| Container            | Imagem             | Portas         |
|---------------------|--------------------|----------------|
| engdados_postgres   | `postgres:16`      | `5432`         |
| engdados_minio      | `minio/minio`      | `9010`, `9011` |
| engdados_minio_init | `minio/mc`         | —              |

O container `minio_init` cria automaticamente os buckets `landing-zone` e `bronze` na primeira execução.
O PostgreSQL executa `bikestores.sql` automaticamente na primeira inicialização do volume, criando e populando todas as 9 tabelas.

**Credenciais:**

| Serviço    | Usuário      | Senha        | Database |
|------------|--------------|--------------|----------|
| PostgreSQL | `engdados`   | `engdados`   | `rh`     |
| MinIO      | `minioadmin` | `minioadmin` | —        |

**URLs:**

| Serviço         | URL                    |
|-----------------|------------------------|
| PostgreSQL      | `localhost:5432`       |
| MinIO API (S3)  | `http://localhost:9010`|
| MinIO Console   | `http://localhost:9011`|

### 2. Configurar o Ambiente Python

```bash
uv venv
source .venv/bin/activate
uv sync
uv sync --group docs
```


### 3. Gerando Docs (MkDocs)

A documentação completa está em `docs/` e cobre a contextualização do projeto, a arquitetura, o modelo de dados e os detalhes de cada operação para Delta Lake e Iceberg.

```bash
# Build estático
uv run mkdocs build

# Servir localmente
uv run mkdocs serve
# Acesse: http://127.0.0.1:8000

# Publicar no GitHub Pages
uv run mkdocs gh-deploy
```

## Executando o Projeto

Execute os notebooks **em ordem**:

| # | Notebook | Descrição |
|---|----------|-----------|
| 1 | `01_extract_to_landing.ipynb` | Lê as 9 tabelas do PostgreSQL via JDBC e grava CSV no MinIO (`landing-zone`) |
| 2 | `02_landing_to_bronze.ipynb` | Lê os CSVs do `landing-zone` e converte para Delta Lake no bucket `bronze` |
| 3 | `03_bronze_dml.ipynb` | Executa DML (INSERT, UPDATE, DELETE, MERGE), Time Travel e histórico de versões |

> **Importante:** Selecione o ambiente virtual (`.venv`) como Kernel do Jupyter antes de executar.

## Estrutura do Projeto

```
eng-de-dados/
├── docker-compose.yml              # PostgreSQL 16 + MinIO
├── bikestores.sql                  # DDL + DML — recria o banco do zero
├── pyproject.toml                  # Dependências Python (UV)
├── .python-version                 # Python 3.11
├── raw/                            # CSVs de referência (BikeStores)
│   ├── brands.csv
│   ├── categories.csv
│   ├── customers.csv
│   ├── stores.csv
│   ├── staffs.csv
│   ├── products.csv
│   ├── stocks.csv
│   ├── orders.csv
│   └── order_items.csv
├── notebooks/
│   ├── 01_extract_to_landing.ipynb # Extração: PostgreSQL → MinIO (CSV)
│   ├── 02_landing_to_bronze.ipynb  # Conversão: CSV → Delta Lake
│   └── 03_bronze_dml.ipynb         # DML: INSERT, UPDATE, DELETE, MERGE
└── README.md
```

## Domínio dos Dados

O dataset utilizado é o **BikeStores** — sistema fictício de lojas de bicicletas com as seguintes tabelas:

| Tabela        | Descrição                                 | Partição no bronze |
|---------------|-------------------------------------------|--------------------|
| `brands`      | Marcas de bicicletas                      | —                  |
| `categories`  | Categorias de produtos                    | —                  |
| `customers`   | Clientes                                  | `state`            |
| `stores`      | Lojas físicas                             | —                  |
| `staffs`      | Funcionários                              | —                  |
| `products`    | Catálogo de produtos                      | `model_year`       |
| `stocks`      | Estoque por loja e produto                | —                  |
| `orders`      | Pedidos de venda                          | `order_status`     |
| `order_items` | Itens de cada pedido                      | —                  |

## Tecnologias Utilizadas

- **Apache Spark 3.5.0** (PySpark) — Motor de processamento distribuído
- **Delta Lake 3.1.0** — Formato de armazenamento com suporte ACID e Time Travel
- **MinIO** — Object Storage compatível com S3
- **PostgreSQL 16** — Banco de dados relacional
- **Docker Compose** — Orquestração de containers
- **Python 3.11** com UV

## Conceitos Demonstrados

- **Extração de dados** de banco relacional via JDBC
- **Object Storage** como repositório intermediário (MinIO/S3)
- **Arquitetura Medalhão** (Landing Zone → Bronze)
- **Delta Lake** como formato de armazenamento lakehouse
- **Transações ACID** em data lakes
- **DataFrame API** para transformações e DML
- **DML** (INSERT, UPDATE, DELETE, MERGE/UPSERT) em tabelas Delta
- **Versionamento** de dados (History e Time Travel)

## Links e Referências

- [Delta Lake - Documentação](https://docs.delta.io/latest/index.html)
- [Delta Lake - Releases](https://docs.delta.io/latest/releases.html)
- [MinIO - Documentação](https://min.io/docs/minio/linux/index.html)
- [PostgreSQL - Docker Hub](https://hub.docker.com/_/postgres)
- [Database Kaggle](https://www.kaggle.com/datasets/dillonmyrick/bike-store-sample-database/data)
