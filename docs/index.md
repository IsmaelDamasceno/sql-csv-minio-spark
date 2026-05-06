---
hide:
  - toc
---

# Contextualização do Projeto

## Cenário

A **BikeStores** é uma rede fictícia de lojas de bicicletas com três unidades físicas nos Estados Unidos. O objetivo deste projeto é construir uma **pipeline de dados moderna** que extrai os dados transacionais do banco PostgreSQL, os armazena em um Object Storage (MinIO) e os transforma em tabelas Delta Lake prontas para análise.

A pipeline implementa as operações fundamentais de qualquer plataforma de dados lakehouse:

<div class="grid cards" markdown>

- :material-table-plus: **INSERT** — Carga inicial e incremental de produtos e estoques
- :material-table-edit: **UPDATE** — Ajuste de preços e atualização de status de pedidos
- :material-table-remove: **DELETE** — Remoção de registros obsoletos
- :material-table-sync: **MERGE (UPSERT)** — Sincronização incremental de inventário via WMS

</div>

---

## Arquitetura da Pipeline

```mermaid
flowchart LR
    A[("PostgreSQL\nBikeStores")] -->|JDBC| B["PySpark\nNotebook 01"]
    B -->|CSV| C[("MinIO\nlanding-zone/")]
    C -->|CSV| D["PySpark\nNotebook 02"]
    D -->|Delta Lake| E[("MinIO\nbronze/")]
    E --> F["PySpark\nNotebook 03\nDML"]
```

| Etapa | Notebook | Descrição |
|-------|----------|-----------|
| **Extração** | `01_extract_to_landing.ipynb` | Lê as 9 tabelas via JDBC e grava CSV no bucket `landing-zone` |
| **Conversão** | `02_landing_to_bronze.ipynb` | Lê os CSVs e converte para Delta Lake no bucket `bronze` |
| **DML** | `03_bronze_dml.ipynb` | INSERT, UPDATE, DELETE, MERGE e Time Travel no bronze |

---

## Infraestrutura

```mermaid
flowchart TB
    subgraph Docker Compose
        PG["PostgreSQL 16\nlocalhost:5432\nDB: rh"]
        MN["MinIO\nAPI: localhost:9010\nConsole: localhost:9011"]
        MI["minio-init\nCria buckets na inicialização"]
    end
    MI --> MN
    PG & MN --> NB["Jupyter Notebooks\n(PySpark)"]
```

O PostgreSQL executa `bikestores.sql` automaticamente na **primeira inicialização do volume**, criando e populando todas as 9 tabelas.

!!! warning "Reinicializando o banco"
    O init script só roda quando o volume `postgres_data` está vazio. Para recriar o banco do zero:
    ```bash
    docker compose down -v   # apaga os volumes
    docker compose up -d     # sobe novamente e executa o SQL
    ```

**Credenciais:**

| Serviço | Usuário | Senha | Database |
|---------|---------|-------|----------|
| PostgreSQL | `engdados` | `engdados` | `rh` |
| MinIO | `minioadmin` | `minioadmin` | — |

---

## Fonte de Dados

O dataset **BikeStores** é composto por 9 tabelas que modelam o sistema de pedidos de uma rede de lojas de bicicletas.

| Tabela | Descrição |
|--------|-----------|
| `brands` | Marcas de bicicletas
| `categories` | Categorias de produtos
| `customers` | Cadastro de clientes
| `stores` | Lojas físicas
| `staffs` | Funcionários
| `products` | Catálogo de produtos
| `stocks` | Estoque por loja/produto
| `orders` | Pedidos de venda
| `order_items` | Itens de cada pedido

---

## Modelo Entidade-Relacionamento

```mermaid
erDiagram
    BRANDS {
        int brand_id PK
        string brand_name
    }
    CATEGORIES {
        int category_id PK
        string category_name
    }
    PRODUCTS {
        int product_id PK
        string product_name
        int brand_id FK
        int category_id FK
        int model_year
        double list_price
    }
    STORES {
        int store_id PK
        string store_name
        string phone
        string email
        string city
        string state
    }
    STAFFS {
        int staff_id PK
        string first_name
        string last_name
        int store_id FK
        int manager_id FK
    }
    CUSTOMERS {
        int customer_id PK
        string first_name
        string last_name
        string email
        string city
        string state
    }
    ORDERS {
        int order_id PK
        int customer_id FK
        int order_status
        date order_date
        date required_date
        date shipped_date
        int store_id FK
        int staff_id FK
    }
    ORDER_ITEMS {
        int order_id FK
        int item_id
        int product_id FK
        int quantity
        double list_price
        double discount
    }
    STOCKS {
        int store_id FK
        int product_id FK
        int quantity
    }

    BRANDS ||--o{ PRODUCTS : "fabrica"
    CATEGORIES ||--o{ PRODUCTS : "classifica"
    STORES ||--o{ STAFFS : "emprega"
    STORES ||--o{ ORDERS : "processa"
    STORES ||--o{ STOCKS : "armazena"
    STAFFS ||--o{ ORDERS : "atende"
    CUSTOMERS ||--o{ ORDERS : "realiza"
    ORDERS ||--o{ ORDER_ITEMS : "contém"
    PRODUCTS ||--o{ ORDER_ITEMS : "incluso em"
    PRODUCTS ||--o{ STOCKS : "estocado em"
```

!!! info "`order_status`"
    Os valores possíveis são: `1` = Pending, `2` = Processing, `3` = Rejected, `4` = Completed.
