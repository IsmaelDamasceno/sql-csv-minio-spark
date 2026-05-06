# Delta Lake

## O que é Delta Lake?

**Delta Lake** é um formato de tabela open-source desenvolvido pela Databricks que adiciona uma camada de **confiabilidade ACID** sobre arquivos Parquet no data lake. Em vez de um banco de dados centralizado, o Delta Lake usa um **transaction log** baseado em JSON para rastrear todas as operações — permitindo transações atômicas, rollback e viagem no tempo sobre arquivos no sistema de arquivos local, HDFS ou Object Storage (S3/MinIO).

!!! success "Princípios ACID no Delta Lake"
    - **Atomicidade** — uma operação ou completa totalmente ou não acontece
    - **Consistência** — o schema é sempre validado antes de qualquer escrita
    - **Isolamento** — leituras simultâneas nunca veem dados parcialmente escritos
    - **Durabilidade** — cada commit é persistido no `_delta_log` antes de ser confirmado

---

## Arquitetura: O Transaction Log (`_delta_log`)

Toda tabela Delta é um diretório com dois tipos de conteúdo:

```
s3a://bronze/products/
├── _delta_log/
│   ├── 00000000000000000000.json       ← versão 0: INSERT inicial (overwrite)
│   ├── 00000000000000000001.json       ← versão 1: INSERT incremental (append)
│   ├── 00000000000000000002.json       ← versão 2: UPDATE preços 2016
│   ├── 00000000000000000003.json       ← versão 3: DELETE produtos antigos
│   └── 00000000000000000004.json       ← versão 4: MERGE estoques
├── model_year=2016/
│   └── part-00000-....snappy.parquet
├── model_year=2017/
│   └── part-00000-....snappy.parquet
└── model_year=2018/
    └── part-00000-....snappy.parquet
```

Cada arquivo JSON no `_delta_log` registra:

- Quais arquivos Parquet foram **adicionados** (`add`)
- Quais foram **removidos** (`remove`)
- Os **metadados** da operação (tipo, timestamp, métricas)

O **checkpoint** (a cada 10 versões por padrão) consolida o log em Parquet para acelerar a leitura do histórico.

---

## INSERT — Inserção de Dados

### Carga Inicial (overwrite)

```python
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import IntegerType, DoubleType

df_products = (
    spark.read
    .jdbc(url=JDBC_URL, table="products", properties=jdbc_props)
    .withColumn("bronze_ingested_at", current_timestamp())
    .withColumn("extracted_at",       current_timestamp())
    .withColumn("source_system",      lit("postgres"))
    .withColumn("product_id",  col("product_id").cast(IntegerType()))
    .withColumn("list_price",  col("list_price").cast(DoubleType()))
)

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("model_year") \
    .save("s3a://bronze/products")
```

### Inserção Incremental (append)

Novos produtos chegando do catálogo 2019:

```python
from pyspark.sql import Row

new_products = [
    Row(product_id=322, product_name="Trek Checkpoint ALR 4 - 2019",
        brand_id=9, category_id=7, model_year=2019, list_price=1499.99,
        source_system="catalog_2019"),
    Row(product_id=323, product_name="Surly Midnight Special - 2019",
        brand_id=8, category_id=7, model_year=2019, list_price=1699.00,
        source_system="catalog_2019"),
]

df_new = (
    spark.createDataFrame(new_products)
    .withColumn("bronze_ingested_at", current_timestamp())
    .withColumn("extracted_at",       current_timestamp())
    .withColumn("product_id",  col("product_id").cast(IntegerType()))
    .withColumn("list_price",  col("list_price").cast(DoubleType()))
)

df_new.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("model_year") \
    .save("s3a://bronze/products")

total = spark.read.format("delta").load("s3a://bronze/products").count()
print(f"Total após INSERT: {total} produtos")
```

!!! tip "Por que fazer o cast dos tipos?"
    Python infere literais inteiros como `LongType` (64-bit), mas o PostgreSQL retorna `IntegerType` (32-bit). O Delta rejeita a diferença de tipo sem cast explícito.

---

## UPDATE — Atualização de Registros

O Delta Lake oferece UPDATE via **DeltaTable API** usando expressões Column do PySpark.

### Via DeltaTable API

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.functions import round as spark_round

delta_products = DeltaTable.forPath(spark, "s3a://bronze/products")

# Desconto de 15% em produtos do model_year 2016
delta_products.update(
    condition=col("model_year") == 2016,
    set={
        "list_price":         spark_round(col("list_price") * 0.85, 2),
        "bronze_ingested_at": current_timestamp(),
    },
)
```

```python
# Pedidos pendentes vencidos → Rejected
delta_orders = DeltaTable.forPath(spark, "s3a://bronze/orders")

delta_orders.update(
    condition=(
        (col("order_status") == 1)
        & (col("required_date").cast("date") < current_date())
        & col("shipped_date").isNull()
    ),
    set={
        "order_status":      lit(3),
        "bronze_ingested_at": current_timestamp(),
    },
)
```

!!! info "Como o UPDATE funciona internamente"
    O Delta Lake **nunca modifica arquivos Parquet existentes**. Um UPDATE gera novos arquivos com as linhas atualizadas e registra os arquivos antigos como `remove` no `_delta_log`. Os dados anteriores continuam acessíveis via Time Travel.

---

## DELETE — Remoção de Registros

### Via DeltaTable API

```python
from pyspark.sql.functions import col, current_date, datediff

antes = delta_products.toDF().count()

# Remove produtos anteriores a 2016
delta_products.delete(condition=col("model_year") < 2016)

depois = delta_products.toDF().count()
print(f"{antes - depois} produto(s) removido(s).")
```

```python
# Remove pedidos rejeitados com mais de 2 anos
delta_orders.delete(
    condition=(
        (col("order_status") == 3)
        & (datediff(current_date(), col("order_date").cast("date")) > 730)
    )
)
```

!!! note "Soft Delete vs Hard Delete"
    `DELETE` no Delta Lake é um **hard delete lógico** — os dados desaparecem da versão atual, mas permanecem nos arquivos Parquet antigos e são acessíveis via Time Travel enquanto não forem fisicamente removidos por `VACUUM`. Para soft delete, prefira um `UPDATE` que mude `order_status` para `3` (Rejected).

---

## MERGE — Sincronização Incremental (UPSERT)

O `MERGE` combina INSERT e UPDATE em uma única operação atômica: atualiza registros existentes e insere novos. Ideal para sincronizar inventário incremental.

```python
from pyspark.sql import Row

stock_updates = [
    # Atualiza estoque existente
    Row(store_id=1, product_id=1,   quantity=30, source_system="wms"),
    Row(store_id=2, product_id=5,   quantity=12, source_system="wms"),
    # Novo par loja/produto
    Row(store_id=1, product_id=322, quantity=5,  source_system="wms"),
    Row(store_id=2, product_id=323, quantity=3,  source_system="wms"),
]

df_updates = (
    spark.createDataFrame(stock_updates)
    .withColumn("bronze_ingested_at", current_timestamp())
    .withColumn("extracted_at",       current_timestamp())
    .withColumn("store_id",   col("store_id").cast(IntegerType()))
    .withColumn("product_id", col("product_id").cast(IntegerType()))
    .withColumn("quantity",   col("quantity").cast(IntegerType()))
)

delta_stocks = DeltaTable.forPath(spark, "s3a://bronze/stocks")

(
    delta_stocks.alias("target")
    .merge(
        df_updates.alias("source"),
        (col("target.store_id")   == col("source.store_id"))    # (1)
        & (col("target.product_id") == col("source.product_id")),
    )
    .whenMatchedUpdate(set={
        "quantity":           col("source.quantity"),
        "bronze_ingested_at": current_timestamp(),
    })
    .whenNotMatchedInsertAll()
    .execute()
)
```

1. A condição de JOIN define o que torna um registro único — aqui a chave composta `(store_id, product_id)`.

---

## Time Travel — Viagem no Tempo

O Delta Lake mantém o histórico completo de versões. Você pode inspecionar o histórico e consultar qualquer versão anterior.

### Histórico de operações

```python
delta_products.history().select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)
```

```
+-------+-------------------+-----------+-------------------------------+
|version|timestamp          |operation  |operationMetrics               |
+-------+-------------------+-----------+-------------------------------+
|0      |2026-05-06 10:00:00|WRITE      |{numFiles->3, numRows->321}    |
|1      |2026-05-06 10:01:00|WRITE      |{numFiles->1, numRows->2}      |
|2      |2026-05-06 10:02:00|UPDATE     |{numUpdatedRows->56}           |
|3      |2026-05-06 10:03:00|DELETE     |{numDeletedRows->10}           |
+-------+-------------------+-----------+-------------------------------+
```

### Consulta por versão

```python
# Snapshot inicial (versão 0) — dados originais vindos do landing-zone
df_v0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load("s3a://bronze/products")
)
print(f"Versão 0: {df_v0.count()} produtos")
df_v0.select("product_id", "product_name", "model_year", "list_price").show(5)
```

### Consulta por timestamp

```python
df_ontem = (
    spark.read
    .format("delta")
    .option("timestampAsOf", "2026-05-05")
    .load("s3a://bronze/products")
)
```

### Limpeza física com VACUUM

```python
from delta.tables import DeltaTable

# Remove arquivos com mais de 7 dias (padrão)
delta_products.vacuum()

# Reduz retenção para 1 hora
delta_products.vacuum(retentionHours=1)
```

!!! danger "VACUUM e Time Travel"
    Após `VACUUM`, as versões cujos arquivos foram removidos **não podem mais ser acessadas**. Em produção, alinhe o período de retenção com suas necessidades de auditoria antes de executar.
