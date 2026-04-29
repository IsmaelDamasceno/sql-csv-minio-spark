# Delta Lake

## O que é Delta Lake?

**Delta Lake** é um formato de tabela open-source desenvolvido pela Databricks que adiciona uma camada de **confiabilidade ACID** sobre arquivos Parquet no data lake. Em vez de um banco de dados centralizado, o Delta Lake usa um **transaction log** baseado em JSON para rastrear todas as operações — permitindo transações atômicas, rollback e viagem no tempo sobre arquivos no sistema de arquivos local, HDFS ou S3.

!!! success "Princípios ACID no Delta Lake"
    - **Atomicidade** — uma operação ou completa totalmente ou não acontece (sem arquivos corrompidos a meio caminho)
    - **Consistência** — o schema é sempre validado antes de qualquer escrita
    - **Isolamento** — leituras simultâneas nunca veem dados parcialmente escritos
    - **Durabilidade** — cada commit é persistido no `_delta_log` antes de ser confirmado

---

## Arquitetura: O Transaction Log (`_delta_log`)

Toda tabela Delta é um diretório com dois tipos de conteúdo:

```
warehouse/delta/funcionarios/
├── _delta_log/                         ← transaction log
│   ├── 00000000000000000000.json       ← versão 0: INSERT inicial
│   ├── 00000000000000000001.json       ← versão 1: INSERT incremental
│   ├── 00000000000000000002.json       ← versão 2: UPDATE salários
│   ├── ...
│   └── 00000000000000000010.checkpoint.parquet  ← checkpoint a cada 10 commits
├── status=ATIVO/
│   └── part-00000-...snappy.parquet    ← dados reais
└── status=INATIVO/
    └── part-00000-...snappy.parquet
```

Cada arquivo JSON no `_delta_log` registra:
- Quais arquivos Parquet foram **adicionados** (`add`)
- Quais foram **removidos** (`remove`)
- Os **metadados** da operação (tipo, timestamp, métricas)

O **checkpoint** (a cada 10 versões por padrão) consolida o log em Parquet para acelerar a leitura do histórico.

---

## Configuração da SparkSession

```python
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

DATA_PATH  = os.path.abspath('../raw')
DELTA_PATH = os.path.abspath('../warehouse/delta')  # (1)

builder = (
    SparkSession.builder
    .appName('Pipeline_RH_DeltaLake')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.sql.warehouse.dir', os.path.abspath('../warehouse'))
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
```

1. Sempre use `os.path.abspath()` para evitar que o Spark SQL resolva caminhos relativos de forma diferente do Python.

---

## INSERT — Inserção de Dados

### Carga Inicial com Overwrite

A carga inicial usa `mode('overwrite')` para criar a tabela Delta do zero:

```python
from pyspark.sql.functions import to_date, current_timestamp, lit, col
from pyspark.sql.types import DoubleType

# Transforma e enriquece os dados antes de gravar
df_funcionarios_clean = (
    df_funcionarios
    .withColumn('data_nascimento', to_date(col('data_nascimento'), 'yyyy-MM-dd'))
    .withColumn('data_admissao',   to_date(col('data_admissao'),   'yyyy-MM-dd'))
    .withColumn('salario',         col('salario').cast(DoubleType()))
    .withColumn('ingested_at',     current_timestamp())
    .withColumn('source_system',   lit('CSV_LEGADO'))
)

# Grava no formato Delta, particionando por status
df_funcionarios_clean.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('status') \
    .save(f'{DELTA_PATH}/funcionarios')
```

### Registro no Catálogo Spark

Após gravar os arquivos, registra a tabela para uso via Spark SQL:

```python
spark.sql(f"""
    CREATE OR REPLACE TABLE delta_funcionarios
    USING DELTA LOCATION '{DELTA_PATH}/funcionarios'
""")
```

!!! warning "Use `CREATE OR REPLACE`, não `CREATE IF NOT EXISTS`"
    `CREATE TABLE IF NOT EXISTS` mantém um registro antigo no metastore se a tabela já foi criada antes — inclusive com um caminho errado. `CREATE OR REPLACE` garante que o registro aponta para o local correto.

### Inserção Incremental com Append

Novos registros são adicionados com `mode('append')`, preservando os dados existentes:

```python
from pyspark.sql import Row
from datetime import date
from pyspark.sql.types import IntegerType

novos_funcionarios = [
    Row(funcionario_id=16, nome='Renata Gomes',
        email='renata.gomes@techcorp.com', cpf='666.777.888-99',
        data_nascimento=date(1998, 4, 12), data_admissao=date(2024, 3, 1),
        departamento_id=1, cargo_id=1, salario=5000.0,
        status='ATIVO', source_system='SISTEMA_RH_2024'),
    Row(funcionario_id=17, nome='Gustavo Prado',
        email='gustavo.prado@techcorp.com', cpf='777.888.999-00',
        data_nascimento=date(1990, 9, 27), data_admissao=date(2024, 3, 15),
        departamento_id=3, cargo_id=4, salario=10500.0,
        status='ATIVO', source_system='SISTEMA_RH_2024'),
]

df_novos = (
    spark.createDataFrame(novos_funcionarios)
    .withColumn('ingested_at', current_timestamp())
    # Garante compatibilidade de tipo com a tabela Delta existente
    .withColumn('funcionario_id',  col('funcionario_id').cast(IntegerType()))
    .withColumn('departamento_id', col('departamento_id').cast(IntegerType()))
    .withColumn('cargo_id',        col('cargo_id').cast(IntegerType()))
)

df_novos.write \
    .format('delta') \
    .mode('append') \
    .partitionBy('status') \
    .save(f'{DELTA_PATH}/funcionarios')

total = spark.read.format('delta').load(f'{DELTA_PATH}/funcionarios').count()
print(f'Total após INSERT: {total} registros')
# Total após INSERT: 17 registros
```

!!! tip "Por que fazer o cast dos IDs?"
    Python infere literais inteiros como `LongType` (64-bit), mas o CSV original foi lido com `inferSchema=True` que produziu `IntegerType` (32-bit). O Delta rejeita a diferença de tipo sem cast explícito.

---

## UPDATE — Atualização de Registros

O Delta Lake oferece duas formas de UPDATE: a **DeltaTable API** (Python) e o **Spark SQL**.

### Via DeltaTable API (Python)

```python
from delta.tables import DeltaTable

delta_funcionarios = DeltaTable.forPath(spark, f'{DELTA_PATH}/funcionarios')

# Reajuste de 10% para Analistas Sênior (cargo_id = 3) ativos
delta_funcionarios.update(
    condition="cargo_id = 3 AND status = 'ATIVO'",
    set={
        'salario':     'salario * 1.10',
        'ingested_at': 'current_timestamp()'
    }
)

print('✅ UPDATE: Analistas Sênior reajustados em 10%')

# Verifica o resultado
spark.read.format('delta').load(f'{DELTA_PATH}/funcionarios') \
    .filter("cargo_id = 3") \
    .select('nome', 'salario', 'status') \
    .show()
```

```
+---------------+-------+------+
|           nome|salario|status|
+---------------+-------+------+
|Ana Paula Silva| 9350.0| ATIVO|
|  Patrícia Lima| 9020.0| ATIVO|
+---------------+-------+------+
```

### Via Spark SQL

O Spark SQL permite UPDATE com sintaxe familiar a qualquer analista SQL:

```python
spark.sql("""
    UPDATE delta_funcionarios
    SET status      = 'DESLIGADO',
        ingested_at = current_timestamp()
    WHERE status = 'INATIVO'
    AND   datediff(current_date(), data_admissao) > 1825
""")

print('✅ Funcionários inativos há mais de 5 anos marcados como DESLIGADO')
spark.sql("""
    SELECT nome, status, data_admissao
    FROM delta_funcionarios
    WHERE status = 'DESLIGADO'
""").show()
```

!!! info "Como o UPDATE funciona internamente"
    O Delta Lake **nunca modifica arquivos Parquet existentes**. Um UPDATE gera novos arquivos com as linhas atualizadas e registra os arquivos antigos como `remove` no `_delta_log`. Os dados anteriores continuam acessíveis via Time Travel.

---

## DELETE — Remoção de Registros

### Via DeltaTable API

```python
antes = spark.read.format('delta').load(f'{DELTA_PATH}/funcionarios').count()
print(f'Registros antes do DELETE: {antes}')  # 17

delta_funcionarios.delete(condition="status = 'DESLIGADO'")

depois = spark.read.format('delta').load(f'{DELTA_PATH}/funcionarios').count()
print(f'Registros após o DELETE: {depois}')    # 15
print(f'{antes - depois} registro(s) removido(s)!')
```

### Via Spark SQL

```python
# Remove folhas de pagamento com competência anterior a 2024
spark.sql("""
    DELETE FROM delta_folha
    WHERE competencia < '2024-01'
""")
print('✅ Folhas antigas removidas')
```

!!! note "Soft Delete vs Hard Delete"
    `DELETE` no Delta Lake é um **hard delete lógico** — os dados desaparecem da versão atual, mas permanecem acessíveis via Time Travel enquanto não forem fisicamente removidos por `VACUUM`. Para soft delete, prefira um `UPDATE` que mude a coluna `status` para `'DESLIGADO'`.

---

## MERGE — Sincronização Incremental (UPSERT)

O `MERGE` é o comando mais importante em pipelines incrementais. Ele combina INSERT e UPDATE em uma única operação atômica: atualiza registros existentes e insere novos.

```python
from pyspark.sql import Row
from datetime import date

dados_incremental = [
    # Funcionário existente — salário será atualizado
    Row(funcionario_id=1, nome='Ana Paula Silva',
        email='ana.paula@techcorp.com', cpf='123.456.789-01',
        data_nascimento=date(1990, 3, 15), data_admissao=date(2018, 6, 1),
        departamento_id=1, cargo_id=3, salario=9200.0,
        status='ATIVO', source_system='API_RH'),
    # Funcionário novo — será inserido
    Row(funcionario_id=18, nome='Sofia Barros',
        email='sofia.barros@techcorp.com', cpf='888.999.000-11',
        data_nascimento=date(2000, 6, 18), data_admissao=date(2024, 4, 1),
        departamento_id=4, cargo_id=1, salario=4800.0,
        status='ATIVO', source_system='API_RH'),
]

df_incremental = (
    spark.createDataFrame(dados_incremental)
    .withColumn('ingested_at', current_timestamp())
    .withColumn('funcionario_id',  col('funcionario_id').cast(IntegerType()))
    .withColumn('departamento_id', col('departamento_id').cast(IntegerType()))
    .withColumn('cargo_id',        col('cargo_id').cast(IntegerType()))
)

# MERGE: atualiza se o ID já existe, insere se não existe
delta_funcionarios.alias('target').merge(
    df_incremental.alias('source'),
    'target.funcionario_id = source.funcionario_id'  # (1)
).whenMatchedUpdate(set={
    'salario':     'source.salario',
    'email':       'source.email',
    'ingested_at': 'current_timestamp()'
}).whenNotMatchedInsertAll().execute()

print('✅ MERGE executado!')
total = spark.read.format('delta').load(f'{DELTA_PATH}/funcionarios').count()
print(f'Total após MERGE: {total} registros')
# Total após MERGE: 16 registros (15 existentes + 1 novo Sofia Barros)
```

1. A condição de JOIN define qual campo identifica o registro de forma única — normalmente a chave primária da tabela.

---

## Time Travel — Viagem no Tempo

O Delta Lake mantém o histórico completo de versões. Você pode consultar qualquer versão anterior:

```python
# Exibe o histórico de operações
delta_funcionarios.history().select(
    'version', 'timestamp', 'operation', 'operationMetrics'
).show(truncate=False)
```

```
+-------+-------------------+-----------+----------------------------------+
|version|timestamp          |operation  |operationMetrics                  |
+-------+-------------------+-----------+----------------------------------+
|0      |2024-04-29 00:46:26|WRITE      |{numFiles -> 2, numRows -> 15}   |
|1      |2024-04-29 00:46:30|WRITE      |{numFiles -> 1, numRows -> 2}    |
|2      |2024-04-29 00:46:34|UPDATE     |{numUpdatedRows -> 2}             |
|3      |2024-04-29 00:46:37|UPDATE     |{numUpdatedRows -> 2}             |
|4      |2024-04-29 00:46:40|DELETE     |{numDeletedRows -> 2}             |
|5      |2024-04-29 00:46:43|MERGE      |{numTargetRowsInserted -> 1}      |
+-------+-------------------+-----------+----------------------------------+
```

### Consulta por versão

```python
# Lê a versão 0 (dados originais do CSV)
df_original = spark.read \
    .format('delta') \
    .option('versionAsOf', 0) \
    .load(f'{DELTA_PATH}/funcionarios')

print(f'Versão 0: {df_original.count()} registros')
df_original.select('nome', 'salario', 'status').show(5)
```

### Consulta por timestamp

```python
df_ontem = spark.read \
    .format('delta') \
    .option('timestampAsOf', '2024-04-28') \
    .load(f'{DELTA_PATH}/funcionarios')
```

### Limpeza física com VACUUM

```python
# Remove arquivos antigos — padrão: arquivos com mais de 7 dias
spark.sql(f"VACUUM delta.`{DELTA_PATH}/funcionarios`")

# Reduz para 1 hora (cuidado: impossibilita time travel nesse intervalo)
spark.sql(f"VACUUM delta.`{DELTA_PATH}/funcionarios` RETAIN 1 HOURS")
```

!!! danger "VACUUM e Time Travel"
    Após `VACUUM`, as versões cujos arquivos foram removidos **não podem mais ser acessadas**. Em produção, defina um período de retenção alinhado com suas necessidades de auditoria antes de executar o `VACUUM`.
