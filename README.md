# Pipeline de Migração de Dados de RH — Delta Lake & Apache Iceberg

[![Lint & Tests](https://img.shields.io/github/actions/workflow/status/jlsilva01/projeto-ed-satc/ci.yml?branch=main)](https://github.com/jlsilva01/projeto-ed-satc/actions)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen.svg)](https://github.com/jlsilva01/projeto-ed-satc)
[![Docker Pulls](https://img.shields.io/docker/pulls/jlsilva01/projeto-ed-satc)](https://hub.docker.com/r/jlsilva01/projeto-ed-satc)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://jlsilva01.github.io/projeto-ed-satc/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Projeto da disciplina de **Engenharia de Dados** do curso de Engenharia de Software da **UNISATC**. Implementa uma pipeline completa de migração e modernização de dados de RH da empresa fictícia **TechCorp**, utilizando **PySpark** com os formatos de tabela abertos **Delta Lake** e **Apache Iceberg**.

A pipeline cobre as operações fundamentais de qualquer plataforma de dados moderna: **INSERT**, **UPDATE**, **DELETE** e **MERGE (UPSERT)**, além de **Time Travel** e **Schema Evolution**.

---

## Desenho de Arquitetura

```
raw/ (CSVs legado)
  └── funcionarios.csv
  └── departamentos.csv
  └── cargos.csv
  └── folha_pagamento.csv
        │
        ▼
  PySpark DataFrame API
  (leitura + transformação + metadados de auditoria)
        │
        ├──► Delta Lake  (warehouse/delta/)
        │       └── INSERT / UPDATE / DELETE / MERGE / Time Travel
        │
        └──► Apache Iceberg  (/tmp/iceberg/warehouse)
                └── INSERT / UPDATE / DELETE / MERGE INTO / Snapshots / Schema Evolution
```

---

## Fonte de Dados

Quatro arquivos CSV exportados do sistema legado de RH:

| Arquivo | Registros | Descrição |
|---|---|---|
| `funcionarios.csv` | 15 | Cadastro completo de colaboradores |
| `departamentos.csv` | 4 | Estrutura organizacional |
| `cargos.csv` | 7 | Plano de cargos e salários |
| `folha_pagamento.csv` | 26 | Folha de Jan e Fev/2024 |

---

## Pré-requisitos e ferramentas utilizadas

- **Linguagem:** Python 3.11+
- **Processamento distribuído:** PySpark 3.5.0
- **Formatos de tabela:** Delta Lake 3.1.0 · Apache Iceberg 0.7+ (via `pyiceberg[pyarrow,pandas]`)
- **Gerenciador de pacotes:** [uv](https://github.com/astral-sh/uv)
- **Documentação:** MkDocs + mkdocs-material

---

## Instalação

### 1. Clonar o repositório

```bash
git clone https://github.com/jlsilva01/projeto-ed-satc.git
cd projeto-ed-satc
```

## Pré-requisitos

### Java (JDK 17+)

O PySpark exige o Java Development Kit instalado e configurado no `PATH`.

**Linux (Ubuntu/Debian):**
```bash
sudo apt update && sudo apt install -y openjdk-17-jdk
java -version
```

**Windows:**
Baixe o instalador em [adoptium.net](https://adoptium.net/) e siga o assistente. Após a instalação, confirme:
```powershell
java -version
```

---

### uv

`uv` é o gerenciador de pacotes e ambientes virtuais utilizado no projeto.

**Linux/macOS:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.local/bin/env   # ou reinicie o terminal
uv --version
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
uv --version
```

### Instalar dependências

```bash
uv venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

uv sync
```

---

## Como executar

### Pipeline Delta Lake

```bash
uv run jupyter notebook notebooks/pipeline_delta_lake.ipynb
```

Ou execute como script Python:

```bash
uv run python notebooks/pipeline_delta_lake.py
```

### Pipeline Apache Iceberg

```bash
uv run jupyter notebook notebooks/pipeline_iceberg.ipynb
```

As tabelas Delta são gravadas em `warehouse/delta/` e as tabelas Iceberg em `/tmp/iceberg/warehouse`.

---

## Estrutura do Projeto

```
projeto-ed-satc/
├── raw/                        # CSVs de origem (dados fictícios)
│   ├── funcionarios.csv
│   ├── departamentos.csv
│   ├── cargos.csv
│   └── folha_pagamento.csv
├── notebooks/
│   ├── pipeline_delta_lake.ipynb   # Pipeline completa com Delta Lake
│   └── pipeline_iceberg.ipynb      # Pipeline completa com Apache Iceberg
├── warehouse/
│   └── delta/                  # Tabelas Delta Lake (geradas na execução)
├── docs/                       # Documentação MkDocs
├── pyproject.toml
└── README.md
```

---

## Operações implementadas

Ambas as pipelines implementam o mesmo conjunto de operações sobre as tabelas de RH:

| Operação | Delta Lake | Apache Iceberg |
|---|---|---|
| **INSERT** (carga inicial) | `write.format('delta').mode('overwrite')` | `writeTo(...).append()` |
| **INSERT** (incremental) | `write.format('delta').mode('append')` | `INSERT INTO ... VALUES` |
| **UPDATE** | `DeltaTable.update()` · `UPDATE` SQL | `UPDATE` SQL |
| **DELETE** | `DeltaTable.delete()` · `DELETE` SQL | `DELETE FROM` SQL |
| **MERGE (UPSERT)** | `DeltaTable.merge(...).whenMatched...whenNotMatched` | `MERGE INTO` SQL |
| **Time Travel** | `.option('versionAsOf', N)` | `.option('snapshot-id', id)` |
| **Histórico** | `DeltaTable.history()` | `funcionarios.snapshots` |
| **Schema Evolution** | — | `ALTER TABLE ... ADD COLUMN` |

---

## Documentação (MkDocs)

A documentação completa está em `docs/` e cobre a contextualização do projeto, a arquitetura, o modelo de dados e os detalhes de cada operação para Delta Lake e Iceberg.

```bash
# Servir localmente
uv run mkdocs serve
# Acesse: http://127.0.0.1:8000

# Build estático
uv run mkdocs build

# Publicar no GitHub Pages
uv run mkdocs gh-deploy
```

## Colaboração

1. Abra uma **issue** para discutir sua feature ou correção.
2. Crie um branch:

   ```bash
   git checkout -b feature/nome-da-feature
   ```

3. Faça suas alterações e commit seguindo o [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
4. Envie um **pull request** para `main` e aguarde revisão.

---

## Referências

- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [uv — Python package manager](https://github.com/astral-sh/uv)
- [MkDocs Material Theme](https://squidfunk.github.io/mkdocs-material/)