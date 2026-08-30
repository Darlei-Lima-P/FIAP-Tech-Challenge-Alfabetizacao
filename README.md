# FIAP Tech Challenge — Avaliação da Alfabetização

## 1. Objetivo

Este projeto tem como objetivo estruturar os dados da Avaliação da Alfabetização, disponibilizados pela Base dos Dados, utilizando uma arquitetura de dados em camadas.

A primeira etapa do projeto consiste na ingestão e organização dos dados brutos, preparando-os para as etapas posteriores do pipeline.

---

## 2. Fonte dos dados

Os dados utilizados são provenientes da Base dos Dados, a partir do conjunto:

**Avaliação da Alfabetização**

A tabela de alunos possui 3.867.999 registros, referentes aos anos de 2023 e 2024.

Os dados foram obtidos em formato CSV e armazenados inicialmente na camada `RAW` do projeto.

Os arquivos utilizados são:

- `alunos.csv`
- `br_inep_avaliacao_alfabetizacao_municipio.csv`
- `br_inep_avaliacao_alfabetizacao_uf.csv`
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv`
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv`
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv`

Fonte: Base dos Dados — Avaliação da Alfabetização.

---

## 3. Arquitetura de dados

O projeto utiliza uma organização em camadas seguindo o conceito de arquitetura de dados em modelo medalhão:

```text
Dados de origem
      ↓
     RAW
      ↓
   BRONZE
      ↓
   SILVER
      ↓
    GOLD
```

Nesta primeira etapa do projeto, foram implementadas a ingestão dos dados brutos e a organização da camada **Bronze**.

A arquitetura contempla dois fluxos de ingestão:

- **Batch:** processamento dos arquivos CSV completos disponibilizados pela Base dos Dados.
- **Streaming simulado:** processamento de uma amostra de 500 registros, dividida em 5 lotes de 100 registros, simulando a chegada contínua de eventos.

### 3.1 Ingestão Batch

O fluxo Batch é responsável por carregar os arquivos CSV da camada `RAW` e convertê-los para o formato **Parquet**.

O processamento é realizado pelo script:

```text
src/ingest_bronze.py
```

Durante a ingestão:

- os arquivos CSV são carregados;
- os dados são convertidos para Parquet;
- é utilizada compressão Snappy;
- os dados são particionados por ano;
- os arquivos são organizados na camada Bronze.

Ao final da ingestão Batch foram gerados **13 arquivos Parquet**, totalizando aproximadamente **57,3 MiB**.

A tabela `alunos` contém:

- **1.747.439 registros em 2023**
- **2.120.560 registros em 2024**
- **3.867.999 registros no total**

### 3.2 Ingestão Streaming simulada

Como parte da arquitetura híbrida exigida pelo projeto, também foi implementada uma simulação de ingestão em Streaming.

O fluxo é executado pelo script:

```text
src/streaming_simulado.py
```

Para a simulação foram utilizados **500 registros** da tabela de alunos.

Os registros são processados em:

```text
5 lotes × 100 registros
```

Cada lote é gravado individualmente em formato Parquet, mantendo o particionamento por ano.

A validação da simulação confirmou:

- **10 arquivos Parquet**
- **500 eventos processados**
- **251 registros de 2023**
- **249 registros de 2024**

A estrutura gerada é:

```text
data/
└── bronze/
    └── streaming/
        ├── ano=2023/
        │   ├── lote-001.parquet
        │   ├── lote-002.parquet
        │   ├── lote-003.parquet
        │   ├── lote-004.parquet
        │   └── lote-005.parquet
        │
        └── ano=2024/
            ├── lote-001.parquet
            ├── lote-002.parquet
            ├── lote-003.parquet
            ├── lote-004.parquet
            └── lote-005.parquet
```

### 3.3 Organização da camada Bronze

A camada Bronze Batch possui a seguinte estrutura:

```text
data/
└── bronze/
    ├── alunos/
    │   ├── ano=2023/
    │   │   └── part-000.parquet
    │   └── ano=2024/
    │       └── part-000.parquet
    │
    ├── municipio/
    │   ├── ano=2023/
    │   │   └── part-000.parquet
    │   └── ano=2024/
    │       └── part-000.parquet
    │
    ├── uf/
    │   ├── ano=2023/
    │   │   └── part-000.parquet
    │   └── ano=2024/
    │       └── part-000.parquet
    │
    ├── meta_brasil/
    │   ├── ano=2023/
    │   ├── ano=2024/
    │   └── ano=2025/
    │
    ├── meta_municipio/
    │   ├── ano=2023/
    │   └── ano=2024/
    │
    └── meta_uf/
        ├── ano=2023/
        └── ano=2024/
```

### 3.4 Armazenamento na AWS

Após o processamento local, os dados da camada Bronze foram disponibilizados no **Amazon S3**.

O bucket utilizado é:

```text
fiap-tech-challenge-alfabetizacao-data
```

Região:

```text
sa-east-1
```

A estrutura principal armazenada no S3 é:

```text
s3://fiap-tech-challenge-alfabetizacao-data/
└── bronze/
    ├── alunos/
    ├── municipio/
    ├── uf/
    ├── meta_brasil/
    ├── meta_municipio/
    ├── meta_uf/
    └── streaming/
        ├── ano=2023/
        └── ano=2024/
```

A carga Batch foi validada no S3 com:

```text
13 objetos
57,3 MiB
```

A carga de Streaming simulada foi validada com:

```text
10 objetos
87,8 KiB
```

### 3.5 Diagrama da arquitetura

A arquitetura implementada nesta etapa está representada no diagrama abaixo:

![Arquitetura da Solução](docs/arquitetura-solucao.png)

O diagrama apresenta os fluxos de ingestão Batch e Streaming simulado, a organização da camada Bronze, o armazenamento no Amazon S3 e a evolução planejada para as camadas Silver e Gold.

---

## 4. Estrutura do projeto

```text
FIAP/
├── data/
│   ├── raw/
│   └── bronze/
│
├── docs/
│   └── arquitetura-solucao.png
│
├── src/
│   ├── ingest_bronze.py
│   └── streaming_simulado.py
│
├── .venv/
│
└── README.md
```

---

## 5. Tecnologias utilizadas

- Python
- Pandas
- PyArrow
- Parquet
- Amazon S3
- AWS CLI
- Visual Studio Code
