# FIAP Tech Challenge — Avaliação da Alfabetização

## 1. Objetivo

Este projeto tem como objetivo estruturar os dados da Avaliação da
Alfabetização, disponibilizados pela Base dos Dados, utilizando uma arquitetura
de dados em camadas.

A solução implementa um pipeline híbrido, escalável e confiável para integrar
resultados, metas e microdados educacionais, apoiando análises e políticas
públicas baseadas em evidências.

---

## 2. Fonte dos dados

Os dados utilizados são provenientes da Base dos Dados, a partir do conjunto:

**Avaliação da Alfabetização**

A tabela de alunos possui **3.867.999 registros**, referentes aos anos de 2023
e 2024. Os dados de origem foram obtidos em formato CSV e organizados na camada
`RAW` antes da ingestão no data lake.

### Arquivos utilizados

- `alunos.csv`
- `br_inep_avaliacao_alfabetizacao_municipio.csv`
- `br_inep_avaliacao_alfabetizacao_uf.csv`
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv`
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv`
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv`

Fonte: Base dos Dados — Avaliação da Alfabetização.

---

## 3. Arquitetura de dados

O projeto utiliza uma organização em camadas seguindo o conceito de arquitetura
de dados em modelo medalhão:

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

A arquitetura contempla ingestão Batch e Streaming simulado, armazenamento no
Amazon S3, catálogo no AWS Glue Data Catalog, consultas no Amazon Athena,
observabilidade e aplicações de inteligência artificial.

### 3.1 Visão completa da solução

![Arquitetura completa da solução](docs/arquitetura-completa.png)

```text
Fontes educacionais
        ↓
Ingestão batch + streaming simulado
        ↓
Amazon S3: Bronze → Silver → Gold
        ↓
Catálogo e consultas analíticas
        ↓
Visualizações, monitoramento e aplicações de IA
```

- **Bronze:** preservação dos dados ingeridos em Parquet Snappy, particionados por ano.
- **Silver:** limpeza, tipagem, padronização, integração e validações de qualidade.
- **Gold:** indicadores por município, UF e Brasil, metas, rankings e evolução temporal.
- **Consumo:** consultas SQL, visualizações, dashboards e aplicações de inteligência artificial.

O fluxo batch processa o histórico completo. O streaming simulado representa a
chegada contínua de eventos, com 500 registros organizados em lotes.

### 3.2 Ingestão Batch

O fluxo Batch carrega os arquivos CSV da camada `RAW` e os converte para
**Parquet** por meio do script:

```text
src/ingest_bronze.py
```

Durante a ingestão:

- os arquivos CSV são carregados;
- os dados são convertidos para Parquet;
- é utilizada compressão Snappy;
- os dados são particionados por ano;
- os arquivos são organizados na camada Bronze.

Ao final da ingestão Batch foram gerados **13 arquivos Parquet**, totalizando
aproximadamente **57,3 MiB**.

#### Base de alunos

- **1.747.439 registros em 2023**
- **2.120.560 registros em 2024**
- **3.867.999 registros no total**

### 3.3 Ingestão Streaming simulada

Como parte da arquitetura híbrida, o script `src/streaming_simulado.py`
processa uma amostra de **500 registros** da tabela de alunos em cinco lotes,
simulando a chegada contínua de eventos.

A validação da simulação confirmou:

- **10 arquivos Parquet**;
- **500 eventos processados**;
- **251 registros de 2023**;
- **249 registros de 2024**.

```text
data/bronze/streaming/
├── ano=2023/
│   ├── lote-001.parquet
│   └── ... lote-005.parquet
└── ano=2024/
    ├── lote-001.parquet
    └── ... lote-005.parquet
```

### 3.4 Organização da camada Bronze

```text
data/bronze/
├── alunos/
│   ├── ano=2023/part-000.parquet
│   └── ano=2024/part-000.parquet
├── municipio/
│   ├── ano=2023/part-000.parquet
│   └── ano=2024/part-000.parquet
├── uf/
│   ├── ano=2023/part-000.parquet
│   └── ano=2024/part-000.parquet
├── meta_brasil/
├── meta_municipio/
├── meta_uf/
└── streaming/
    ├── ano=2023/
    └── ano=2024/
```

A camada Bronze contém **23 objetos**, sendo 13 do processamento Batch e 10 do
Streaming simulado, com aproximadamente **57,4 MiB**.

### 3.5 Armazenamento e consumo na AWS

As camadas do data lake são organizadas no Amazon S3:

- **Bucket:** `fiap-tech-challenge-alfabetizacao-data`
- **Região:** `sa-east-1`

```text
s3://fiap-tech-challenge-alfabetizacao-data/
├── bronze/
├── silver/
└── gold/
```

O AWS Glue Data Catalog mantém os metadados das tabelas e o Amazon Athena
executa consultas SQL diretamente sobre os arquivos Parquet. A separação por
camadas preserva os dados ingeridos, aplica qualidade e integração na Silver e
disponibiliza indicadores prontos para consumo na Gold.

### 3.6 Diagrama inicial da ingestão

O diagrama desenvolvido na primeira fase permanece como visão específica dos
fluxos Batch e Streaming e da construção da camada Bronze:

![Arquitetura inicial da solução](docs/arquitetura-solucao.png)

---

## 4. Resultados das Fases 2 e 3

As três fases técnicas estão concluídas:

| Fase | Entrega | Situação |
|---|---|---|
| 1 | Arquitetura, ingestão híbrida e Bronze | Concluída |
| 2 | Tratamento, integração, Silver e qualidade | Concluída |
| 3 | Gold, consultas, visualizações e IA aplicada | Concluída |

Principais produtos analíticos:

- 10.704 indicadores municipais;
- 54 indicadores por UF;
- 2 indicadores nacionais, referentes a 2023 e 2024;
- cinco visualizações para metas, evolução e desigualdades territoriais.

Em 2024, o indicador nacional foi 59,2%, diante da meta de 59,9%, com evolução
de +3,3 pontos percentuais em relação a 2023.

## 5. Qualidade e governança

O pipeline verifica duplicidades, valores ausentes, chaves de relacionamento e
consistência entre tabelas. Também produz manifestos e relatórios de execução.
A validação final não apresentou erros bloqueantes.

Os dados individuais permanecem nas camadas de processamento. A camada Gold
oferece apenas informações agregadas adequadas ao consumo analítico.

## 6. Monitoramento

A arquitetura prevê o Amazon CloudWatch para acompanhar falhas, duração,
latência e volume processado. EventBridge e Amazon SNS podem distribuir alertas
operacionais. O Amazon Bedrock pode transformar métricas e relatórios de
qualidade em resumos objetivos, sugerindo possíveis causas e próximos passos,
sempre com validação humana.

## 7. Aplicação de inteligência artificial

O Amazon Bedrock pode disponibilizar um assistente analítico sobre a camada
Gold, permitindo perguntas em linguagem natural, geração de resumos para
gestores e apoio à interpretação de alertas. Para modelos preditivos tabulares,
como risco de não atingimento das metas, a camada Gold pode alimentar modelos
no Amazon SageMaker.

Os usos propostos incluem:

- previsão da evolução da alfabetização;
- identificação de municípios com maior risco educacional;
- análise de desigualdades territoriais;
- síntese de indicadores para políticas públicas;
- triagem e contextualização de alertas do pipeline.

Detalhes em [`docs/registro-uso-ia.md`](docs/registro-uso-ia.md).

## 8. FinOps

As decisões de arquitetura reduzem custo sem comprometer a análise:

- Parquet e compressão Snappy reduzem armazenamento e leitura;
- particionamento por ano evita varreduras desnecessárias;
- consultas sobre a camada Gold reduzem o volume processado;
- serviços gerenciados e execução sob demanda evitam capacidade ociosa;
- métricas de uso permitem acompanhar volume, duração e consumo.

## 9. Decisões de arquitetura e trade-offs

- **Batch e streaming:** batch garante reprocessamento histórico; streaming atende eventos recentes e adiciona complexidade operacional.
- **Data lake e warehouse:** o data lake preserva flexibilidade e baixo custo; a Gold fornece o contrato analítico necessário ao consumo.
- **Custo e performance:** arquivos colunares, partições e agregações antecipadas favorecem consultas rápidas com menor leitura.

## 10. Reprodução e validação

Requer Python 3.11 ou superior.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.pipeline all
python -m pytest -q
```

Resultado de referência: `9 passed`.

## 11. Estrutura e evidências

- [Mapa das pastas](docs/estrutura-do-projeto.md)
- `data/bronze/`: dados de entrada em Parquet
- `data/silver/`: dados tratados, integrados e validados
- `data/gold/`: indicadores e agregações analíticas
- [Implementação técnica das Fases 2 e 3](docs/implementacao-fases-2-3.md)
- [Registro e proposta de uso de IA](docs/registro-uso-ia.md)
- [Visualizações e resumos analíticos](docs/evidencias/fase-3/)

As definições e consultas do Amazon Athena estão em `sql/athena/`. Consultas
complementares de validação estão versionadas em `sql/analytics/`.

## 12. Tecnologias utilizadas

Python, Pandas, DuckDB, PyArrow, Parquet, Amazon S3, AWS Glue Data Catalog,
Amazon Athena, Amazon CloudWatch, Amazon Bedrock e Amazon SageMaker.

O código, os testes e a documentação estão versionados em branches de
desenvolvimento e integrados à branch principal do projeto.
