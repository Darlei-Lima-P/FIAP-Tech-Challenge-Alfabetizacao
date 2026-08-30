# Estrutura do projeto

```text
FIAP-Tech-Challenge-Alfabetizacao/
├── .github/
│   └── workflows/tests.yml # validação automatizada
├── config/
│   └── pipeline.yml        # parâmetros do pipeline
├── data/
│   ├── bronze/             # dados de entrada em Parquet
│   ├── silver/             # dados tratados, integrados e validados
│   └── gold/               # indicadores e agregações analíticas
├── docs/
│   ├── evidencias/fase-3/  # visualizações e resumos analíticos
│   ├── arquitetura-solucao.png
│   ├── arquitetura-completa.png
│   ├── arquitetura-fases-2-3.md
│   ├── estrutura-do-projeto.md
│   ├── implementacao-fases-2-3.md
│   └── registro-uso-ia.md
├── sql/
│   ├── analytics/
│   │   └── consultas_duckdb.sql
│   └── athena/
│       ├── create_gold_tables.sql
│       └── consultas_gold.sql
├── src/
│   ├── __init__.py
│   ├── download_data.py
│   ├── ingest_bronze.py
│   ├── streaming_simulado.py
│   ├── pipeline.py         # Silver, qualidade e Gold
│   └── visualize.py
├── tests/
│   └── test_pipeline.py    # testes automatizados
├── .gitignore
├── requirements.txt
└── README.md
```

As camadas Bronze, Silver e Gold estão incluídas no repositório para permitir a
inspeção dos dados e dos resultados produzidos pelo pipeline.
