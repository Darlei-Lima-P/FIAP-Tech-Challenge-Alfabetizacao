# Estrutura do projeto

```text
FIAP-Tech-Challenge-Alfabetizacao/
├── config/                 # parâmetros do pipeline
├── data/
│   ├── bronze/             # dados de entrada em Parquet
│   ├── silver/             # dados tratados, integrados e validados
│   └── gold/               # indicadores e agregações analíticas
├── docs/
│   ├── evidencias/fase-3/  # visualizações e resumos analíticos
│   ├── arquitetura-solucao.png
│   ├── arquitetura-completa.png
│   ├── arquitetura-fases-2-3.md
│   ├── handoff-fases-2-3.md
│   └── registro-uso-ia.md
├── sql/
│   ├── analytics/          # consultas DuckDB
│   └── athena/             # DDL e consultas analíticas
├── src/
│   ├── ingest_bronze.py
│   ├── streaming_simulado.py
│   ├── pipeline.py         # Silver, qualidade e Gold
│   └── visualize.py
├── tests/                  # testes automatizados
├── requirements.txt
└── README.md
```

As camadas Bronze, Silver e Gold estão incluídas no repositório para permitir a
inspeção dos dados e dos resultados produzidos pelo pipeline.
