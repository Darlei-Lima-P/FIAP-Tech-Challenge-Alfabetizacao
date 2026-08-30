# Estrutura do projeto

```text
FIAP-Tech-Challenge-Alfabetizacao/
├── config/                 # parâmetros do pipeline
├── data/
│   └── bronze/             # Parquets de entrada processados
├── docs/
│   ├── evidencias/fase-3/  # visualizações e resumos analíticos
│   ├── arquitetura-solucao.png
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

As saídas completas das camadas Silver e Gold, os artefatos de execução e os
dados RAW são gerados localmente e permanecem fora do Git por volume e por não
serem necessários para reproduzir o código.
