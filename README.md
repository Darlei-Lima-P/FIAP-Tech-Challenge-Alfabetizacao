# FIAP Tech Challenge — Avaliação da Alfabetização

## Objetivo

Este projeto estrutura dados da Avaliação da Alfabetização em uma arquitetura
medalhão, desde a ingestão dos arquivos de origem até indicadores analíticos
para município, UF e Brasil.

## Arquitetura

```text
RAW → BRONZE → SILVER → GOLD → consultas e visualizações
```

- **RAW:** arquivos CSV de origem.
- **Bronze:** dados convertidos para Parquet Snappy e particionados por ano.
- **Silver:** dados tipados, padronizados, integrados e submetidos a regras de qualidade.
- **Gold:** indicadores, metas, rankings, evolução e cobertura para análise.

O diagrama da arquitetura inicial está em
[`docs/arquitetura-solucao.png`](docs/arquitetura-solucao.png). A evolução das
Fases 2 e 3 está documentada em
[`docs/arquitetura-fases-2-3.md`](docs/arquitetura-fases-2-3.md).

## Dados e fluxos

Foram utilizados dados da **Avaliação da Alfabetização**, disponibilizados pela
Base dos Dados. A base de alunos possui 3.867.999 registros, referentes a 2023
e 2024.

O projeto contempla dois fluxos:

- **Batch:** processamento integral dos CSVs de alunos, resultados e metas.
- **Streaming simulado:** 500 eventos de alunos, organizados em 5 lotes de 100
  registros, mantidos separados do processamento batch.

## Implementação

| Fase | Entrega | Situação |
|---|---|---|
| 1 | Arquitetura, ingestão e Bronze | Concluída |
| 2 | Tratamento, integração, Silver e qualidade | Concluída |
| 3 | Gold, consultas, visualizações e validação analítica | Concluída |

### Qualidade e Silver

A Fase 2 aplica tipagem, padronização de identificadores, integração de alunos,
municípios, UFs e metas, além de exportação em Parquet Snappy particionado por
ano. A execução gera relatórios de qualidade e manifestos. O resultado validado
foi `PASS_WITH_WARNINGS`, sem erros bloqueantes.

### Indicadores Gold

As tabelas principais geradas são:

- 10.704 indicadores municipais;
- 54 indicadores por UF;
- 2 indicadores nacionais, para 2023 e 2024.

Em 2024, o indicador nacional foi 59,2%, com meta de 59,9%, diferença de
-0,7 ponto percentual e evolução de +3,3 pontos percentuais em relação a 2023.

## Execução local

Requer Python 3.11 ou superior.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.pipeline all
python -m pytest -q
```

Resultado de referência da suíte: `9 passed`.

## Visualizações e consultas

As cinco visualizações e os resumos analíticos estão em
[`docs/evidencias/fase-3/`](docs/evidencias/fase-3/). As consultas locais estão
em [`sql/analytics/consultas_duckdb.sql`](sql/analytics/consultas_duckdb.sql) e
as consultas analíticas equivalentes em `sql/athena/`.

## Estrutura e documentação

O mapa completo de pastas está em
[`docs/estrutura-do-projeto.md`](docs/estrutura-do-projeto.md).

- [`docs/handoff-fases-2-3.md`](docs/handoff-fases-2-3.md): regras, resultados e evidências das Fases 2 e 3.
- [`docs/registro-uso-ia.md`](docs/registro-uso-ia.md): registro de uso de IA no desenvolvimento.
- [`docs/evidencias/fase-3/`](docs/evidencias/fase-3/): gráficos, CSVs e resumo de indicadores.

## Tecnologias e decisões

Python, Pandas, DuckDB, PyArrow, Parquet, Snappy e consultas SQL foram usados
para priorizar processamento reproduzível, armazenamento colunar e consultas
por período. O particionamento por ano reduz o volume lido em análises com
recorte temporal.

## Status final

As três fases técnicas estão implementadas e os testes automatizados foram
aprovados. O fechamento da entrega depende da revisão da equipe e da gravação
do vídeo executivo.
