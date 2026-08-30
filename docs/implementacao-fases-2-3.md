# Implementação técnica — Fases 2 e 3

Este documento apresenta a implementação das camadas Silver e Gold na
plataforma de dados AWS, incluindo transformação, qualidade, indicadores e
consultas analíticas.

## Fase 2 — Silver e qualidade

O comando abaixo executa o perfil da Bronze, constrói Silver, valida qualidade e gera Gold:

```powershell
python -m src.pipeline all
```

As seis entidades originais foram tipadas, limpas e preservadas. Também foram criadas visões integradas para município, UF e Brasil. O streaming simulado permanece separado do batch porque os 500 eventos são cópias de registros já existentes na base completa.

Principais regras:

- IDs tratados como texto para preservar zeros;
- compressão Snappy e partições por ano;
- nenhuma imputação de nulos legítimos;
- código de rede `0` preservado sem rótulo por ausência de dicionário oficial versionado;
- microdados reconciliados por média ponderada, mantendo o indicador oficial como valor canônico;
- execução idempotente, com substituição controlada das partições geradas.

Resultado de qualidade: `PASS_WITH_WARNINGS`, sem erros bloqueantes. Os avisos conhecidos são:

| Verificação | Quantidade | Interpretação |
|---|---:|---|
| alunos sem resultado municipal | 5 | lacuna referencial informativa |
| rede código 0 sem dicionário | 399 | código preservado sem tradução inventada |
| resultados municipais sem meta | 242 | resultados sem correspondência na tabela de metas |
| metas municipais sem taxa | 120 | meta existente sem resultado oficial |
| divergência acima de 1 pp | 28 | reconciliação entre microdados e resultado oficial |

Os relatórios de qualidade e os manifestos de execução integram a
rastreabilidade do pipeline e alimentam a observabilidade da solução.

## Fase 3 — Gold e análises

As tabelas principais são:

- `gold_indicadores_municipio`: 10.704 registros;
- `gold_indicadores_uf`: 54 registros;
- `gold_indicadores_brasil`: 2 registros, com recorte explícito de 2023 e 2024.

Também foram geradas tabelas auxiliares de evolução, ranking, resumo por UF, metas em formato longo e cobertura de qualidade.

As consultas analíticas sobre o data lake utilizam o Amazon Athena, apoiado
pelo AWS Glue Data Catalog. As definições das tabelas e consultas estão
versionadas em `sql/athena/`; consultas complementares de validação estão em
`sql/analytics/`.

As cinco visualizações e os resumos em CSV/JSON estão em `docs/evidencias/fase-3/`:

- resultado das UFs versus meta em 2024;
- evolução do Brasil de 2023 para 2024;
- distribuição dos municípios em 2024;
- participação versus alfabetização;
- quinze maiores déficits municipais.

Valores de aceite:

- Brasil 2024: 59,2%, meta de 59,9%, diferença de -0,7 pp;
- evolução nacional: +3,3 pp;
- municípios em 2024: 2.788 atingiram, 2.444 ficaram abaixo e 120 não possuem meta;
- tendências municipais: 3.061 melhoraram, 2.157 pioraram, 14 ficaram estáveis e 120 não possuem comparação.

## Testes

```powershell
python -m pytest -q
```

Resultado validado: `9 passed`.
