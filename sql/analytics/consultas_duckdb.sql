-- Consultas analíticas locais para as tabelas Gold.
-- Execução: DuckDB conectado ao arquivo artifacts/pipeline.duckdb.

-- 1. Panorama nacional e evolução anual.
SELECT
    ano,
    taxa_alfabetizacao,
    meta_ano,
    gap_meta_ano_pp,
    percentual_participacao,
    variacao_anual_pp,
    status_meta
FROM gold_indicadores_brasil
ORDER BY ano;

-- 2. Ranking das UFs em 2024 e distância para a meta.
SELECT
    ranking_ano,
    sigla_uf,
    taxa_alfabetizacao,
    meta_ano,
    gap_meta_ano_pp,
    percentual_participacao,
    status_meta
FROM gold_indicadores_uf
WHERE ano = 2024
ORDER BY ranking_ano, sigla_uf;

-- 3. Quinze maiores déficits municipais em 2024.
SELECT
    id_municipio,
    codigo_uf,
    taxa_alfabetizacao,
    meta_ano,
    gap_meta_ano_pp,
    ranking_uf
FROM gold_indicadores_municipio
WHERE ano = 2024
  AND status_meta = 'ABAIXO'
ORDER BY gap_meta_ano_pp, id_municipio
LIMIT 15;

-- 4. Municípios que mais melhoraram ou pioraram entre 2023 e 2024.
SELECT
    id_municipio,
    codigo_uf,
    taxa_ano_anterior,
    taxa_alfabetizacao,
    variacao_anual_pp,
    tendencia
FROM gold_indicadores_municipio
WHERE ano = 2024
  AND variacao_anual_pp IS NOT NULL
ORDER BY variacao_anual_pp DESC, id_municipio
LIMIT 20;

-- Para observar as maiores quedas, trocar DESC por ASC.

-- 5. Participação versus taxa de alfabetização municipal.
SELECT
    id_municipio,
    codigo_uf,
    percentual_participacao,
    taxa_alfabetizacao,
    status_meta
FROM gold_indicadores_municipio
WHERE ano = 2024
  AND percentual_participacao IS NOT NULL
  AND taxa_alfabetizacao IS NOT NULL
ORDER BY percentual_participacao DESC, id_municipio;

-- 6. Maiores divergências entre microdados ponderados e resultado oficial.
SELECT
    ano,
    id_municipio,
    codigo_uf,
    taxa_alfabetizacao,
    taxa_alfabetizacao_ponderada,
    diferenca_microdados_pp,
    total_alunos
FROM gold_indicadores_municipio
WHERE diferenca_microdados_pp IS NOT NULL
ORDER BY abs(diferenca_microdados_pp) DESC, ano, id_municipio
LIMIT 30;
