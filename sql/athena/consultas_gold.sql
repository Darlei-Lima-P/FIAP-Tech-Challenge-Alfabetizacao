-- Consultas de aceite no Amazon Athena.
-- Execute no workgroup fiap_tc_alfabetizacao_wg.

-- 1. Validação das quantidades publicadas.
SELECT 'municipio' AS entidade, count(*) AS registros
FROM fiap_tc_alfabetizacao.gold_indicadores_municipio
UNION ALL
SELECT 'uf', count(*)
FROM fiap_tc_alfabetizacao.gold_indicadores_uf
UNION ALL
SELECT 'brasil', count(*)
FROM fiap_tc_alfabetizacao.gold_indicadores_brasil;

-- 2. Panorama Brasil.
SELECT
    ano,
    taxa_alfabetizacao,
    meta_ano,
    gap_meta_ano_pp,
    percentual_participacao,
    variacao_anual_pp,
    status_meta
FROM fiap_tc_alfabetizacao.gold_indicadores_brasil
WHERE ano IN (2023, 2024)
ORDER BY ano;

-- 3. UFs em 2024: resultado, meta e ranking.
SELECT
    ranking_ano,
    sigla_uf,
    taxa_alfabetizacao,
    meta_ano,
    gap_meta_ano_pp,
    status_meta
FROM fiap_tc_alfabetizacao.gold_indicadores_uf
WHERE ano = 2024
ORDER BY ranking_ano, sigla_uf;

-- 4. Situação dos municípios perante a meta de 2024.
SELECT status_meta, count(*) AS municipios
FROM fiap_tc_alfabetizacao.gold_indicadores_municipio
WHERE ano = 2024
GROUP BY status_meta
ORDER BY status_meta;

-- 5. Quinze maiores déficits municipais.
SELECT
    id_municipio,
    codigo_uf,
    taxa_alfabetizacao,
    meta_ano,
    gap_meta_ano_pp,
    ranking_uf
FROM fiap_tc_alfabetizacao.gold_indicadores_municipio
WHERE ano = 2024
  AND status_meta = 'ABAIXO'
ORDER BY gap_meta_ano_pp, id_municipio
LIMIT 15;

-- 6. Maiores evoluções municipais.
SELECT
    id_municipio,
    codigo_uf,
    taxa_ano_anterior,
    taxa_alfabetizacao,
    variacao_anual_pp,
    tendencia
FROM fiap_tc_alfabetizacao.gold_indicadores_municipio
WHERE ano = 2024
  AND variacao_anual_pp IS NOT NULL
ORDER BY variacao_anual_pp DESC, id_municipio
LIMIT 20;

-- 7. Reconciliação entre microdados ponderados e taxa oficial.
SELECT
    ano,
    id_municipio,
    codigo_uf,
    taxa_alfabetizacao,
    taxa_alfabetizacao_ponderada,
    diferenca_microdados_pp,
    total_alunos
FROM fiap_tc_alfabetizacao.gold_indicadores_municipio
WHERE ano = 2024
  AND diferenca_microdados_pp IS NOT NULL
ORDER BY abs(diferenca_microdados_pp) DESC, id_municipio
LIMIT 30;
