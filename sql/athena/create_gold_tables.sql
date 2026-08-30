-- Substitua SEU_BUCKET pelo bucket temporário antes da execução.
-- Execute no workgroup fiap_tc_alfabetizacao_wg, região sa-east-1.

-- Execute uma instrução por vez no Query Editor do Athena.
-- Athena aceita CREATE SCHEMA como alias de CREATE DATABASE.
CREATE SCHEMA IF NOT EXISTS fiap_tc_alfabetizacao;

CREATE EXTERNAL TABLE IF NOT EXISTS fiap_tc_alfabetizacao.gold_indicadores_municipio (
    id_municipio string,
    codigo_uf string,
    rede_codigo int,
    rede string,
    taxa_alfabetizacao double,
    meta_ano double,
    meta_alfabetizacao_2030 double,
    nivel_alfabetizacao double,
    percentual_participacao double,
    taxa_resultado_agregado double,
    media_portugues double,
    total_alunos bigint,
    alunos_presentes bigint,
    taxa_presenca double,
    proficiencia_media_microdados double,
    taxa_alfabetizacao_simples double,
    taxa_alfabetizacao_ponderada double,
    gap_meta_ano_pp double,
    gap_meta_2030_pp double,
    diferenca_microdados_pp double,
    status_meta string,
    taxa_ano_anterior double,
    ano_anterior int,
    ranking_brasil bigint,
    ranking_uf bigint,
    variacao_anual_pp double,
    tendencia string
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://SEU_BUCKET/gold/indicadores_municipio/';

CREATE EXTERNAL TABLE IF NOT EXISTS fiap_tc_alfabetizacao.gold_indicadores_uf (
    sigla_uf string,
    rede_codigo int,
    rede string,
    taxa_alfabetizacao double,
    meta_ano double,
    meta_alfabetizacao_2030 double,
    percentual_participacao double,
    taxa_resultado_agregado double,
    media_portugues double,
    gap_meta_ano_pp double,
    gap_meta_2030_pp double,
    status_meta string,
    taxa_ano_anterior double,
    ranking_ano bigint,
    variacao_anual_pp double
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://SEU_BUCKET/gold/indicadores_uf/';

CREATE EXTERNAL TABLE IF NOT EXISTS fiap_tc_alfabetizacao.gold_indicadores_brasil (
    rede_codigo int,
    rede string,
    taxa_alfabetizacao double,
    meta_ano double,
    gap_meta_ano_pp double,
    meta_alfabetizacao_2030 double,
    gap_meta_2030_pp double,
    status_meta string,
    percentual_participacao double,
    taxa_ano_anterior double,
    variacao_anual_pp double
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://SEU_BUCKET/gold/indicadores_brasil/';

MSCK REPAIR TABLE fiap_tc_alfabetizacao.gold_indicadores_municipio;
MSCK REPAIR TABLE fiap_tc_alfabetizacao.gold_indicadores_uf;
MSCK REPAIR TABLE fiap_tc_alfabetizacao.gold_indicadores_brasil;

SHOW TABLES IN fiap_tc_alfabetizacao;
