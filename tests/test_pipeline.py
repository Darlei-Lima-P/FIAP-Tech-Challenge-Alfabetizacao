from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pytest

from src.pipeline import LiteracyPipeline, ROOT
from src.visualize import generate_visualizations


@pytest.fixture(scope="session")
def pipeline_result() -> dict:
    pipeline = LiteracyPipeline()
    try:
        return pipeline.run_all()
    finally:
        pipeline.close()


@pytest.fixture()
def database(pipeline_result: dict):
    connection = duckdb.connect(str(ROOT / "artifacts" / "pipeline.duckdb"), read_only=True)
    try:
        yield connection
    finally:
        connection.close()


def test_silver_baseline_counts(pipeline_result: dict) -> None:
    assert pipeline_result["silver"] == {
        "silver_alunos": 3_867_999,
        "silver_municipio": 23_995,
        "silver_uf": 145,
        "silver_meta_brasil": 3,
        "silver_meta_municipio": 10_704,
        "silver_meta_uf": 54,
        "silver_streaming_alunos": 500,
        "silver_alunos_municipio": 12_431,
        "silver_municipio_integrado": 24_045,
        "silver_uf_integrado": 150,
        "silver_brasil_integrado": 3,
    }


def test_gold_baseline_counts(pipeline_result: dict) -> None:
    assert pipeline_result["gold"]["gold_indicadores_municipio"] == 10_704
    assert pipeline_result["gold"]["gold_indicadores_uf"] == 54
    assert pipeline_result["gold"]["gold_indicadores_brasil"] == 2
    assert pipeline_result["gold"]["gold_evolucao_municipio"] == 10_704
    assert pipeline_result["gold"]["gold_ranking_municipio"] == 10_704
    assert pipeline_result["gold"]["gold_metas_brasil_long"] == 14


def test_quality_has_no_blocking_errors(pipeline_result: dict) -> None:
    assert pipeline_result["quality"] == "PASS_WITH_WARNINGS"


def test_brazil_2024_acceptance_values(database: duckdb.DuckDBPyConnection) -> None:
    row = database.execute(
        """
        SELECT taxa_alfabetizacao, meta_ano, gap_meta_ano_pp, variacao_anual_pp, status_meta
        FROM gold_indicadores_brasil
        WHERE ano = 2024
        """
    ).fetchone()
    assert row[0] == pytest.approx(59.2)
    assert row[1] == pytest.approx(59.9)
    assert row[2] == pytest.approx(-0.7)
    assert row[3] == pytest.approx(3.3)
    assert row[4] == "ABAIXO"


def test_municipal_status_counts_2024(database: duckdb.DuckDBPyConnection) -> None:
    result = dict(
        database.execute(
            """
            SELECT status_meta, count(*)
            FROM gold_indicadores_municipio
            WHERE ano = 2024
            GROUP BY status_meta
            """
        ).fetchall()
    )
    assert result == {"ATINGIU": 2_788, "ABAIXO": 2_444, "SEM_META": 120}


def test_student_semantics(database: duckdb.DuckDBPyConnection) -> None:
    invalid = database.execute(
        """
        SELECT count(*) FROM silver_alunos
        WHERE (proficiencia IS NULL) <> (peso_aluno IS NULL)
           OR (preenchimento_caderno_flag = 0 AND (
                proficiencia IS NOT NULL OR peso_aluno IS NOT NULL OR alfabetizado_flag <> 0
           ))
           OR (proficiencia IS NOT NULL AND alfabetizado_flag <> CAST(proficiencia >= 743 AS INTEGER))
        """
    ).fetchone()[0]
    assert invalid == 0


def test_streaming_is_kept_separate(database: duckdb.DuckDBPyConnection) -> None:
    streaming_rows = database.execute(
        "SELECT count(*) FROM silver_streaming_alunos"
    ).fetchone()[0]
    overlap = database.execute(
        """
        SELECT count(*)
        FROM silver_streaming_alunos s
        JOIN silver_alunos a
          USING (ano, id_municipio, id_escola, id_aluno)
        """
    ).fetchone()[0]
    assert streaming_rows == 500
    assert overlap == 500
    assert pipeline_table_count(database, "gold_indicadores_municipio") == 10_704


def pipeline_table_count(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    return connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_partitioned_outputs_are_idempotent(pipeline_result: dict) -> None:
    output = ROOT / "data" / "gold" / "indicadores_brasil"
    first_files = sorted(path.relative_to(output) for path in output.rglob("*.parquet"))
    first_hashes = [hashlib.sha256((output / path).read_bytes()).hexdigest() for path in first_files]

    pipeline = LiteracyPipeline()
    try:
        second = pipeline.run_all()
    finally:
        pipeline.close()

    second_files = sorted(path.relative_to(output) for path in output.rglob("*.parquet"))
    second_hashes = [hashlib.sha256((output / path).read_bytes()).hexdigest() for path in second_files]
    assert second["source_fingerprint"] == pipeline_result["source_fingerprint"]
    assert second["gold"] == pipeline_result["gold"]
    assert first_files == second_files
    assert first_hashes == second_hashes


def test_visualizations_are_generated(pipeline_result: dict) -> None:
    summary = generate_visualizations()
    output = ROOT / "docs" / "evidencias" / "fase-3"
    images = sorted(output.glob("*.png"))
    assert len(images) == 5
    assert all(image.stat().st_size > 20_000 for image in images)
    assert summary["brasil_taxa_2024"] == pytest.approx(59.2)
    assert summary["municipios_atingiram_meta_2024"] == 2_788
