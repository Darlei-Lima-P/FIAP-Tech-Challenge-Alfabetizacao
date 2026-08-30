from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
import yaml


ROOT = Path(__file__).resolve().parent.parent


@dataclass
class QualityCheck:
    layer: str
    name: str
    severity: str
    value: int | float
    expected: str
    passed: bool
    detail: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    path = config_path or ROOT / "config" / "pipeline.yml"
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def reset_generated_dir(target: Path, allowed_root: Path) -> None:
    target_resolved = target.resolve()
    allowed_resolved = allowed_root.resolve()
    if target_resolved == allowed_resolved or allowed_resolved not in target_resolved.parents:
        raise ValueError(f"Diretorio de saida inseguro: {target_resolved}")
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)


class LiteracyPipeline:
    def __init__(self, root: Path = ROOT, config_path: Path | None = None) -> None:
        self.root = root.resolve()
        self.config = load_config(config_path)
        paths = self.config["paths"]
        self.bronze = self.root / paths["bronze"]
        self.silver = self.root / paths["silver"]
        self.gold = self.root / paths["gold"]
        self.artifacts = self.root / paths["artifacts"]
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.database = self.artifacts / "pipeline.duckdb"
        self.con = duckdb.connect(str(self.database))
        self.con.execute("SET threads = 4")
        self.con.execute("SET preserve_insertion_order = false")

    def close(self) -> None:
        self.con.close()

    def bronze_glob(self, table: str) -> str:
        return sql_path(self.bronze / table / "**" / "*.parquet")

    def read_bronze(self, table: str) -> str:
        return f"read_parquet('{self.bronze_glob(table)}', hive_partitioning=false)"

    def profile(self) -> dict[str, Any]:
        tables: dict[str, Any] = {}
        all_hashes: list[str] = []
        for table_dir in sorted(path for path in self.bronze.iterdir() if path.is_dir()):
            files = sorted(table_dir.rglob("*.parquet"))
            if not files:
                continue
            row_count = 0
            byte_count = 0
            schemas: set[str] = set()
            file_entries = []
            for file_path in files:
                metadata = pq.ParquetFile(file_path).metadata
                size = file_path.stat().st_size
                digest = hashlib.sha256(file_path.read_bytes()).hexdigest()
                row_count += metadata.num_rows
                byte_count += size
                schemas.add(str(pq.ParquetFile(file_path).schema_arrow))
                relative = file_path.relative_to(self.root).as_posix()
                all_hashes.append(f"{relative}:{size}:{digest}")
                file_entries.append(
                    {
                        "path": relative,
                        "rows": metadata.num_rows,
                        "bytes": size,
                        "sha256": digest,
                    }
                )
            tables[table_dir.name] = {
                "files": len(files),
                "rows": row_count,
                "bytes": byte_count,
                "schema_variants": len(schemas),
                "schema": sorted(schemas),
                "objects": file_entries,
            }

        fingerprint = hashlib.sha256("\n".join(all_hashes).encode("utf-8")).hexdigest()
        report = {
            "generated_at": utc_now(),
            "source_fingerprint": fingerprint,
            "bronze_path": self.bronze.relative_to(self.root).as_posix(),
            "tables": tables,
        }
        output = self.artifacts / "profile"
        output.mkdir(parents=True, exist_ok=True)
        (output / "bronze_profile.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return report

    def build_silver(self) -> dict[str, int]:
        alunos = self.read_bronze("alunos")
        municipio = self.read_bronze("municipio")
        uf = self.read_bronze("uf")
        meta_brasil = self.read_bronze("meta_brasil")
        meta_municipio = self.read_bronze("meta_municipio")
        meta_uf = self.read_bronze("meta_uf")
        streaming = self.read_bronze("streaming")

        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE silver_alunos AS
            SELECT DISTINCT
                CAST(ano AS INTEGER) AS ano,
                lpad(CAST(id_municipio AS VARCHAR), 7, '0') AS id_municipio,
                lpad(CAST(id_escola AS VARCHAR), 8, '0') AS id_escola,
                lpad(CAST(id_aluno AS VARCHAR), 8, '0') AS id_aluno,
                CAST(caderno AS VARCHAR) AS caderno,
                CAST(serie AS VARCHAR) AS serie,
                CAST(rede AS INTEGER) AS rede_codigo,
                CASE CAST(rede AS INTEGER)
                    WHEN 0 THEN NULL
                    WHEN 2 THEN 'Estadual'
                    WHEN 3 THEN 'Municipal'
                    WHEN 4 THEN 'Privada'
                    WHEN 5 THEN 'Publica'
                    ELSE 'Desconhecida'
                END AS rede,
                CAST(presenca AS INTEGER) AS presenca_flag,
                CAST(preenchimento_caderno AS INTEGER) AS preenchimento_caderno_flag,
                CAST(alfabetizado AS INTEGER) AS alfabetizado_flag,
                CAST(proficiencia AS DOUBLE) AS proficiencia,
                CAST(peso_aluno AS DOUBLE) AS peso_aluno,
                substr(lpad(CAST(id_municipio AS VARCHAR), 7, '0'), 1, 2) AS codigo_uf
            FROM {alunos}
            """
        )

        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE silver_municipio AS
            SELECT DISTINCT
                CAST(ano AS INTEGER) AS ano,
                lpad(CAST(id_municipio AS VARCHAR), 7, '0') AS id_municipio,
                CAST(serie AS VARCHAR) AS serie,
                CAST(rede AS INTEGER) AS rede_codigo,
                CASE CAST(rede AS INTEGER)
                    WHEN 0 THEN NULL
                    WHEN 2 THEN 'Estadual'
                    WHEN 3 THEN 'Municipal'
                    WHEN 4 THEN 'Privada'
                    WHEN 5 THEN 'Publica'
                    ELSE 'Desconhecida'
                END AS rede,
                CAST(taxa_alfabetizacao AS DOUBLE) AS taxa_alfabetizacao,
                CAST(media_portugues AS DOUBLE) AS media_portugues,
                CAST(proporcao_aluno_nivel_0 AS DOUBLE) AS proporcao_aluno_nivel_0,
                CAST(proporcao_aluno_nivel_1 AS DOUBLE) AS proporcao_aluno_nivel_1,
                CAST(proporcao_aluno_nivel_2 AS DOUBLE) AS proporcao_aluno_nivel_2,
                CAST(proporcao_aluno_nivel_3 AS DOUBLE) AS proporcao_aluno_nivel_3,
                CAST(proporcao_aluno_nivel_4 AS DOUBLE) AS proporcao_aluno_nivel_4,
                CAST(proporcao_aluno_nivel_5 AS DOUBLE) AS proporcao_aluno_nivel_5,
                CAST(proporcao_aluno_nivel_6 AS DOUBLE) AS proporcao_aluno_nivel_6,
                CAST(proporcao_aluno_nivel_7 AS DOUBLE) AS proporcao_aluno_nivel_7,
                CAST(proporcao_aluno_nivel_8 AS DOUBLE) AS proporcao_aluno_nivel_8,
                substr(lpad(CAST(id_municipio AS VARCHAR), 7, '0'), 1, 2) AS codigo_uf
            FROM {municipio}
            """
        )

        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE silver_uf AS
            SELECT DISTINCT
                CAST(ano AS INTEGER) AS ano,
                upper(trim(CAST(sigla_uf AS VARCHAR))) AS sigla_uf,
                CAST(serie AS VARCHAR) AS serie,
                CAST(rede AS INTEGER) AS rede_codigo,
                CASE CAST(rede AS INTEGER)
                    WHEN 0 THEN NULL
                    WHEN 2 THEN 'Estadual'
                    WHEN 3 THEN 'Municipal'
                    WHEN 4 THEN 'Privada'
                    WHEN 5 THEN 'Publica'
                    ELSE 'Desconhecida'
                END AS rede,
                CAST(taxa_alfabetizacao AS DOUBLE) AS taxa_alfabetizacao,
                CAST(media_portugues AS DOUBLE) AS media_portugues,
                CAST(proporcao_aluno_nivel_0 AS DOUBLE) AS proporcao_aluno_nivel_0,
                CAST(proporcao_aluno_nivel_1 AS DOUBLE) AS proporcao_aluno_nivel_1,
                CAST(proporcao_aluno_nivel_2 AS DOUBLE) AS proporcao_aluno_nivel_2,
                CAST(proporcao_aluno_nivel_3 AS DOUBLE) AS proporcao_aluno_nivel_3,
                CAST(proporcao_aluno_nivel_4 AS DOUBLE) AS proporcao_aluno_nivel_4,
                CAST(proporcao_aluno_nivel_5 AS DOUBLE) AS proporcao_aluno_nivel_5,
                CAST(proporcao_aluno_nivel_6 AS DOUBLE) AS proporcao_aluno_nivel_6,
                CAST(proporcao_aluno_nivel_7 AS DOUBLE) AS proporcao_aluno_nivel_7,
                CAST(proporcao_aluno_nivel_8 AS DOUBLE) AS proporcao_aluno_nivel_8
            FROM {uf}
            """
        )

        meta_columns = """
            CAST(taxa_alfabetizacao AS DOUBLE) AS taxa_alfabetizacao,
            CAST(meta_alfabetizacao_2024 AS DOUBLE) AS meta_alfabetizacao_2024,
            CAST(meta_alfabetizacao_2025 AS DOUBLE) AS meta_alfabetizacao_2025,
            CAST(meta_alfabetizacao_2026 AS DOUBLE) AS meta_alfabetizacao_2026,
            CAST(meta_alfabetizacao_2027 AS DOUBLE) AS meta_alfabetizacao_2027,
            CAST(meta_alfabetizacao_2028 AS DOUBLE) AS meta_alfabetizacao_2028,
            CAST(meta_alfabetizacao_2029 AS DOUBLE) AS meta_alfabetizacao_2029,
            CAST(meta_alfabetizacao_2030 AS DOUBLE) AS meta_alfabetizacao_2030
        """
        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE silver_meta_brasil AS
            SELECT DISTINCT
                CAST(ano AS INTEGER) AS ano,
                5 AS rede_codigo,
                trim(CAST(rede AS VARCHAR)) AS rede,
                {meta_columns},
                CAST(percentual_participacao AS DOUBLE) AS percentual_participacao
            FROM {meta_brasil}
            """
        )
        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE silver_meta_municipio AS
            SELECT DISTINCT
                CAST(ano AS INTEGER) AS ano,
                lpad(CAST(id_municipio AS VARCHAR), 7, '0') AS id_municipio,
                3 AS rede_codigo,
                trim(CAST(rede AS VARCHAR)) AS rede,
                {meta_columns},
                CAST(nivel_alfabetizacao AS DOUBLE) AS nivel_alfabetizacao,
                CAST(percentual_participacao AS DOUBLE) AS percentual_participacao,
                substr(lpad(CAST(id_municipio AS VARCHAR), 7, '0'), 1, 2) AS codigo_uf
            FROM {meta_municipio}
            """
        )
        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE silver_meta_uf AS
            SELECT DISTINCT
                CAST(ano AS INTEGER) AS ano,
                upper(trim(CAST(sigla_uf AS VARCHAR))) AS sigla_uf,
                5 AS rede_codigo,
                trim(CAST(rede AS VARCHAR)) AS rede,
                {meta_columns},
                CAST(percentual_participacao AS DOUBLE) AS percentual_participacao
            FROM {meta_uf}
            """
        )

        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE silver_streaming_alunos AS
            SELECT DISTINCT
                CAST(ano AS INTEGER) AS ano,
                lpad(CAST(id_municipio AS VARCHAR), 7, '0') AS id_municipio,
                lpad(CAST(id_escola AS VARCHAR), 8, '0') AS id_escola,
                lpad(CAST(id_aluno AS VARCHAR), 8, '0') AS id_aluno,
                CAST(caderno AS VARCHAR) AS caderno,
                CAST(serie AS VARCHAR) AS serie,
                CAST(rede AS INTEGER) AS rede_codigo,
                CAST(presenca AS INTEGER) AS presenca_flag,
                CAST(preenchimento_caderno AS INTEGER) AS preenchimento_caderno_flag,
                CAST(alfabetizado AS INTEGER) AS alfabetizado_flag,
                CAST(proficiencia AS DOUBLE) AS proficiencia,
                CAST(peso_aluno AS DOUBLE) AS peso_aluno,
                try_cast(timestamp_ingestao AS TIMESTAMPTZ) AS timestamp_ingestao
            FROM {streaming}
            """
        )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE silver_alunos_municipio AS
            SELECT
                ano,
                id_municipio,
                codigo_uf,
                rede_codigo,
                any_value(rede) AS rede,
                count(*) AS total_alunos,
                CAST(sum(presenca_flag) AS BIGINT) AS alunos_presentes,
                100.0 * avg(presenca_flag) AS taxa_presenca,
                sum(proficiencia * peso_aluno) FILTER (
                    WHERE preenchimento_caderno_flag = 1
                ) / nullif(sum(peso_aluno) FILTER (
                    WHERE preenchimento_caderno_flag = 1
                ), 0) AS proficiencia_media,
                100.0 * avg(alfabetizado_flag) FILTER (
                    WHERE preenchimento_caderno_flag = 1
                ) AS taxa_alfabetizacao_simples,
                100.0 * sum(alfabetizado_flag * peso_aluno) FILTER (
                    WHERE preenchimento_caderno_flag = 1
                ) / nullif(sum(peso_aluno) FILTER (
                    WHERE preenchimento_caderno_flag = 1
                ), 0) AS taxa_alfabetizacao_ponderada
            FROM silver_alunos
            GROUP BY ano, id_municipio, codigo_uf, rede_codigo
            """
        )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE silver_municipio_integrado AS
            SELECT
                coalesce(m.ano, mt.ano) AS ano,
                coalesce(m.id_municipio, mt.id_municipio) AS id_municipio,
                coalesce(m.codigo_uf, mt.codigo_uf) AS codigo_uf,
                coalesce(m.rede_codigo, mt.rede_codigo) AS rede_codigo,
                coalesce(m.rede, mt.rede) AS rede,
                m.serie,
                m.taxa_alfabetizacao AS taxa_resultado_agregado,
                m.media_portugues,
                m.proporcao_aluno_nivel_0,
                m.proporcao_aluno_nivel_1,
                m.proporcao_aluno_nivel_2,
                m.proporcao_aluno_nivel_3,
                m.proporcao_aluno_nivel_4,
                m.proporcao_aluno_nivel_5,
                m.proporcao_aluno_nivel_6,
                m.proporcao_aluno_nivel_7,
                m.proporcao_aluno_nivel_8,
                mt.taxa_alfabetizacao AS taxa_alfabetizacao,
                mt.meta_alfabetizacao_2024,
                mt.meta_alfabetizacao_2025,
                mt.meta_alfabetizacao_2026,
                mt.meta_alfabetizacao_2027,
                mt.meta_alfabetizacao_2028,
                mt.meta_alfabetizacao_2029,
                mt.meta_alfabetizacao_2030,
                mt.nivel_alfabetizacao,
                mt.percentual_participacao,
                a.total_alunos,
                a.alunos_presentes,
                a.taxa_presenca,
                a.proficiencia_media AS proficiencia_media_microdados,
                a.taxa_alfabetizacao_simples,
                a.taxa_alfabetizacao_ponderada,
                m.id_municipio IS NOT NULL AS tem_resultado,
                mt.id_municipio IS NOT NULL AS tem_meta
            FROM silver_municipio m
            FULL OUTER JOIN silver_meta_municipio mt
              ON m.ano = mt.ano
             AND m.id_municipio = mt.id_municipio
             AND m.rede_codigo = mt.rede_codigo
            LEFT JOIN silver_alunos_municipio a
              ON coalesce(m.ano, mt.ano) = a.ano
             AND coalesce(m.id_municipio, mt.id_municipio) = a.id_municipio
             AND coalesce(m.rede_codigo, mt.rede_codigo) = a.rede_codigo
            """
        )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE silver_uf_integrado AS
            SELECT
                coalesce(u.ano, mt.ano) AS ano,
                coalesce(u.sigla_uf, mt.sigla_uf) AS sigla_uf,
                coalesce(u.rede_codigo, mt.rede_codigo) AS rede_codigo,
                coalesce(u.rede, mt.rede) AS rede,
                u.serie,
                u.taxa_alfabetizacao AS taxa_resultado_agregado,
                u.media_portugues,
                u.proporcao_aluno_nivel_0,
                u.proporcao_aluno_nivel_1,
                u.proporcao_aluno_nivel_2,
                u.proporcao_aluno_nivel_3,
                u.proporcao_aluno_nivel_4,
                u.proporcao_aluno_nivel_5,
                u.proporcao_aluno_nivel_6,
                u.proporcao_aluno_nivel_7,
                u.proporcao_aluno_nivel_8,
                mt.taxa_alfabetizacao AS taxa_alfabetizacao,
                mt.meta_alfabetizacao_2024,
                mt.meta_alfabetizacao_2025,
                mt.meta_alfabetizacao_2026,
                mt.meta_alfabetizacao_2027,
                mt.meta_alfabetizacao_2028,
                mt.meta_alfabetizacao_2029,
                mt.meta_alfabetizacao_2030,
                mt.percentual_participacao,
                u.sigla_uf IS NOT NULL AS tem_resultado,
                mt.sigla_uf IS NOT NULL AS tem_meta
            FROM silver_uf u
            FULL OUTER JOIN silver_meta_uf mt
              ON u.ano = mt.ano
             AND u.sigla_uf = mt.sigla_uf
             AND u.rede_codigo = mt.rede_codigo
            """
        )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE silver_brasil_integrado AS
            SELECT * FROM silver_meta_brasil
            """
        )

        silver_tables = [
            "silver_alunos",
            "silver_municipio",
            "silver_uf",
            "silver_meta_brasil",
            "silver_meta_municipio",
            "silver_meta_uf",
            "silver_streaming_alunos",
            "silver_alunos_municipio",
            "silver_municipio_integrado",
            "silver_uf_integrado",
            "silver_brasil_integrado",
        ]
        counts = {table: self.table_count(table) for table in silver_tables}
        for table in silver_tables:
            self.export_partitioned(table, self.silver)
        self.write_run_manifest("silver", counts)
        return counts

    def table_count(self, table: str) -> int:
        return int(self.con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])

    def export_partitioned(self, table: str, layer_root: Path) -> None:
        target = layer_root / table.removeprefix("silver_").removeprefix("gold_")
        reset_generated_dir(target, layer_root)
        columns = [row[0] for row in self.con.execute(f"DESCRIBE {table}").fetchall()]
        if "ano" not in columns:
            file_path = target / "part-000.parquet"
            self.con.execute(
                f"COPY {table} TO '{sql_path(file_path)}' "
                "(FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            return
        years = [row[0] for row in self.con.execute(f"SELECT DISTINCT ano FROM {table} ORDER BY ano").fetchall()]
        for year in years:
            if year is None:
                continue
            partition = target / f"ano={int(year)}"
            partition.mkdir(parents=True, exist_ok=True)
            file_path = partition / "part-000.parquet"
            self.con.execute(
                f"COPY (SELECT * EXCLUDE (ano) FROM {table} WHERE ano = {int(year)}) "
                f"TO '{sql_path(file_path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )

    def run_quality(self) -> dict[str, Any]:
        checks: list[QualityCheck] = []

        def add(
            layer: str,
            name: str,
            severity: str,
            sql: str,
            expected: str = "0",
            detail: str = "",
        ) -> None:
            value = self.con.execute(sql).fetchone()[0]
            checks.append(
                QualityCheck(
                    layer=layer,
                    name=name,
                    severity=severity,
                    value=value,
                    expected=expected,
                    passed=value == 0,
                    detail=detail,
                )
            )

        bronze_keys = {
            "alunos": "ano, id_municipio, id_escola, id_aluno",
            "municipio": "ano, id_municipio, rede",
            "uf": "ano, sigla_uf, rede",
            "meta_brasil": "ano, rede",
            "meta_municipio": "ano, id_municipio, rede",
            "meta_uf": "ano, sigla_uf, rede",
        }
        for table, keys in bronze_keys.items():
            add(
                "bronze",
                f"{table}_chaves_duplicadas",
                "ERROR",
                f"""
                SELECT count(*) FROM (
                    SELECT {keys}, count(*) n
                    FROM {self.read_bronze(table)}
                    GROUP BY {keys}
                    HAVING count(*) > 1
                )
                """,
                detail="Nenhuma chave logica pode aparecer mais de uma vez.",
            )

        add(
            "silver",
            "alunos_chaves_nulas",
            "ERROR",
            """
            SELECT count(*) FROM silver_alunos
            WHERE ano IS NULL OR id_municipio IS NULL OR id_escola IS NULL OR id_aluno IS NULL
            """,
        )
        add(
            "silver",
            "municipios_invalidos",
            "ERROR",
            "SELECT count(*) FROM silver_municipio WHERE length(id_municipio) <> 7",
        )
        add(
            "silver",
            "identificadores_alunos_invalidos",
            "ERROR",
            """
            SELECT count(*) FROM silver_alunos
            WHERE length(id_municipio) <> 7
               OR length(id_escola) <> 8
               OR length(id_aluno) <> 8
            """,
        )
        add(
            "silver",
            "taxas_municipais_fora_da_faixa",
            "ERROR",
            "SELECT count(*) FROM silver_municipio WHERE taxa_alfabetizacao NOT BETWEEN 0 AND 100",
        )
        add(
            "silver",
            "taxas_uf_fora_da_faixa",
            "ERROR",
            "SELECT count(*) FROM silver_uf WHERE taxa_alfabetizacao NOT BETWEEN 0 AND 100",
        )
        add(
            "silver",
            "flags_alunos_invalidas",
            "ERROR",
            """
            SELECT count(*) FROM silver_alunos
            WHERE presenca_flag NOT IN (0,1)
               OR preenchimento_caderno_flag NOT IN (0,1)
               OR alfabetizado_flag NOT IN (0,1)
            """,
        )
        add(
            "silver",
            "semantica_caderno_presenca",
            "ERROR",
            """
            SELECT count(*) FROM silver_alunos
            WHERE preenchimento_caderno_flag = 1 AND presenca_flag <> 1
            """,
        )
        add(
            "silver",
            "semantica_medidas_aluno",
            "ERROR",
            """
            SELECT count(*) FROM silver_alunos
            WHERE (proficiencia IS NULL) <> (peso_aluno IS NULL)
               OR (preenchimento_caderno_flag = 0 AND (
                    proficiencia IS NOT NULL OR peso_aluno IS NOT NULL OR alfabetizado_flag <> 0
               ))
               OR (preenchimento_caderno_flag = 1 AND (
                    proficiencia IS NULL OR peso_aluno IS NULL
               ))
               OR (proficiencia IS NOT NULL AND alfabetizado_flag <> CAST(proficiencia >= 743 AS INTEGER))
            """,
        )
        monotonic = " OR ".join(
            f"(meta_alfabetizacao_{year} IS NOT NULL AND meta_alfabetizacao_{year + 1} IS NOT NULL "
            f"AND meta_alfabetizacao_{year + 1} < meta_alfabetizacao_{year})"
            for year in range(2024, 2030)
        )
        for meta_table in ("silver_meta_brasil", "silver_meta_municipio", "silver_meta_uf"):
            add(
                "silver",
                f"{meta_table.removeprefix('silver_')}_metas_nao_monotonicas",
                "ERROR",
                f"SELECT count(*) FROM {meta_table} WHERE {monotonic}",
                detail="Metas anuais devem ser nao decrescentes ate 2030.",
            )
        tolerance = float(self.config["quality"]["level_sum_tolerance_pp"])
        levels = " + ".join(f"proporcao_aluno_nivel_{index}" for index in range(9))
        not_null = " AND ".join(
            f"proporcao_aluno_nivel_{index} IS NOT NULL" for index in range(9)
        )
        add(
            "silver",
            "soma_niveis_municipio",
            "ERROR",
            f"SELECT count(*) FROM silver_municipio WHERE {not_null} AND abs(({levels}) - 100) > {tolerance}",
            detail=f"Tolerancia de {tolerance} ponto percentual para arredondamento.",
        )
        add(
            "silver",
            "soma_niveis_uf",
            "ERROR",
            f"SELECT count(*) FROM silver_uf WHERE {not_null} AND abs(({levels}) - 100) > {tolerance}",
            detail=f"Tolerancia de {tolerance} ponto percentual para arredondamento.",
        )
        add(
            "silver",
            "alunos_sem_resultado_municipal",
            "WARN",
            """
            SELECT count(*) FROM (
                SELECT DISTINCT a.ano, a.id_municipio
                FROM silver_alunos a
                LEFT JOIN silver_municipio m
                  ON a.ano = m.ano AND a.id_municipio = m.id_municipio
                WHERE m.id_municipio IS NULL
            )
            """,
            detail="Cobertura referencial informativa; nao remove alunos validos.",
        )
        add(
            "silver",
            "rede_codigo_zero_sem_dicionario",
            "WARN",
            """
            SELECT
                (SELECT count(*) FROM silver_municipio WHERE rede_codigo = 0)
              + (SELECT count(*) FROM silver_uf WHERE rede_codigo = 0)
            """,
            detail="Codigo 0 e preservado sem rotulo ate existir dicionario oficial versionado.",
        )
        add(
            "silver",
            "resultados_municipais_sem_meta",
            "WARN",
            """
            SELECT count(*) FROM silver_municipio_integrado
            WHERE tem_resultado AND NOT tem_meta AND rede_codigo = 3
            """,
            detail="Municipios sem correspondencia na tabela oficial de metas.",
        )
        add(
            "silver",
            "metas_municipais_sem_taxa",
            "WARN",
            "SELECT count(*) FROM silver_meta_municipio WHERE taxa_alfabetizacao IS NULL",
        )
        reconciliation = float(self.config["quality"]["reconciliation_warning_pp"])
        add(
            "silver",
            "divergencia_microdados_resultado_oficial",
            "WARN",
            f"""
            SELECT count(*) FROM silver_municipio_integrado
            WHERE taxa_alfabetizacao_ponderada IS NOT NULL
              AND abs(taxa_alfabetizacao_ponderada - taxa_alfabetizacao) > {reconciliation}
            """,
            detail=f"Diferenca superior a {reconciliation} ponto percentual.",
        )
        add(
            "silver",
            "streaming_sem_timestamp",
            "ERROR",
            "SELECT count(*) FROM silver_streaming_alunos WHERE timestamp_ingestao IS NULL",
        )
        add(
            "silver",
            "streaming_eventos_duplicados",
            "ERROR",
            """
            SELECT count(*) FROM (
                SELECT ano, id_municipio, id_escola, id_aluno, count(*) n
                FROM silver_streaming_alunos
                GROUP BY ano, id_municipio, id_escola, id_aluno
                HAVING count(*) > 1
            )
            """,
        )

        errors = [check for check in checks if check.severity == "ERROR" and not check.passed]
        warnings = [check for check in checks if check.severity == "WARN" and not check.passed]
        status = "FAIL" if errors else ("PASS_WITH_WARNINGS" if warnings else "PASS")
        report = {
            "generated_at": utc_now(),
            "status": status,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "checks": [asdict(check) for check in checks],
        }
        output = self.artifacts / "quality"
        output.mkdir(parents=True, exist_ok=True)
        (output / "quality_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        header = "layer,name,severity,value,expected,passed,detail\n"
        rows = [
            ",".join(
                [
                    check.layer,
                    check.name,
                    check.severity,
                    str(check.value),
                    check.expected,
                    str(check.passed),
                    '"' + check.detail.replace('"', '""') + '"',
                ]
            )
            for check in checks
        ]
        (output / "quality_report.csv").write_text(
            header + "\n".join(rows) + "\n", encoding="utf-8"
        )
        if errors:
            names = ", ".join(check.name for check in errors)
            raise RuntimeError(f"Qualidade reprovada: {names}")
        return report

    @staticmethod
    def current_meta_sql(alias: str = "m") -> str:
        return f"""
            CASE {alias}.ano
                WHEN 2024 THEN {alias}.meta_alfabetizacao_2024
                WHEN 2025 THEN {alias}.meta_alfabetizacao_2025
                WHEN 2026 THEN {alias}.meta_alfabetizacao_2026
                WHEN 2027 THEN {alias}.meta_alfabetizacao_2027
                WHEN 2028 THEN {alias}.meta_alfabetizacao_2028
                WHEN 2029 THEN {alias}.meta_alfabetizacao_2029
                WHEN 2030 THEN {alias}.meta_alfabetizacao_2030
                ELSE NULL
            END
        """

    def build_gold(self) -> dict[str, int]:
        meta_municipio_atual = self.current_meta_sql("mt")
        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE gold_indicadores_municipio AS
            WITH base AS (
                SELECT
                    mt.ano,
                    mt.id_municipio,
                    mt.codigo_uf,
                    mt.rede_codigo,
                    mt.rede,
                    mt.taxa_alfabetizacao,
                    {meta_municipio_atual} AS meta_ano,
                    mt.meta_alfabetizacao_2030,
                    mt.nivel_alfabetizacao,
                    mt.percentual_participacao,
                    m.taxa_alfabetizacao AS taxa_resultado_agregado,
                    m.media_portugues,
                    a.total_alunos,
                    a.alunos_presentes,
                    a.taxa_presenca,
                    a.proficiencia_media AS proficiencia_media_microdados,
                    a.taxa_alfabetizacao_simples,
                    a.taxa_alfabetizacao_ponderada
                FROM silver_meta_municipio mt
                LEFT JOIN silver_municipio m
                  ON mt.ano = m.ano
                 AND mt.id_municipio = m.id_municipio
                 AND mt.rede_codigo = m.rede_codigo
                LEFT JOIN silver_alunos_municipio a
                  ON mt.ano = a.ano
                 AND mt.id_municipio = a.id_municipio
                 AND mt.rede_codigo = a.rede_codigo
                WHERE mt.ano IN (2023, 2024)
            ), metrics AS (
                SELECT
                    *,
                    taxa_alfabetizacao - meta_ano AS gap_meta_ano_pp,
                    taxa_alfabetizacao - meta_alfabetizacao_2030 AS gap_meta_2030_pp,
                    taxa_alfabetizacao_ponderada - taxa_alfabetizacao
                        AS diferenca_microdados_pp,
                    CASE
                        WHEN taxa_alfabetizacao IS NULL THEN 'SEM_RESULTADO'
                        WHEN meta_ano IS NULL THEN 'SEM_META'
                        WHEN taxa_alfabetizacao >= meta_ano THEN 'ATINGIU'
                        ELSE 'ABAIXO'
                    END AS status_meta,
                    lag(taxa_alfabetizacao) OVER (
                        PARTITION BY id_municipio ORDER BY ano
                    ) AS taxa_ano_anterior,
                    lag(ano) OVER (
                        PARTITION BY id_municipio ORDER BY ano
                    ) AS ano_anterior,
                    rank() OVER (
                        PARTITION BY ano ORDER BY taxa_alfabetizacao DESC NULLS LAST
                    ) AS ranking_brasil,
                    rank() OVER (
                        PARTITION BY ano, codigo_uf ORDER BY taxa_alfabetizacao DESC NULLS LAST
                    ) AS ranking_uf
                FROM base
            )
            SELECT
                *,
                taxa_alfabetizacao - taxa_ano_anterior AS variacao_anual_pp,
                CASE
                    WHEN taxa_ano_anterior IS NULL THEN 'SEM_COMPARACAO'
                    WHEN taxa_alfabetizacao > taxa_ano_anterior THEN 'MELHORA'
                    WHEN taxa_alfabetizacao < taxa_ano_anterior THEN 'PIORA'
                    ELSE 'ESTAVEL'
                END AS tendencia
            FROM metrics
            """
        )

        meta_uf_atual = self.current_meta_sql("mt")
        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE gold_indicadores_uf AS
            WITH base AS (
                SELECT
                    mt.ano,
                    mt.sigla_uf,
                    mt.rede_codigo,
                    mt.rede,
                    mt.taxa_alfabetizacao,
                    {meta_uf_atual} AS meta_ano,
                    mt.meta_alfabetizacao_2030,
                    mt.percentual_participacao,
                    u.taxa_alfabetizacao AS taxa_resultado_agregado,
                    u.media_portugues
                FROM silver_meta_uf mt
                LEFT JOIN silver_uf u
                  ON mt.ano = u.ano
                 AND mt.sigla_uf = u.sigla_uf
                 AND mt.rede_codigo = u.rede_codigo
                WHERE mt.ano IN (2023, 2024)
            ), metrics AS (
                SELECT
                    *,
                    taxa_alfabetizacao - meta_ano AS gap_meta_ano_pp,
                    taxa_alfabetizacao - meta_alfabetizacao_2030 AS gap_meta_2030_pp,
                    CASE
                        WHEN taxa_alfabetizacao IS NULL THEN 'SEM_RESULTADO'
                        WHEN meta_ano IS NULL THEN 'SEM_META'
                        WHEN taxa_alfabetizacao >= meta_ano THEN 'ATINGIU'
                        ELSE 'ABAIXO'
                    END AS status_meta,
                    lag(taxa_alfabetizacao) OVER (
                        PARTITION BY sigla_uf ORDER BY ano
                    ) AS taxa_ano_anterior,
                    rank() OVER (
                        PARTITION BY ano ORDER BY taxa_alfabetizacao DESC NULLS LAST
                    ) AS ranking_ano
                FROM base
            )
            SELECT
                *,
                taxa_alfabetizacao - taxa_ano_anterior AS variacao_anual_pp
            FROM metrics
            """
        )

        meta_brasil_atual = self.current_meta_sql("b")
        self.con.execute(
            f"""
            CREATE OR REPLACE TABLE gold_indicadores_brasil AS
            WITH base AS (
                SELECT
                    b.*,
                    {meta_brasil_atual} AS meta_ano,
                    lag(taxa_alfabetizacao) OVER (ORDER BY ano) AS taxa_ano_anterior
                FROM silver_brasil_integrado b
                WHERE ano IN (2023, 2024)
            )
            SELECT
                ano,
                rede_codigo,
                rede,
                taxa_alfabetizacao,
                meta_ano,
                taxa_alfabetizacao - meta_ano AS gap_meta_ano_pp,
                meta_alfabetizacao_2030,
                taxa_alfabetizacao - meta_alfabetizacao_2030 AS gap_meta_2030_pp,
                CASE
                    WHEN taxa_alfabetizacao IS NULL THEN 'SEM_RESULTADO'
                    WHEN meta_ano IS NULL THEN 'SEM_META'
                    WHEN taxa_alfabetizacao >= meta_ano THEN 'ATINGIU'
                    ELSE 'ABAIXO'
                END AS status_meta,
                percentual_participacao,
                taxa_ano_anterior,
                taxa_alfabetizacao - taxa_ano_anterior AS variacao_anual_pp
            FROM base
            """
        )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE gold_evolucao_municipio AS
            SELECT
                ano,
                id_municipio,
                codigo_uf,
                taxa_alfabetizacao,
                ano_anterior,
                taxa_ano_anterior,
                variacao_anual_pp,
                tendencia
            FROM gold_indicadores_municipio
            """
        )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE gold_ranking_municipio AS
            SELECT
                ano,
                id_municipio,
                codigo_uf,
                taxa_alfabetizacao,
                meta_ano,
                gap_meta_ano_pp,
                status_meta,
                ranking_brasil,
                ranking_uf,
                percent_rank() OVER (
                    PARTITION BY ano ORDER BY taxa_alfabetizacao NULLS FIRST
                ) AS percentil_brasil
            FROM gold_indicadores_municipio
            """
        )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE gold_resumo_municipio_uf AS
            SELECT
                ano,
                codigo_uf,
                count(*) AS municipios_com_resultado,
                avg(taxa_alfabetizacao) AS taxa_media_municipios,
                median(taxa_alfabetizacao) AS taxa_mediana_municipios,
                quantile_cont(taxa_alfabetizacao, 0.25) AS taxa_p25,
                quantile_cont(taxa_alfabetizacao, 0.75) AS taxa_p75,
                100.0 * avg(CASE WHEN status_meta = 'ATINGIU' THEN 1 ELSE 0 END)
                    FILTER (WHERE meta_ano IS NOT NULL) AS percentual_municipios_meta
            FROM gold_indicadores_municipio
            WHERE taxa_alfabetizacao IS NOT NULL
            GROUP BY ano, codigo_uf
            """
        )

        meta_tables = {
            "municipio": (
                "silver_meta_municipio",
                "id_municipio, codigo_uf, rede_codigo, rede",
            ),
            "uf": ("silver_meta_uf", "sigla_uf, rede_codigo, rede"),
            "brasil": ("silver_meta_brasil", "rede_codigo, rede"),
        }
        for suffix, (source, dimensions) in meta_tables.items():
            unions = []
            for year in range(2024, 2031):
                unions.append(
                    f"""
                    SELECT ano AS ano_avaliacao, {dimensions}, {year} AS ano_meta,
                           meta_alfabetizacao_{year} AS meta_alfabetizacao
                    FROM {source}
                    WHERE ano IN (2023, 2024)
                    """
                )
            self.con.execute(
                f"CREATE OR REPLACE TABLE gold_metas_{suffix}_long AS "
                + " UNION ALL ".join(unions)
            )

        self.con.execute(
            """
            CREATE OR REPLACE TABLE gold_qualidade_cobertura AS
            SELECT 'alunos' AS entidade, count(*) AS registros,
                   count(*) FILTER (WHERE id_municipio IS NULL) AS chaves_nulas
            FROM silver_alunos
            UNION ALL
            SELECT 'municipio', count(*),
                   count(*) FILTER (WHERE id_municipio IS NULL)
            FROM silver_municipio
            UNION ALL
            SELECT 'uf', count(*), count(*) FILTER (WHERE sigla_uf IS NULL)
            FROM silver_uf
            UNION ALL
            SELECT 'meta_municipio', count(*),
                   count(*) FILTER (WHERE id_municipio IS NULL)
            FROM silver_meta_municipio
            UNION ALL
            SELECT 'meta_uf', count(*), count(*) FILTER (WHERE sigla_uf IS NULL)
            FROM silver_meta_uf
            UNION ALL
            SELECT 'meta_brasil', count(*), count(*) FILTER (WHERE ano IS NULL)
            FROM silver_meta_brasil
            """
        )

        gold_tables = [
            "gold_indicadores_municipio",
            "gold_indicadores_uf",
            "gold_indicadores_brasil",
            "gold_evolucao_municipio",
            "gold_ranking_municipio",
            "gold_resumo_municipio_uf",
            "gold_metas_municipio_long",
            "gold_metas_uf_long",
            "gold_metas_brasil_long",
            "gold_qualidade_cobertura",
        ]
        counts = {table: self.table_count(table) for table in gold_tables}
        for table in gold_tables:
            self.export_partitioned(table, self.gold)
        self.write_run_manifest("gold", counts)
        return counts

    def write_run_manifest(self, layer: str, counts: dict[str, int]) -> None:
        profile_path = self.artifacts / "profile" / "bronze_profile.json"
        fingerprint = None
        if profile_path.exists():
            fingerprint = json.loads(profile_path.read_text(encoding="utf-8")).get(
                "source_fingerprint"
            )
        output = self.artifacts / "runs"
        output.mkdir(parents=True, exist_ok=True)
        manifest = {
            "generated_at": utc_now(),
            "layer": layer,
            "source_fingerprint": fingerprint,
            "tables": counts,
        }
        (output / f"{layer}_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def run_all(self) -> dict[str, Any]:
        profile = self.profile()
        silver = self.build_silver()
        quality = self.run_quality()
        gold = self.build_gold()
        return {
            "source_fingerprint": profile["source_fingerprint"],
            "silver": silver,
            "quality": quality["status"],
            "gold": gold,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline de alfabetizacao")
    parser.add_argument(
        "command",
        choices=["profile", "silver", "quality", "gold", "all"],
        help="Etapa a executar",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline = LiteracyPipeline()
    try:
        if args.command == "profile":
            result = pipeline.profile()
        elif args.command == "silver":
            result = pipeline.build_silver()
        elif args.command == "quality":
            result = pipeline.run_quality()
        elif args.command == "gold":
            result = pipeline.build_gold()
        else:
            result = pipeline.run_all()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
