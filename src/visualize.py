from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
ROOT = Path(__file__).resolve().parent.parent
MPL_CACHE = ROOT / "artifacts" / "matplotlib"
MPL_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CACHE))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


DATABASE = ROOT / "artifacts" / "pipeline.duckdb"
OUTPUT = ROOT / "docs" / "evidencias" / "fase-3"

COLORS = {
    "resultado": "#1f77b4",
    "meta": "#f59e0b",
    "atingiu": "#2a9d8f",
    "abaixo": "#e76f51",
    "sem_meta": "#8d99ae",
}


def save_figure(fig: plt.Figure, name: str) -> None:
    fig.tight_layout()
    fig.savefig(OUTPUT / name, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def generate_visualizations() -> dict[str, int | float | str]:
    if not DATABASE.exists():
        raise FileNotFoundError("Execute `python -m src.pipeline all` antes das visualizacoes.")

    OUTPUT.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", context="talk")
    con = duckdb.connect(str(DATABASE), read_only=True)
    try:
        uf = con.execute(
            """
            SELECT sigla_uf, taxa_alfabetizacao, meta_ano, status_meta
            FROM gold_indicadores_uf
            WHERE ano = 2024 AND taxa_alfabetizacao IS NOT NULL
            ORDER BY taxa_alfabetizacao
            """
        ).fetchdf()
        brasil = con.execute(
            "SELECT * FROM gold_indicadores_brasil ORDER BY ano"
        ).fetchdf()
        municipios = con.execute(
            """
            SELECT id_municipio, codigo_uf, taxa_alfabetizacao,
                   meta_ano, gap_meta_ano_pp, percentual_participacao, status_meta
            FROM gold_indicadores_municipio
            WHERE ano = 2024
            """
        ).fetchdf()

        fig, ax = plt.subplots(figsize=(12, 11))
        positions = range(len(uf))
        ax.barh(
            [position - 0.2 for position in positions],
            uf["taxa_alfabetizacao"],
            height=0.38,
            color=COLORS["resultado"],
            label="Resultado 2024",
        )
        ax.barh(
            [position + 0.2 for position in positions],
            uf["meta_ano"],
            height=0.38,
            color=COLORS["meta"],
            label="Meta 2024",
        )
        ax.set_yticks(list(positions), uf["sigla_uf"])
        ax.set_xlim(0, 100)
        ax.set_xlabel("Percentual de estudantes alfabetizados")
        ax.set_ylabel("UF")
        ax.set_title("Alfabetização por UF: resultado versus meta em 2024")
        ax.legend(loc="lower right")
        save_figure(fig, "01-uf-resultado-versus-meta-2024.png")

        fig, ax = plt.subplots(figsize=(9, 6))
        bars = ax.bar(
            brasil["ano"].astype(str),
            brasil["taxa_alfabetizacao"],
            color=[COLORS["resultado"], COLORS["atingiu"]],
            width=0.55,
        )
        meta_2024 = float(brasil.loc[brasil["ano"] == 2024, "meta_ano"].iloc[0])
        ax.axhline(meta_2024, color=COLORS["meta"], linestyle="--", linewidth=2.5)
        ax.text(-0.38, meta_2024 + 1.2, f"Meta 2024: {meta_2024:.1f}%", color=COLORS["meta"])
        ax.bar_label(bars, fmt="%.1f%%", padding=-36, color="white", fontweight="bold")
        ax.set_ylim(0, 75)
        ax.set_xlabel("Ano")
        ax.set_ylabel("Taxa de alfabetização (%)")
        ax.set_title("Brasil avancou 3,3 p.p., mas ficou abaixo da meta de 2024")
        save_figure(fig, "02-brasil-evolucao-2023-2024.png")

        fig, ax = plt.subplots(figsize=(11, 6))
        sns.histplot(
            data=municipios,
            x="taxa_alfabetizacao",
            bins=30,
            color=COLORS["resultado"],
            edgecolor="white",
            ax=ax,
        )
        ax.axvline(
            municipios["taxa_alfabetizacao"].median(),
            color="#264653",
            linestyle="--",
            label=f"Mediana: {municipios['taxa_alfabetizacao'].median():.1f}%",
        )
        ax.set_xlabel("Taxa municipal de alfabetização (%)")
        ax.set_ylabel("Quantidade de municípios")
        ax.set_title("Distribuicao das taxas municipais em 2024")
        ax.legend()
        save_figure(fig, "03-distribuicao-municipios-2024.png")

        scatter = municipios.dropna(
            subset=["percentual_participacao", "taxa_alfabetizacao"]
        ).copy()
        palette = {
            "ATINGIU": COLORS["atingiu"],
            "ABAIXO": COLORS["abaixo"],
            "SEM_META": COLORS["sem_meta"],
            "SEM_RESULTADO": "#6c757d",
        }
        fig, ax = plt.subplots(figsize=(11, 7))
        sns.scatterplot(
            data=scatter,
            x="percentual_participacao",
            y="taxa_alfabetizacao",
            hue="status_meta",
            palette=palette,
            alpha=0.55,
            s=35,
            linewidth=0,
            ax=ax,
        )
        ax.set_xlabel("Participação na avaliação (%)")
        ax.set_ylabel("Taxa de alfabetização (%)")
        ax.set_title("Participação e resultado municipal em 2024")
        ax.legend(title="Situacao da meta", loc="lower right")
        save_figure(fig, "04-participacao-versus-alfabetizacao-2024.png")

        deficits = (
            municipios.loc[municipios["status_meta"] == "ABAIXO"]
            .nsmallest(15, "gap_meta_ano_pp")
            .sort_values("gap_meta_ano_pp", ascending=False)
        )
        fig, ax = plt.subplots(figsize=(10, 7))
        sns.barplot(
            data=deficits,
            x="gap_meta_ano_pp",
            y="id_municipio",
            color=COLORS["abaixo"],
            ax=ax,
        )
        ax.axvline(0, color="#264653", linewidth=1)
        ax.set_xlabel("Distancia para a meta (pontos percentuais)")
        ax.set_ylabel("ID do município")
        ax.set_title("Quinze maiores déficits municipais em 2024")
        save_figure(fig, "05-maiores-deficits-municipais-2024.png")

        uf.to_csv(OUTPUT / "resumo-ufs-2024.csv", index=False, encoding="utf-8")
        brasil.to_csv(OUTPUT / "resumo-brasil.csv", index=False, encoding="utf-8")
        deficits.to_csv(
            OUTPUT / "top-15-deficits-municipais-2024.csv",
            index=False,
            encoding="utf-8",
        )

        summary = {
            "generated_from": "artifacts/pipeline.duckdb",
            "municipios_2024": int(len(municipios)),
            "municipios_atingiram_meta_2024": int(
                (municipios["status_meta"] == "ATINGIU").sum()
            ),
            "municipios_abaixo_meta_2024": int(
                (municipios["status_meta"] == "ABAIXO").sum()
            ),
            "brasil_taxa_2024": float(
                brasil.loc[brasil["ano"] == 2024, "taxa_alfabetizacao"].iloc[0]
            ),
            "brasil_meta_2024": meta_2024,
            "brasil_variacao_2024_pp": float(
                brasil.loc[brasil["ano"] == 2024, "variacao_anual_pp"].iloc[0]
            ),
        }
        (OUTPUT / "resumo-indicadores.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return summary
    finally:
        con.close()


if __name__ == "__main__":
    print(json.dumps(generate_visualizations(), ensure_ascii=False, indent=2))
