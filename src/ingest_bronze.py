from pathlib import Path
import pandas as pd


# Diretórios do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"


# Arquivos que serão processados na camada Bronze
ARQUIVOS = {
    "uf": "br_inep_avaliacao_alfabetizacao_uf.csv",
    "municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "meta_brasil": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    "meta_municipio": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
    "meta_uf": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "alunos": "alunos.csv",
}


def converter_para_parquet(nome_tabela, nome_arquivo):
    """Converte um CSV da camada raw para Parquet na camada bronze."""

    caminho_csv = RAW_DIR / nome_arquivo
    diretorio_saida = BRONZE_DIR / nome_tabela

    diretorio_saida.mkdir(parents=True, exist_ok=True)

    print(f"\nProcessando: {nome_arquivo}")

    df = pd.read_csv(caminho_csv)

    print(f"Linhas: {len(df):,}")
    print(f"Colunas: {len(df.columns)}")

    # Verifica se existe coluna de ano para particionamento
    if "ano" in df.columns:
        anos = df["ano"].dropna().unique()

        for ano in anos:
            df_ano = df[df["ano"] == ano]

            pasta_ano = diretorio_saida / f"ano={int(ano)}"
            pasta_ano.mkdir(parents=True, exist_ok=True)

            caminho_parquet = pasta_ano / "part-000.parquet"

            df_ano.to_parquet(
                caminho_parquet,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

            print(
                f"  Ano {int(ano)}: "
                f"{len(df_ano):,} linhas → {caminho_parquet}"
            )

    else:
        caminho_parquet = diretorio_saida / "part-000.parquet"

        df.to_parquet(
            caminho_parquet,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        print(f"  Arquivo criado: {caminho_parquet}")


def main():
    print("=" * 60)
    print("INGESTÃO - CAMADA BRONZE")
    print("=" * 60)

    for nome_tabela, nome_arquivo in ARQUIVOS.items():
        converter_para_parquet(nome_tabela, nome_arquivo)

    print("\n" + "=" * 60)
    print("INGESTÃO CONCLUÍDA")
    print("=" * 60)


if __name__ == "__main__":
    main()