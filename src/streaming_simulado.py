from pathlib import Path
from datetime import datetime, timezone
import time

import pandas as pd


# Diretórios do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
STREAMING_DIR = BASE_DIR / "data" / "bronze" / "streaming"


# Configurações da simulação
ARQUIVO_ALUNOS = RAW_DIR / "alunos.csv"
TAMANHO_LOTE = 100
TOTAL_EVENTOS = 500
INTERVALO_SEGUNDOS = 2


def simular_streaming():
    """Simula a chegada de eventos de alunos em pequenos lotes."""

    print("=" * 60)
    print("SIMULAÇÃO DE STREAMING")
    print("=" * 60)

    print(f"\nArquivo fonte: {ARQUIVO_ALUNOS}")
    print(f"Tamanho do lote: {TAMANHO_LOTE}")
    print(f"Total de eventos: {TOTAL_EVENTOS}")
    print(f"Intervalo entre lotes: {INTERVALO_SEGUNDOS} segundos")

    # Lê somente a quantidade necessária para a simulação
    df = pd.read_csv(
        ARQUIVO_ALUNOS,
        nrows=TOTAL_EVENTOS,
    )

    print(f"\nRegistros carregados: {len(df):,}")

    total_processados = 0

    for inicio in range(0, len(df), TAMANHO_LOTE):
        lote = df.iloc[inicio:inicio + TAMANHO_LOTE].copy()

        # Timestamp simulando o momento de chegada do evento
        lote["timestamp_ingestao"] = datetime.now(
            timezone.utc
        ).isoformat()

        numero_lote = (inicio // TAMANHO_LOTE) + 1

        print(
            f"\nEvento/lote {numero_lote:03d} recebido: "
            f"{len(lote):,} registros"
        )

        # Particionamento por ano
        for ano in lote["ano"].dropna().unique():

            lote_ano = lote[lote["ano"] == ano]

            pasta_saida = (
                STREAMING_DIR
                / f"ano={int(ano)}"
            )

            pasta_saida.mkdir(
                parents=True,
                exist_ok=True,
            )

            caminho_saida = (
                pasta_saida
                / f"lote-{numero_lote:03d}.parquet"
            )

            lote_ano.to_parquet(
                caminho_saida,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

            print(
                f"  Ano {int(ano)}: "
                f"{len(lote_ano):,} registros → "
                f"{caminho_saida}"
            )

        total_processados += len(lote)

        print(
            f"Total processado: "
            f"{total_processados:,}/{len(df):,}"
        )

        # Simula o intervalo entre eventos
        if total_processados < len(df):
            time.sleep(INTERVALO_SEGUNDOS)

    print("\n" + "=" * 60)
    print("SIMULAÇÃO DE STREAMING CONCLUÍDA")
    print("=" * 60)


if __name__ == "__main__":
    simular_streaming()