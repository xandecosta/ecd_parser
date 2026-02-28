import os
import time
import pandas as pd
import logging
from typing import List, Set, Optional
from exporters.exporter import ECDExporter
from core.telemetry import monitor_task, TelemetryCollector


class ECDConsolidator:
    """
    Consolida múltiplos outputs de períodos individuais em arquivos únicos de forma DINÂMICA.
    """

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.consolidated_dir = os.path.join(output_dir, "consolidado")
        self.telemetry: Optional[TelemetryCollector] = None
        self.current_ecd_id = "GLOBAL"

        # Filtros de tabelas que devem ser exportadas para Excel (Consolidado)
        self._excel_eligible_prefixes: Set[str] = {
            "01_",
            "02_",
            "03_",
            "04_",
            "07_",
            "BP",
            "DRE",
            "Balancete",
            "Scorecard",
        }

    @monitor_task("ECDConsolidator", "_preparar_pasta")
    def _preparar_pasta(self) -> None:
        os.makedirs(self.consolidated_dir, exist_ok=True)

    @monitor_task("ECDConsolidator", "_descobrir_tabelas")
    def _descobrir_tabelas(self, subpastas: List[str]) -> Set[str]:
        """Varre as subpastas para descobrir quais nomes de tabelas existem no disco."""
        tabelas_encontradas: Set[str] = set()
        for pasta in subpastas:
            for f in os.listdir(pasta):
                if f.endswith(".parquet"):
                    # O padrão é YYYYMMDD_NOME_TABELA.parquet
                    # Tentamos extrair a parte da tabela após o primeiro underscore (YYYYMMDD_)
                    partes = f.split("_", 1)
                    if len(partes) > 1:
                        nome_tabela = partes[1].replace(".parquet", "")
                        tabelas_encontradas.add(nome_tabela)
        return tabelas_encontradas

    @monitor_task("ECDConsolidator", "consolidar")
    def consolidar(self) -> None:
        """
        Percorre as pastas de saída e agrupa os dados por tabela de forma dinâmica.
        """
        print("\n>>> INICIANDO CONSOLIDAÇÃO DINÂMICA DOS RELATÓRIOS...")
        self._preparar_pasta()

        # 1. Localiza subpastas de períodos (Exclui a pasta 'consolidado' e 'file_logs')
        pastas_ignorar = {"consolidado", "file_logs", "test_output"}
        subpastas = [
            os.path.join(self.output_dir, f)
            for f in os.listdir(self.output_dir)
            if os.path.isdir(os.path.join(self.output_dir, f))
            and f not in pastas_ignorar
        ]

        if not subpastas:
            logging.warning("Nenhuma pasta de período encontrada para consolidar.")
            return

        # 2. Descoberta dinâmica de tabelas
        tabelas = sorted(list(self._descobrir_tabelas(subpastas)))
        logging.info(f"Tabelas identificadas para consolidação: {tabelas}")

        # --- TELEMETRIA: Loop de Consolidação ---
        start_loop = time.time()
        for tabela in tabelas:
            dfs: List[pd.DataFrame] = []
            print(f"      Processando: {tabela}")

            for pasta in subpastas:
                periodo = os.path.basename(pasta)
                # Tenta localizar o arquivo seguindo o padrão PERIOD_TABELA.parquet ou apenas TABELA.parquet
                arquivos_possiveis = [
                    os.path.join(pasta, f"{periodo}_{tabela}.parquet"),
                    os.path.join(pasta, f"{tabela}.parquet"),
                ]

                for path in arquivos_possiveis:
                    if os.path.exists(path):
                        try:
                            # VETORIZAÇÃO: read_parquet é extremamente eficiente
                            df = pd.read_parquet(path)
                            if not df.empty:
                                # Adiciona coluna de origem para auditoria no consolidado
                                if "ORIGEM_PERIODO" not in df.columns:
                                    df.insert(0, "ORIGEM_PERIODO", periodo)
                                dfs.append(df)
                            break  # Encontrou o arquivo nesta pasta, para de procurar
                        except Exception as e:
                            logging.error(f"Erro ao ler {path}: {e}")

            if dfs:
                # 3. Concatenação
                df_final = pd.concat(dfs, ignore_index=True)

                # --- TELEMETRIA: Exportação Consolidado ---
                start_export = time.time()

                # 3. Salvamento: Parquet (Sempre)
                parquet_path = os.path.join(
                    self.consolidated_dir, f"CONSOLIDADO_{tabela}.parquet"
                )
                df_final.to_parquet(
                    parquet_path, index=False, engine="pyarrow"
                )  # Added engine="pyarrow" for consistency

                # 4. Salvamento: CSV (Universal e sem limite de linhas)
                if any(tabela.startswith(pre) for pre in self._excel_eligible_prefixes):
                    csv_path = os.path.join(
                        self.consolidated_dir, f"CONSOLIDADO_{tabela}.csv"
                    )
                    # Aplica formatação PT-BR antes de salvar
                    df_csv = ECDExporter.aplicar_formatacao_regional(df_final)
                    df_csv.to_csv(
                        csv_path,
                        index=False,
                        sep=";",
                        encoding="utf-8-sig",
                        decimal=",",
                    )
                    logging.info(f"      [CSV] Gerado: {os.path.basename(csv_path)}")

                if self.telemetry:
                    self.telemetry.record_global(
                        "ECDConsolidator",
                        "Exportação Consolidado",
                        time.time() - start_export,
                    )
            else:
                logging.debug(f"Sem dados para a tabela {tabela}")

        if self.telemetry:
            self.telemetry.record_global(
                "ECDConsolidator", "Loop de Consolidação", time.time() - start_loop
            )

        print(f"      [OK] Consolidação finalizada em: {self.consolidated_dir}")


if __name__ == "__main__":
    # Setup básico de logging para execução standalone
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(base_dir, "data", "output")

    if os.path.exists(output_path):
        consolidator = ECDConsolidator(output_path)
        consolidator.consolidar()
    else:
        print(f"Diretório de saída não encontrado: {output_path}")
