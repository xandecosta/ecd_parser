import pandas as pd
import os
import time
import logging
from datetime import datetime
from typing import Dict, Optional
from exporters.formatting import apply_region_format
from core.telemetry import monitor_task, TelemetryCollector


class ECDExporter:
    def __init__(self, path_saida: str):
        """
        Inicializa o exportador.
        Args:
            path_saida: Caminho base onde os arquivos serão salvos (ex: output/20211231).
        """
        self.path_saida = path_saida
        self.output_base = os.path.dirname(path_saida)
        self.id_folder = os.path.basename(path_saida)
        os.makedirs(self.path_saida, exist_ok=True)
        self.base_log_dir = os.path.join(self.output_base, "file_logs")
        os.makedirs(self.base_log_dir, exist_ok=True)
        self.telemetry: Optional[TelemetryCollector] = None
        self.current_ecd_id = ""

    @staticmethod
    def aplicar_formatacao_regional(df: pd.DataFrame) -> pd.DataFrame:
        """Proxy para o utilitário centralizado (Mantém compatibilidade)."""
        return apply_region_format(df)

    @monitor_task("ECDExporter", "exportar_lote")
    def exportar_lote(
        self,
        dicionario_dfs: Dict[str, pd.DataFrame],
        nome_base: str,
        prefixo: str = "",
        itens_adicionais: Optional[list] = None,
        tempo_inicio: Optional[float] = None,
    ) -> None:
        """
        Exporta DataFrames para Parquet e CSV e centraliza logs.
        """
        if itens_adicionais is None:
            itens_adicionais = []

        start_export = time.time()
        log_gerados = []

        for nome_tabela, df in dicionario_dfs.items():
            if df is None or df.empty:
                continue

            nome_final = f"{prefixo}_{nome_tabela}" if prefixo else nome_tabela

            # 1. Exportação para PARQUET (sempre mantida para reprocessamento)
            caminho_parquet = os.path.join(self.path_saida, f"{nome_final}.parquet")
            df.to_parquet(caminho_parquet, index=False, engine="pyarrow")
            log_gerados.append(f"PARQUET: {os.path.basename(caminho_parquet)}")

            # 2. Exportação para CSV (substitui o antigo XLSX)
            termos_csv = [
                "BP",
                "DRE",
                "Balancete",
                "Plano_Contas",
                "Lancamentos_Contabeis",
                "Saldos_Mensais",
                "baseRFB",
            ]
            if any(term in nome_tabela for term in termos_csv):
                caminho_csv = os.path.join(self.path_saida, f"{nome_final}.csv")
                # Compatibilidade Excel PT-BR: Usando sep=";" e decimal="," com utf-8-sig
                df.to_csv(
                    caminho_csv,
                    index=False,
                    sep=";",
                    decimal=",",
                    encoding="utf-8-sig",
                )
                log_gerados.append(f"CSV:     {os.path.basename(caminho_csv)}")

        end_export = time.time()
        duracao = end_export - (tempo_inicio if tempo_inicio else start_export)

        # Registra métricas na telemetria manualmente
        if self.telemetry and self.current_ecd_id:
            self.telemetry.record_metric(
                self.current_ecd_id,
                "ECDExporter",
                "exportar_lote",
                end_export - start_export,
            )

        self._atualizar_log_centralizado(
            log_gerados + itens_adicionais,
            tempo_inicio=tempo_inicio if tempo_inicio else start_export,
            tempo_fim=end_export,
            duracao=duracao,
        )
        logging.info(f"Exportação concluída: {self.id_folder}")

    def _atualizar_log_centralizado(
        self,
        lista_arquivos: list,
        tempo_inicio: float,
        tempo_fim: float,
        duracao: float,
    ) -> None:
        """
        Salva o log na pasta 'file_logs' com o padrão ECD_PERIODO.log (Persistente/Append).
        """
        log_dir = self.base_log_dir

        nome_log = f"ECD_{self.id_folder}.log"
        caminho_log = os.path.join(log_dir, nome_log)

        ts_inicio = datetime.fromtimestamp(tempo_inicio).strftime("%Y-%m-%d %H:%M:%S")
        ts_fim = datetime.fromtimestamp(tempo_fim).strftime("%Y-%m-%d %H:%M:%S")

        # Modo 'a' para ser cumulativo (Append)
        with open(caminho_log, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 60 + "\n")
            f.write(f"ID PROCESSAMENTO: {self.id_folder}\n")
            f.write(f"PASTA DE DESTINO: {os.path.abspath(self.path_saida)}\n")
            f.write(f"INÍCIO:           {ts_inicio}\n")
            f.write(f"TÉRMINO:          {ts_fim}\n")
            f.write(f"DURAÇÃO:          {duracao:.2f} segundos\n")

            if self.telemetry and self.current_ecd_id in self.telemetry.data:
                f.write("-" * 60 + "\n")
                f.write("DETALHAMENTO DE PERFORMANCE:\n")
                metrics = self.telemetry.data[self.current_ecd_id]["metrics"]
                for comp, methods in metrics.items():
                    comp_total = sum(methods.values())
                    f.write(f"\n[{comp}] (Subtotal: {comp_total:.2f}s)\n")
                    for meth, dur in methods.items():
                        f.write(f"  - {meth.ljust(30)}: {dur:.2f}s\n")

            f.write("-" * 60 + "\n")
            f.write("ARQUIVOS GERADOS:\n")
            for item in lista_arquivos:
                f.write(f"{item}\n")
            f.write("=" * 60 + "\n")
