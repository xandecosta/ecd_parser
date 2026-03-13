import os
import pandas as pd
import logging
from typing import List, Set, Optional, Dict
from core.telemetry import monitor_task, TelemetryCollector

logger = logging.getLogger(__name__)


class ECDConsolidator:
    """
    Consolida múltiplos outputs de períodos individuais em arquivos únicos de forma DINÂMICA.
    """

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.consolidated_dir = os.path.join(output_dir, "consolidado")
        self.telemetry: Optional[TelemetryCollector] = None
        self.current_ecd_id = "GLOBAL"

        # Filtros de tabelas que devem ser exportadas para CSV amigável ao Excel
        self._excel_eligible_prefixes: Set[str] = {
            "01_", "02_", "03_", "04_", "07_",
            "BP", "DRE", "Balancete", "Scorecard",
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
        logger.info("Iniciando consolidação dinâmica dos relatórios...")
        self._preparar_pasta()

        # 1. Localiza subpastas de períodos
        pastas_ignorar = {"consolidado", "file_logs", "test_output"}
        try:
            entries = os.listdir(self.output_dir)
        except OSError as e:
            logger.error(f"Erro ao acessar diretório de saída: {e}")
            return

        subpastas = [
            os.path.join(self.output_dir, f)
            for f in entries
            if os.path.isdir(os.path.join(self.output_dir, f))
            and f not in pastas_ignorar
        ]

        if not subpastas:
            logger.warning("Nenhuma pasta de período encontrada para consolidar.")
            return

        # 2. Mapeamento dinâmico (Tabela -> Lista de Arquivos) para evitar varredura dupla
        mapeamento_tabelas: Dict[str, List[str]] = {}
        for pasta in subpastas:
            periodo = os.path.basename(pasta)
            try:
                for f in os.listdir(pasta):
                    if f.endswith(".parquet"):
                        # Extrai nome da tabela (remove prefixo de data se houver)
                        partes = f.split("_", 1)
                        nome_tabela = partes[1].replace(".parquet", "") if len(partes) > 1 else f.replace(".parquet", "")
                        
                        if nome_tabela not in mapeamento_tabelas:
                            mapeamento_tabelas[nome_tabela] = []
                        mapeamento_tabelas[nome_tabela].append(os.path.join(pasta, f))
            except OSError:
                continue

        # 3. Processamento por Tabela
        for tabela, caminhos in sorted(mapeamento_tabelas.items()):
            logger.info(f"      Processando: {tabela} ({len(caminhos)} arquivos)")
            dfs: List[pd.DataFrame] = []

            for path in caminhos:
                try:
                    periodo = os.path.basename(os.path.dirname(path))
                    df = pd.read_parquet(path)
                    if not df.empty:
                        if "ORIGEM_PERIODO" not in df.columns:
                            df.insert(0, "ORIGEM_PERIODO", periodo)
                        dfs.append(df)
                except Exception as e:
                    logger.error(f"Erro ao ler {path}: {e}")

            if not dfs:
                continue

            # Concatenação e Salvamento
            df_final = pd.concat(dfs, ignore_index=True)
            del dfs # Limpeza explícita para ajudar o GC em tabelas grandes

            # A. Parquet Consolidado (Cru)
            parquet_path = os.path.join(self.consolidated_dir, f"CONSOLIDADO_{tabela}.parquet")
            df_final.to_parquet(parquet_path, index=False, engine="pyarrow")

            # B. CSV Consolidado (Apenas se elegível)
            if any(tabela.startswith(pre) or pre in tabela for pre in self._excel_eligible_prefixes):
                csv_path = os.path.join(self.consolidated_dir, f"CONSOLIDADO_{tabela}.csv")
                
                # Padrão Ouro: Compatibilidade Excel PT-BR (BOM + sep=';')
                # Mantemos o dado como float (ponto decimal) para não quebrar leituras futuras
                df_final.to_csv(
                    csv_path,
                    index=False,
                    sep=";",
                    decimal=",",
                    encoding="utf-8-sig"
                )
                logger.info(f"      [CSV] Gerado: {os.path.basename(csv_path)}")

        logger.info(f"Consolidação finalizada com sucesso em: {self.consolidated_dir}")


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
