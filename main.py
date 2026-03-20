import os
import time
import glob
import logging
import warnings
import re
import shutil
from typing import Optional, cast, Any, Set, Dict
import pandas as pd
from core.reader_ecd import ECDReader
from core.processor import ECDProcessor
from core.auditor import ECDAuditor
from core.telemetry import TelemetryCollector
from exporters.exporter import ECDExporter
from exporters.consolidator import ECDConsolidator
from exporters.audit_exporter import AuditExporter
from intelligence.historical_mapper import HistoricalMapper
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing


import sys

if sys.platform == "win32":
    # Garante que o terminal aceite UTF-8 mesmo sem variável de ambiente
    if hasattr(sys.stdout, "reconfigure"):
        cast(Any, sys.stdout).reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        cast(Any, sys.stderr).reconfigure(encoding="utf-8")

# 1. Silenciar Avisos de Bibliotecas (Pandas, etc)
warnings.filterwarnings("ignore")

# Configuração de Logs Padrão Ouro
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("core.reader_ecd").setLevel(logging.WARNING)
logging.getLogger("core.processor").setLevel(logging.WARNING)


def processar_um_arquivo(
    caminho_arquivo: str,
    output_base: str,
    mapper: Optional[HistoricalMapper] = None,
    telemetry: Optional[TelemetryCollector] = None,
) -> Dict[str, Any]:
    """Executa o ciclo completo de processamento para um único arquivo ECD."""
    start_proc = time.time()
    nome_arquivo = os.path.basename(caminho_arquivo)
    nome_projeto = nome_arquivo.replace(".txt", "")

    try:
        reader = ECDReader(caminho_arquivo)

        # Extração de ID do Folder (Período)
        match = re.search(r"(\d{8})-(\d{8})-", nome_arquivo)
        id_folder_temp = match.group(1) if match else nome_projeto
        if match:
            match2 = re.search(r"\d{8}-(\d{8})-", nome_arquivo)
            id_folder_temp = match2.group(1) if match2 else id_folder_temp

        if telemetry:
            telemetry.start_ecd(id_folder_temp)
            reader.telemetry = telemetry
            reader.current_ecd_id = id_folder_temp

        # Processamento do Leitor
        registros = list(reader.processar_arquivo())
        if not registros:
            logging.warning(f"Arquivo vazio ou sem registros válidos: {nome_arquivo}")
            return {}

        id_folder = reader.periodo_ecd if reader.periodo_ecd else id_folder_temp
        cnpj_contribuinte = getattr(reader, "cnpj", "")

        # Re-sincronização de Telemetria
        if telemetry and id_folder != id_folder_temp:
            telemetry.data[id_folder] = telemetry.data.pop(id_folder_temp)
            reader.current_ecd_id = id_folder

        # --- PROCESSAMENTO ---
        processor = ECDProcessor(
            registros,
            cnpj=cnpj_contribuinte,
            layout_versao=reader.layout_versao or "",
            knowledge_base=mapper,
        )
        if telemetry:
            processor.telemetry = telemetry
            processor.current_ecd_id = id_folder

        df_plano = processor.processar_plano_contas()
        df_lancamentos = processor.processar_lancamentos(df_plano)
        dict_balancetes = processor.gerar_balancetes()
        dict_demos = processor.processar_demonstracoes()

        # --- AUDITORIA ---
        df_bal_mensal = dict_balancetes.get("03_Balancetes_Mensais", pd.DataFrame())
        auditor = ECDAuditor(
            df_diario=df_lancamentos,
            df_balancete=df_bal_mensal,
            df_plano=df_plano,
            df_naturezas=processor.blocos.get("dfECD_I050"),
            df_mapeamento=processor.blocos.get("dfECD_I051"),
        )
        if telemetry:
            auditor.telemetry = telemetry
            auditor.current_ecd_id = id_folder

        resultados_audit = auditor.executar_auditoria_completa()

        # --- EXPORTAÇÃO ---
        pasta_saida = os.path.join(output_base, id_folder)
        exporter = ECDExporter(pasta_saida)
        if telemetry:
            exporter.telemetry = telemetry
            exporter.current_ecd_id = id_folder

        itens_log = []
        try:
            audit_exporter = AuditExporter(pasta_saida)
            itens_log += audit_exporter.exportar_dashboard(
                resultados_audit, nome_projeto, prefixo=id_folder
            )
            itens_log += audit_exporter.exportar_detalhes_parquet(
                resultados_audit, prefixo=id_folder
            )
        except Exception as e:
            logging.error(f"Erro na exportação de auditoria ({id_folder}): {e}")

        tabelas = {
            "01_BP": dict_demos.get("BP"),
            "02_DRE": dict_demos.get("DRE"),
            "03_Balancetes_Mensais": df_bal_mensal,
            "04_Balancete_baseRFB": dict_balancetes.get("04_Balancete_baseRFB"),
            "05_Plano_Contas": df_plano,
            "06_Lancamentos_Contabeis": df_lancamentos,
        }

        exporter.exportar_lote(
            tabelas,
            nome_projeto,
            prefixo=id_folder,
            itens_adicionais=itens_log,
            tempo_inicio=start_proc,
        )

        if telemetry:
            telemetry.end_ecd(id_folder)

        logging.info(f"Sucesso: {id_folder}")
        return telemetry.data if telemetry else {}

    except Exception as e:
        logging.error(f"Falha ao processar {nome_arquivo}: {e}")
        return telemetry.data if telemetry else {}


def executar_pipeline_batch(telemetry: Optional[TelemetryCollector] = None):
    """
    Localiza todos os arquivos ECD e gerencia o processamento em lote.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(base_dir, "data", "input")
    output_dir = os.path.join(base_dir, "data", "output")

    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
        print(f"Pasta de entrada criada: {input_dir}. Adicione os arquivos .txt nela.")
        return

    arquivos = glob.glob(os.path.join(input_dir, "*.txt"))

    if not arquivos:
        print("Nenhum arquivo .txt encontrado na pasta data/input.")
        return

    logging.info("Limpando pasta de saída (mantendo logs e consolidado)...")
    pastas_preservar = {"file_logs", "consolidado"}
    for item in os.listdir(output_dir):
        if item in pastas_preservar:
            continue

        caminho_item = os.path.join(output_dir, item)
        try:
            if os.path.isdir(caminho_item):
                shutil.rmtree(caminho_item, ignore_errors=False)
            else:
                os.remove(caminho_item)
        except OSError as e:
            logging.warning(
                f"Não foi possível remover {item}: {e}. Certifique-se de que pastas/arquivos não estejam abertos."
            )

    # Garante que a pasta de log existe
    os.makedirs(os.path.join(output_dir, "file_logs"), exist_ok=True)

    print(f"Iniciando processamento de {len(arquivos)} arquivo(s)...")

    # --- PASSO 0: LEARNING PASS (Cross-Temporal) ---
    logging.info("Iniciando aprendizado histórico cirúrgico (RAM Eco Mode)...")
    mapper = HistoricalMapper()
    if telemetry:
        mapper.telemetry = telemetry
        mapper.current_ecd_id = "GLOBAL"

    intelligence_dir = os.path.join(base_dir, "data", "intelligence")
    os.makedirs(intelligence_dir, exist_ok=True)
    history_file = os.path.join(intelligence_dir, "history.json")

    if os.path.exists(history_file):
        logging.info(
            f"Carregando conhecimento prévio: {os.path.basename(history_file)}"
        )
        mapper.load_knowledge(history_file)

    for arquivo in arquivos:
        nome_arq = os.path.basename(arquivo)
        if nome_arq in mapper._processed_files:
            continue

        logging.info(f"Lendo estrutura: {nome_arq}")
        try:
            reader = ECDReader(arquivo)
            # APRENDIZADO CIRÚRGICO: Pede apenas Blocos 0, I e J (ignora K, L e os pesados lançamentos I200/I250)
            # Isso reduz consumo de RAM em até 95% para arquivos grandes
            regs_interesse = list(
                reader.processar_arquivo(blocos_selecionados=["0", "I", "J"])
            )
            if not regs_interesse:
                continue

            df_all = pd.DataFrame(regs_interesse)

            # Extração de Metadados RFB e Mapeamentos (Mesma lógica, agora sobre dados filtrados)
            cod_plan_ref = None
            df_0000 = df_all[df_all["REG"] == "0000"]
            if not df_0000.empty:
                df_0000_norm = df_0000.copy()
                df_0000_norm.columns = df_0000_norm.columns.str.replace(
                    "0000_", "", regex=False
                )
                cod_plan_ref = df_0000_norm.iloc[0].get("COD_PLAN_REF")

            df_i050 = df_all[df_all["REG"] == "I050"]
            df_i050_norm = pd.DataFrame()
            accounting_ctas: Set[str] = set()

            if not df_i050.empty:
                df_i050_norm = df_i050.copy()
                df_i050_norm.columns = df_i050_norm.columns.str.replace(
                    "I050_", "", regex=False
                )
                if "COD_CTA" in df_i050_norm.columns:
                    accounting_ctas = set(
                        df_i050_norm["COD_CTA"].dropna().astype(str).str.strip()
                    )  # type: ignore

            df_i051 = df_all[df_all["REG"] == "I051"]
            df_learn_map = pd.DataFrame()
            if not df_i051.empty and not df_i050_norm.empty:
                df_i051_norm = df_i051.copy()
                df_i051_norm.columns = df_i051_norm.columns.str.replace(
                    "I051_", "", regex=False
                )
                # Inclui CTA (descrição) no aprendizado histórico
                df_learn_map = pd.merge(
                    df_i051_norm,
                    df_i050_norm[["PK", "COD_CTA", "COD_CTA_SUP", "CTA"]],
                    left_on="FK_PAI",
                    right_on="PK",
                    how="inner",
                )  # type: ignore
                df_learn_map.rename(
                    columns={"COD_CTA_SUP": "COD_SUP", "CTA": "DESCRICAO"}, inplace=True
                )
                if not cod_plan_ref:
                    cod_plan_ref = df_i051_norm.iloc[0].get("COD_PLAN_REF")

            if not df_learn_map.empty or cod_plan_ref or accounting_ctas:
                mapper.learn(
                    reader.cnpj or "",
                    str(reader.ano_vigencia or ""),
                    df_learn_map,
                    cod_plan_ref=str(cod_plan_ref) if cod_plan_ref else None,
                    accounting_ctas=accounting_ctas,
                    file_id=nome_arq,
                )

        except Exception as e:
            logging.warning(f"Falha no aprendizado de {nome_arq}: {e}")

    mapper.build_consensus()
    mapper.save_knowledge(history_file)
    logging.info("Consenso histórico persistido.")

    # --- EXECUÇÃO PARALELA (Otimização Ouro) ---
    num_cpus = max(1, multiprocessing.cpu_count() - 1)  # Deixa 1 núcleo livre para o SO
    logging.info(
        f"Iniciando auditoria paralela em {len(arquivos)} arquivos ({num_cpus} núcleos)..."
    )

    results_data = []
    with ProcessPoolExecutor(max_workers=num_cpus) as executor:
        futures = {
            executor.submit(
                processar_um_arquivo, arq, output_dir, mapper, telemetry
            ): arq
            for arq in arquivos
        }  # type: ignore
        for future in as_completed(futures):
            try:
                data = future.result()
                if data:
                    results_data.append(data)
            except Exception as e:
                logging.error(f"Erro em tarefa paralela: {e}")

    if telemetry:
        for d in results_data:
            telemetry.merge(d)  # type: ignore

    # Consolidação Final
    consolidator = ECDConsolidator(output_dir)
    if telemetry:
        consolidator.telemetry = telemetry
        consolidator.current_ecd_id = "GLOBAL"
    consolidator.consolidar()


def gerar_relatorio_final(
    telemetry: TelemetryCollector, start_time: float, elapsed: float
):
    """Gera o log tabular de execução e telemetria forense completa."""
    log_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data", "output", "file_logs"
    )
    os.makedirs(log_dir, exist_ok=True)
    hist_file = os.path.join(log_dir, "execution_history.log")
    end_time = start_time + elapsed

    try:
        with open(hist_file, "a", encoding="utf-8") as f:
            ts_sessao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write("\n" + "=" * 100 + "\n")
            f.write(f"SESSÃO DE ANÁLISE: {ts_sessao} [MODO: PARALELO / RAM-ECO]\n")
            f.write("=" * 100 + "\n\n")

            ecds = sorted(list(telemetry.data.keys()))
            if ecds:
                # I. MATRIZ DE TELEMETRIA POR ECD
                f.write("I. MATRIZ DE TELEMETRIA POR ECD (Processos Individuais)\n")
                f.write("-" * 100 + "\n")
                header = f"{'PROCESSO / ANO'.ljust(30)} | "
                for ecd in ecds:
                    header += f"{str(ecd).ljust(13)} | "
                f.write(header + "TOTAL PROC.\n")
                f.write("-" * 100 + "\n")

                # Linha de Início
                row_inicio = f"{'INÍCIO PROCESSAMENTO'.ljust(30)} | "
                for ecd in ecds:
                    ts = datetime.fromtimestamp(telemetry.data[ecd]["inicio"]).strftime(
                        "%H:%M:%S"
                    )
                    row_inicio += f"{ts.ljust(13)} | "
                f.write(row_inicio + " ---\n")
                f.write("-" * 100 + "\n")

                # Métricas de Componentes
                all_comps: Dict[str, Set[str]] = {}
                for ecd in ecds:
                    for comp, meths in telemetry.data[ecd].get("metrics", {}).items():
                        c_str = str(comp)
                        if c_str not in all_comps:
                            all_comps[c_str] = set()
                        for meth in meths.keys():
                            all_comps[c_str].add(str(meth))  # type: ignore

                grand_total_all: float = 0.0
                for comp in sorted(all_comps.keys()):
                    comp_total_row: float = 0.0
                    row_comp = f"{comp} (Subtotal)".ljust(30) + " | "
                    for ecd in ecds:
                        val = sum(telemetry.data[ecd]["metrics"].get(comp, {}).values())
                        row_comp += f"{(f'{val:.2f}s').ljust(13)} | "
                        comp_total_row += val
                    f.write(row_comp + f"{comp_total_row:.2f}s\n")

                    for meth in sorted(all_comps[comp]):  # type: ignore
                        row_meth = f"  - {meth.ljust(26)} | "
                        meth_total_row: float = 0.0
                        for ecd in ecds:
                            val = (
                                telemetry.data[ecd]["metrics"]
                                .get(comp, {})
                                .get(meth, 0.0)
                            )
                            row_meth += f"{(f'{val:.2f}s').ljust(13)} | "
                            meth_total_row += val
                        f.write(row_meth + f"{meth_total_row:.2f}s\n")
                    f.write(" " * 30 + " | " + " " * 15 * len(ecds) + " | \n")
                    grand_total_all = grand_total_all + comp_total_row  # type: ignore

                f.write("-" * 100 + "\n")
                # Linha de Término
                row_fim = f"{'TÉRMINO PROCESSAMENTO'.ljust(30)} | "
                for ecd in ecds:
                    term = telemetry.data[ecd].get("termino")
                    ts = (
                        datetime.fromtimestamp(term).strftime("%H:%M:%S")
                        if term
                        else "N/A"
                    )
                    row_fim += f"{ts.ljust(13)} | "
                f.write(row_fim + " ---\n")

                # Tempo Total ECD
                row_total = f"{'TEMPO TOTAL ECD (F - I)'.ljust(30)} | "
                for ecd in ecds:
                    term = telemetry.data[ecd].get("termino")
                    if term:
                        dur = term - telemetry.data[ecd]["inicio"]
                        row_total += f"{(f'{dur:.2f}s').ljust(13)} | "
                    else:
                        row_total += "N/A".ljust(13) + " | "
                f.write(row_total + f"{grand_total_all:.2f}s\n")
                f.write("-" * 100 + "\n\n")

            # II. TELEMETRIA DE PROCESSOS GLOBAIS
            f.write("II. TELEMETRIA DE PROCESSOS GLOBAIS (Pós-Processamento)\n")
            f.write("-" * 100 + "\n")
            f.write(
                f"{'COMPONENTE / MÉTODO'.ljust(50)} | {'DURAÇÃO EXATA'.ljust(20)}\n"
            )
            f.write("-" * 100 + "\n")
            global_total = 0.0
            for comp, meths in telemetry.global_stats.items():
                f.write(f"{comp}\n")
                for meth, dur in meths.items():
                    f.write(f"  - {meth.ljust(46)} | {dur:.2f}s\n")
                    global_total = global_total + float(dur)  # type: ignore
            f.write("-" * 100 + "\n")
            f.write(f"{'TOTAL PROCESSOS GLOBAIS'.ljust(50)} | {global_total:.2f}s\n\n")

            # III. RESUMO FINAL DA ANÁLISE
            f.write("III. RESUMO FINAL DA ANÁLISE\n")
            f.write("-" * 100 + "\n")
            f.write(
                f"- INÍCIO DA ANÁLISE:   {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            f.write(
                f"- FINAL DA ANÁLISE:    {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            f.write(f"- DURAÇÃO DA EXECUÇÃO: {str(timedelta(seconds=int(elapsed)))}\n")
            f.write("=" * 100 + "\n")

    except Exception as e:
        print(f"Erro ao gravar log de telemetria: {e}")


if __name__ == "__main__":
    start_time = time.time()
    telemetry = TelemetryCollector()
    try:
        executar_pipeline_batch(telemetry=telemetry)
    except Exception as e:
        logging.critical(f"ERRO NO BATCH: {e}")
    finally:
        elapsed = time.time() - start_time
        gerar_relatorio_final(telemetry, start_time, elapsed)
        print(f"\nTEMPO TOTAL: {elapsed:.2f}s")
