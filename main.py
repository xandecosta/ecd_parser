import os
import time
import glob
import logging
import traceback
import warnings
import re
from typing import Optional, cast, Any, Set
import pandas as pd
from core.reader_ecd import ECDReader
from core.processor import ECDProcessor
from exporters.exporter import ECDExporter
from exporters.consolidator import ECDConsolidator
from intelligence.historical_mapper import HistoricalMapper
from core.auditor import ECDAuditor
from datetime import datetime, timedelta
from exporters.audit_exporter import AuditExporter
from core.telemetry import TelemetryCollector


import sys

if sys.platform == "win32":
    # Garante que o terminal aceite UTF-8 mesmo sem variável de ambiente
    if hasattr(sys.stdout, "reconfigure"):
        cast(Any, sys.stdout).reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        cast(Any, sys.stderr).reconfigure(encoding="utf-8")

# 1. Silenciar Avisos de Bibliotecas (Pandas, etc)
warnings.filterwarnings("ignore")

# 2. Silenciar Logs de Sistema e Módulos (Apenas Erros Críticos)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(level=logging.ERROR, format="%(levelname)s: %(message)s")
logging.getLogger("core.reader_ecd").setLevel(logging.ERROR)


def processar_um_arquivo(
    caminho_arquivo: str,
    output_base: str,
    mapper: Optional[HistoricalMapper] = None,
    telemetry: Optional[TelemetryCollector] = None,
):
    """
    Executa o ciclo completo de processamento para um único arquivo ECD.
    """
    start_proc = time.time()
    nome_arquivo = os.path.basename(caminho_arquivo)

    nome_projeto = nome_arquivo.replace(".txt", "")

    print(f"\n>>> PROCESSANDO: {nome_arquivo}")

    try:
        # --- PASSO 1: LEITURA ---
        reader = ECDReader(caminho_arquivo)

        # ID do folder: tenta extrair do nome do arquivo (padrão AAAAMMDD no nome)
        # para poder configurar a telemetria ANTES de processar_arquivo
        match = re.search(r"(\d{8})-\d{8}-", nome_arquivo)
        id_folder_temp = match.group(1) if match else nome_projeto
        # Converte para o formato do periodo (ex: 20111231)
        if match:
            # Usa a data final do período (segundo grupo de 8 dígitos)
            match2 = re.search(r"\d{8}-(\d{8})-", nome_arquivo)
            id_folder_temp = match2.group(1) if match2 else id_folder_temp

        if telemetry:
            telemetry.start_ecd(id_folder_temp)
            reader.telemetry = telemetry
            reader.current_ecd_id = id_folder_temp

        registros = list(reader.processar_arquivo())

        if not registros:
            print("      [AVISO] Arquivo vazio ou sem registros válidos.")
            return

        # Captura metadados críticos do Reader para o processamento de auditoria
        # Usa o período real detectado pelo reader (pode diferir do temp)
        id_folder = reader.periodo_ecd if reader.periodo_ecd else nome_projeto
        cnpj_contribuinte = getattr(reader, "cnpj", "")

        if telemetry and id_folder != id_folder_temp:
            # Remapeia as métricas para o id_folder real se houve diferença
            telemetry.data[id_folder] = telemetry.data.pop(
                id_folder_temp,
                {
                    "inicio": telemetry.data.get(id_folder_temp, {}).get("inicio", 0),
                    "termino": None,
                    "metrics": {},
                },
            )
            reader.current_ecd_id = id_folder

        # --- PASSO 2: PROCESSAMENTO ---
        # Instancia o processador com os metadados para injeção e mapeamento RFB
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

        # O método gerar_balancetes agora retorna um dicionário (Empresa e baseRFB)
        dict_balancetes = processor.gerar_balancetes()

        # O método processar_demonstracoes retorna um dicionário (BP e DRE)
        dict_demos = processor.processar_demonstracoes()

        # --- PASSO 2.5: AUDITORIA FORENSE ---
        print("      [AUDITORIA] Executando bateria de testes forenses...")

        # Preparação dos dados para o Auditor
        df_balancete_mensal = dict_balancetes.get("04_Balancetes_Mensais")

        # Acesso aos dataframes brutos internos do processor (I050, I051)
        # Nota: O processor armazena em self.blocos
        raw_i050 = processor.blocos.get("dfECD_I050")
        raw_i051 = processor.blocos.get("dfECD_I051")

        auditor = ECDAuditor(
            df_diario=df_lancamentos,
            df_balancete=df_balancete_mensal
            if df_balancete_mensal is not None
            else pd.DataFrame(),
            df_plano=df_plano,
            df_naturezas=raw_i050,
            df_mapeamento=raw_i051,
        )
        if telemetry:
            auditor.telemetry = telemetry
            auditor.current_ecd_id = id_folder

        resultados_audit = auditor.executar_auditoria_completa()

        # --- PASSO 3: EXPORTAÇÃO ---
        pasta_saida_arquivo = os.path.join(output_base, id_folder)
        exporter = ECDExporter(pasta_saida_arquivo)
        if telemetry:
            exporter.telemetry = telemetry
            exporter.current_ecd_id = id_folder

        itens_audit_log = []

        # Exportador de Auditoria (Isolado para resiliência)
        try:
            audit_exporter = AuditExporter(pasta_saida_arquivo)
            # Passa id_folder como prefixo para seguir máscara DATA_07_...
            itens_audit_log += audit_exporter.exportar_dashboard(
                resultados_audit, nome_projeto, prefixo=id_folder
            )
            itens_audit_log += audit_exporter.exportar_detalhes_parquet(
                resultados_audit, prefixo=id_folder
            )
        except Exception as e:
            print(f"      [AVISO] Falha na exportação de auditoria: {str(e)}")

        # Consolidamos todas as tabelas seguindo a hierarquia de prioridade de auditoria
        # Reorganizado para manter as demonstrações no topo (01 e 02)
        tabelas = {
            "01_BP": dict_demos.get("BP"),
            "02_DRE": dict_demos.get("DRE"),
            "03_Balancetes_Mensais": dict_balancetes.get("04_Balancetes_Mensais"),
            "04_Balancete_baseRFB": dict_balancetes.get("04_Balancetes_RFB"),
            "05_Plano_Contas": df_plano,
            "06_Lancamentos_Contabeis": df_lancamentos,
        }

        # Exporta com o prefixo da data para permitir abertura simultânea no Excel
        exporter.exportar_lote(
            tabelas,
            nome_projeto,
            prefixo=id_folder,
            itens_adicionais=itens_audit_log,
            tempo_inicio=start_proc,
        )

        if telemetry:
            telemetry.end_ecd(id_folder)

        print(f"      [OK] Finalizado com sucesso: {id_folder}")

    except Exception as e:
        print(f"      [ERRO] Falha ao processar {nome_arquivo}: {str(e)}")
        logging.error(traceback.format_exc())


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

    # --- PASSO NOVO: LIMPEZA DA PASTA DE SAÍDA ---
    import shutil

    print(">>> LIMPANDO PASTA DE SAÍDA...")
    for item in os.listdir(output_dir):
        caminho_item = os.path.join(output_dir, item)
        # Deleta as pastas de processamento, mas PRESERVA a pasta de logs permanentemente
        if item != "file_logs":
            if os.path.isdir(caminho_item):
                shutil.rmtree(caminho_item)
            else:
                os.remove(caminho_item)

    # Garante que a pasta de log existe
    os.makedirs(os.path.join(output_dir, "file_logs"), exist_ok=True)

    print(f"Iniciando processamento de {len(arquivos)} arquivo(s)...")

    # --- PASSO 0: LEARNING PASS (Cross-Temporal) ---
    print(">>> FASE DE APRENDIZADO HISTÓRICO...")
    mapper = HistoricalMapper()
    if telemetry:
        mapper.telemetry = telemetry
        mapper.current_ecd_id = "GLOBAL"

    # IO INTELIGENTE: Carrega conhecimento prévio se existir
    intelligence_dir = os.path.join(base_dir, "data", "intelligence")
    os.makedirs(intelligence_dir, exist_ok=True)
    history_file = os.path.join(intelligence_dir, "history.json")

    if os.path.exists(history_file):
        print(
            f"      [IO] Carregando conhecimento prévio de {os.path.basename(history_file)}..."
        )
        mapper.load_knowledge(history_file)

    for arquivo in arquivos:
        nome_arq = os.path.basename(arquivo)
        # Pula se o arquivo já foi processado conforme o "cérebro" persistente
        if nome_arq in mapper._processed_files:
            continue

        print(f"      [LEARNING] Aprendendo com: {nome_arq}")
        try:
            reader = ECDReader(arquivo)
            # Lê os registros para o mapa de conhecimento
            all_regs = list(reader.processar_arquivo())
            if not all_regs:
                continue

            df_all = cast(pd.DataFrame, pd.DataFrame(all_regs))

            # 1. Identificação do COD_PLAN_REF para aprendizado
            cod_plan_ref = None
            df_0000 = df_all[df_all["REG"] == "0000"]
            if not df_0000.empty:
                # Normaliza colunas 0000
                df_0000_norm = df_0000.copy()
                df_0000_norm.columns = df_0000_norm.columns.str.replace(
                    "0000_", "", regex=False
                )
                cod_plan_ref = df_0000_norm.iloc[0].get("COD_PLAN_REF")

            # 2. Extrai e Normaliza I050 (Estrutura de Contas)
            df_i050 = df_all[df_all["REG"] == "I050"]
            if not df_i050.empty:
                df_i050_norm = df_i050.copy()
                df_i050_norm.columns = df_i050_norm.columns.str.replace(
                    "I050_", "", regex=False
                )
            else:
                df_i050_norm = pd.DataFrame()

            accounting_ctas: Set[str] = set()
            if not df_i050_norm.empty and "COD_CTA" in df_i050_norm.columns:
                # Simplificado: assume que df_i050_norm["COD_CTA"] é sempre uma Series
                # ou que o cast é suficiente para o type checker.
                col_cta_series = cast(pd.Series, df_i050_norm["COD_CTA"])
                accounting_ctas = set(col_cta_series.dropna().astype(str).str.strip())

            # 3. Prepara Mapeamento (I051) unindo com I050 para obter COD_SUP
            df_i051 = df_all[df_all["REG"] == "I051"]
            df_mapping_to_learn = pd.DataFrame()

            if not df_i051.empty and not df_i050_norm.empty:
                df_i051_norm = df_i051.copy()
                df_i051_norm.columns = df_i051_norm.columns.str.replace(
                    "I051_", "", regex=False
                )
                # Join I051(FK_PAI) -> I050(PK) para capturar a relação hierárquica
                # Importante para a Rodada 2 do Bridging (mapeamento via grupo)
                right_side = cast(
                    pd.DataFrame, df_i050_norm[["PK", "COD_CTA", "COD_CTA_SUP"]]
                )
                df_mapping_to_learn = pd.merge(
                    df_i051_norm,
                    right_side,
                    left_on="FK_PAI",
                    right_on="PK",
                    how="inner",
                )
                # Normaliza nome para o HistoricalMapper
                df_mapping_to_learn.rename(
                    columns={"COD_CTA_SUP": "COD_SUP"}, inplace=True
                )

                # Se ainda não achou cod_plan_ref, tenta no I051
                if not cod_plan_ref:
                    cod_plan_ref = df_i051_norm.iloc[0].get("COD_PLAN_REF")

            # 4. Alimenta o Mapper
            if not df_mapping_to_learn.empty or cod_plan_ref or accounting_ctas:
                mapper.learn(
                    reader.cnpj or "",
                    str(reader.ano_vigencia or ""),
                    df_mapping_to_learn,
                    cod_plan_ref=str(cod_plan_ref) if cod_plan_ref else None,
                    accounting_ctas=accounting_ctas,
                    file_id=nome_arq,
                )

        except Exception as e:
            print(
                f"      [AVISO] Falha no aprendizado de {os.path.basename(arquivo)}: {e}"
            )

    mapper.build_consensus()
    print("      [OK] Consenso histórico construído.")

    # IO INTELIGENTE: Salva o novo conhecimento adquirido
    mapper.save_knowledge(history_file)
    print(f"      [IO] Conhecimento persistido em {os.path.basename(history_file)}")

    for arquivo in arquivos:
        processar_um_arquivo(arquivo, output_dir, mapper, telemetry=telemetry)

    # --- PASSO 4: CONSOLIDAÇÃO ---
    consolidator = ECDConsolidator(output_dir)
    if telemetry:
        consolidator.telemetry = telemetry
        consolidator.current_ecd_id = "GLOBAL"
    consolidator.consolidar()


if __name__ == "__main__":
    start_time = time.time()
    telemetry = TelemetryCollector()

    try:
        executar_pipeline_batch(telemetry=telemetry)
    except Exception as e:
        print(f"\n[ERRO CRÍTICO NO BATCH] {str(e)}")
        logging.error(traceback.format_exc())
    finally:
        end_time = time.time()
        elapsed = end_time - start_time

        # --- GERAÇÃO DO LOG TABULAR ---
        log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data", "output", "file_logs"
        )
        os.makedirs(log_dir, exist_ok=True)
        hist_file = os.path.join(log_dir, "execution_history.log")

        try:
            with open(hist_file, "a", encoding="utf-8") as f:
                ts_sessao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write("\n" + "=" * 100 + "\n")
                f.write(
                    f"SESSÃO DE ANÁLISE: {ts_sessao} [MODO: SEQUENCIAL / IO INTELIGENTE]\n"
                )
                f.write("=" * 100 + "\n\n")

                # I. MATRIZ DE TELEMETRIA POR ECD
                f.write("I. MATRIZ DE TELEMETRIA POR ECD (Processos Individuais)\n")
                f.write("-" * 100 + "\n")

                ecds = list(telemetry.data.keys())
                if ecds:
                    # Cabeçalho
                    header = f"{'PROCESSO / ANO'.ljust(30)} | "
                    for ecd in ecds:
                        header += f"{str(ecd).ljust(13)} | "
                    header += "TOTAL PROC."
                    f.write(header + "\n")
                    f.write("-" * 100 + "\n")

                    # Linha de Início
                    row_inicio = f"{'INÍCIO PROCESSAMENTO'.ljust(30)} | "
                    for ecd in ecds:
                        ts = datetime.fromtimestamp(
                            telemetry.data[ecd]["inicio"]
                        ).strftime("%H:%M:%S")
                        row_inicio += f"{ts.ljust(13)} | "
                    f.write(row_inicio + " ---\n")
                    f.write("-" * 100 + "\n")

                    # Coleta todos os componentes e métodos únicos
                    all_comps = {}
                    for ecd in ecds:
                        for comp, meths in (
                            telemetry.data[ecd].get("metrics", {}).items()
                        ):
                            if comp not in all_comps:
                                all_comps[comp] = set()
                            for meth in meths.keys():
                                all_comps[comp].add(meth)

                    grand_total_all = 0.0
                    for comp in sorted(all_comps.keys()):
                        # Subtotal do Componente
                        comp_subtotals = []
                        comp_total_row = 0.0
                        for ecd in ecds:
                            val = sum(
                                telemetry.data[ecd]["metrics"].get(comp, {}).values()
                            )
                            comp_subtotals.append(val)
                            comp_total_row += val

                        row_comp = f"{comp} (Subtotal)".ljust(30) + " | "
                        for sub in comp_subtotals:
                            row_comp += f"{(f'{sub:.2f}s').ljust(13)} | "
                        f.write(row_comp + f"{comp_total_row:.2f}s\n")

                        # Métodos do Componente
                        for meth in sorted(all_comps[comp]):
                            row_meth = f"  - {meth.ljust(26)} | "
                            meth_total_row = 0.0
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
                        grand_total_all += comp_total_row

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
                        global_total += dur
                f.write("-" * 100 + "\n")
                f.write(
                    f"{'TOTAL PROCESSOS GLOBAIS'.ljust(50)} | {global_total:.2f}s\n\n"
                )

                # III. RESUMO FINAL
                f.write("III. RESUMO FINAL DA ANÁLISE\n")
                f.write("-" * 100 + "\n")
                ts_inicio = datetime.fromtimestamp(start_time).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                ts_final = datetime.fromtimestamp(end_time).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                duracao_sessao = timedelta(seconds=int(elapsed))
                f.write(f"- INÍCIO DA ANÁLISE:   {ts_inicio}\n")
                f.write(f"- FINAL DA ANÁLISE:    {ts_final}\n")
                f.write(f"- DURAÇÃO DA EXECUÇÃO: {str(duracao_sessao)}\n")
                f.write("=" * 100 + "\n")
        except Exception as log_err:
            print(f"[AVISO] Falha ao gravar log de telemetria: {log_err}")

        print("\n" + "=" * 50)
        minutes = int(elapsed // 60)
        seconds = elapsed % 60
        if minutes > 0:
            print(f"TEMPO TOTAL: {minutes}m {seconds:.2f}s")
        else:
            print(f"TEMPO TOTAL: {seconds:.2f}s")
        print("=" * 50 + "\n")
