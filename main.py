import os
import glob
import logging
import traceback
import warnings
from typing import Optional, cast, Any
import pandas as pd
from core.reader_ecd import ECDReader
from core.processor import ECDProcessor
from exporters.exporter import ECDExporter
from exporters.consolidator import ECDConsolidator
from intelligence.historical_mapper import HistoricalMapper
from core.auditor import ECDAuditor
from exporters.audit_exporter import AuditExporter


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
    caminho_arquivo: str, output_base: str, mapper: Optional[HistoricalMapper] = None
):
    """
    Executa o ciclo completo de processamento para um único arquivo ECD.
    """
    nome_arquivo = os.path.basename(caminho_arquivo)
    nome_projeto = nome_arquivo.replace(".txt", "")

    print(f"\n>>> PROCESSANDO: {nome_arquivo}")

    try:
        # --- PASSO 1: LEITURA ---
        reader = ECDReader(caminho_arquivo)
        registros = list(reader.processar_arquivo())

        if not registros:
            print("      [AVISO] Arquivo vazio ou sem registros válidos.")
            return

        # Captura metadados críticos do Reader para o processamento de auditoria
        id_folder = reader.periodo_ecd if reader.periodo_ecd else nome_projeto
        cnpj_contribuinte = getattr(reader, "cnpj", "")

        # --- PASSO 2: PROCESSAMENTO ---
        # Instancia o processador com os metadados para injeção e mapeamento RFB
        processor = ECDProcessor(
            registros,
            cnpj=cnpj_contribuinte,
            layout_versao=reader.layout_versao or "",
            knowledge_base=mapper,
        )

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

        resultados_audit = auditor.executar_auditoria_completa()

        # --- PASSO 3: EXPORTAÇÃO ---
        pasta_saida_arquivo = os.path.join(output_base, id_folder)
        exporter = ECDExporter(pasta_saida_arquivo)
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
            tabelas, nome_projeto, prefixo=id_folder, itens_adicionais=itens_audit_log
        )

        print(f"      [OK] Finalizado com sucesso: {id_folder}")

    except Exception as e:
        print(f"      [ERRO] Falha ao processar {nome_arquivo}: {str(e)}")
        logging.error(traceback.format_exc())


def executar_pipeline_batch():
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

    print(">>> LIMPANDO PASTA DE SAÍDA (PRESERVANDO LOGS)...")
    for item in os.listdir(output_dir):
        caminho_item = os.path.join(output_dir, item)
        # Deleta tudo exceto a pasta de logs
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
    for arquivo in arquivos:
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

            accounting_ctas: set[str] = set()
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
                )

        except Exception as e:
            print(
                f"      [AVISO] Falha no aprendizado de {os.path.basename(arquivo)}: {e}"
            )

    mapper.build_consensus()
    print("      [OK] Consenso histórico construído.")

    for arquivo in arquivos:
        processar_um_arquivo(arquivo, output_dir, mapper)

    # --- PASSO 4: CONSOLIDAÇÃO ---
    consolidator = ECDConsolidator(output_dir)
    consolidator.consolidar()


if __name__ == "__main__":
    executar_pipeline_batch()
