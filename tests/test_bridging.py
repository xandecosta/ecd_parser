import os
import pandas as pd
import logging
import sys
import glob
from typing import Dict, Set, List, Any

# Garantir que a raiz do projeto esteja no PYTHONPATH para execução direta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.reader_ecd import ECDReader
from core.processor import ECDProcessor
from intelligence.historical_mapper import HistoricalMapper

# SILENCIAMENTO DE LOGS PARA FOCO NO RESULTADO
logging.getLogger().setLevel(logging.CRITICAL)
logging.disable(logging.CRITICAL)


def get_analytic_ctas(regs: List[Dict[str, Any]]) -> Set[str]:
    """Retorna o conjunto literal de COD_CTA das contas analíticas (IND_CTA == 'A')."""
    ctas = set()
    for r in regs:
        if r["REG"] == "I050":
            ind = str(r.get("I050_IND_CTA") or r.get("IND_CTA", "")).strip().upper()
            cod = str(r.get("I050_COD_CTA") or r.get("COD_CTA", ""))
            if ind == "A" and cod:
                ctas.add(cod)
    return ctas


def get_learning_data(regs: List[Dict[str, Any]]) -> pd.DataFrame:
    """Cria DataFrame com COD_CTA, COD_SUP e COD_CTA_REF para aprendizado."""
    pk_info = {}
    for r in regs:
        if r["REG"] == "I050":
            pk = r.get("PK")
            cod = str(r.get("I050_COD_CTA") or r.get("COD_CTA", ""))
            sup = str(r.get("I050_COD_CTA_SUP") or r.get("COD_CTA_SUP", ""))
            if pk and cod:
                pk_info[pk] = (cod, sup)

    learning_rows = []
    for r in regs:
        if r["REG"] == "I051":
            fk = r.get("FK_PAI")
            ref = str(r.get("I051_COD_CTA_REF") or r.get("COD_CTA_REF", "")).strip()
            if fk in pk_info and ref:
                cod, sup = pk_info[fk]
                learning_rows.append(
                    {"COD_CTA": cod, "COD_SUP": sup, "COD_CTA_REF": ref}
                )

    return pd.DataFrame(learning_rows)


def run_dynamic_bridging():
    # Ajuste dinâmico do path para quando executado de dentro da pasta tests/
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    input_dir = os.path.join(project_root, "data", "input")
    analysis_dir = os.path.join(project_root, "data", "analysis")

    if not os.path.exists(input_dir):
        print(f"[ERRO] Pasta não encontrada: {input_dir}")
        return

    print("\n" + "=" * 60)
    print(" FERRAMENTA DE AUDITORIA DINÂMICA: BRIDGING (PONTE VIRTUAL)")
    print("=" * 60)

    # 1. Escaneamento de Arquivos
    files_info = []
    txt_files = glob.glob(os.path.join(input_dir, "*.txt"))

    if not txt_files:
        print("[AVISO] Nenhum arquivo .txt na pasta input.")
        return

    print(f"\n[1] ESCANEANDO {len(txt_files)} ARQUIVOS...")
    for path in txt_files:
        try:
            reader = ECDReader(path)
            # Lê apenas o início para pegar metadados (0000)
            for reg in reader.processar_arquivo():
                if reg["REG"] == "0000":
                    break

            files_info.append(
                {
                    "path": path,
                    "name": os.path.basename(path),
                    "cnpj": reader.cnpj,
                    "ano": reader.ano_vigencia,
                    "versao": reader.layout_versao,
                }
            )
        except Exception as e:
            print(f"    > Erro ao ler {os.path.basename(path)}: {e}")

    # Ordena por CNPJ e Ano
    files_info.sort(key=lambda x: (x["cnpj"], x["ano"] or 0))

    # 2. Seleção do Alvo
    print("\nARQUIVOS DISPONÍVEIS:")
    for i, meta in enumerate(files_info):
        ano_str = str(meta["ano"]) if meta["ano"] else "????"
        print(
            f"[{i}] CNPJ: {meta['cnpj']} | Ano: {ano_str} | Arquivo: {meta['name'][:50]}..."
        )

    try:
        entrada = input("\n>>> Selecione o índice do ARQUIVO ALVO para auditoria: ")
        if not entrada:
            return
        idx_alvo = int(entrada)
        if not (0 <= idx_alvo < len(files_info)):
            raise ValueError()
    except (ValueError, EOFError, KeyboardInterrupt):
        print("Seleção cancelada ou inválida.")
        return

    alvo = files_info[idx_alvo]
    cnpj_alvo = alvo["cnpj"]
    ano_alvo = alvo["ano"]

    # 3. Identificação de Vizinhos (Mesmo CNPJ, Ano +- 1)
    vizinhos = []
    if ano_alvo:
        for f in files_info:
            if f["cnpj"] == cnpj_alvo and f["ano"] in [ano_alvo - 1, ano_alvo + 1]:
                vizinhos.append(f)

    print(f"\n[2] ARQUIVO ALVO DEFINIDO: {alvo['name']}")
    if vizinhos:
        print(
            f"    > Vizinhos identificados para aprendizado: {[v['ano'] for v in vizinhos]}"
        )
    else:
        print("    > [AVISO] Nenhum vizinho cronológico (Ano-1 ou Ano+1) encontrado.")

    # 4. Processamento e Aprendizado
    mapper = HistoricalMapper()

    # Aprendizado dos Vizinhos
    for v in vizinhos:
        print(f"\n[3] APRENDENDO COM VIZINHO {v['ano']}...")
        reader_v = ECDReader(v["path"])
        regs_v = list(reader_v.processar_arquivo())
        ctas_v = get_analytic_ctas(regs_v)
        df_map_v = get_learning_data(regs_v)

        # Tenta pegar COD_PLAN_REF (Instituição)
        cod_plan_v = None
        for r in regs_v:
            if r["REG"] == "0000":
                cod_plan_v = r.get("0000_COD_PLAN_REF") or r.get("COD_PLAN_REF")
                break

        if not cod_plan_v:
            for r in regs_v:
                if r["REG"] == "I051":
                    cod_plan_v = r.get("I051_COD_PLAN_REF") or r.get("COD_PLAN_REF")
                    if cod_plan_v:
                        break

        if cod_plan_v:
            print(f"    > Instituição (COD_PLAN_REF) detectada: {cod_plan_v}")
        else:
            print(f"    > [AVISO] COD_PLAN_REF não encontrado no vizinho {v['ano']}")

        mapper.learn(
            cnpj_alvo,
            str(v["ano"]),
            df_map_v,
            cod_plan_ref=str(cod_plan_v) if cod_plan_v else None,
            accounting_ctas=ctas_v,
        )

    # Leitura do Alvo
    print(f"\n[4] ANALISANDO ESTRUTURA DO ALVO {ano_alvo}...")
    reader_alvo = ECDReader(alvo["path"])
    regs_alvo = list(reader_alvo.processar_arquivo())
    ctas_alvo = get_analytic_ctas(regs_alvo)

    mapper.learn(cnpj_alvo, str(ano_alvo), pd.DataFrame(), accounting_ctas=ctas_alvo)
    mapper.build_consensus()

    # 5. Geração de Balancete via Processor
    processor = ECDProcessor(
        regs_alvo, cnpj=cnpj_alvo, layout_versao=alvo["versao"], knowledge_base=mapper
    )

    if not processor.cod_plan_ref:
        inferred = mapper.get_inferred_plan(cnpj_alvo, str(ano_alvo))
        if inferred:
            processor.cod_plan_ref = inferred
            print(f"    > COD_PLAN_REF inferido do histórico: {processor.cod_plan_ref}")

    print(
        f"    > Usando Plano Referencial: {processor.cod_plan_ref or 'NÃO LOCALIZADO'}"
    )

    print("\n[5] GERANDO BALANCETE COM PONTE VIRTUAL...")
    balancetes = processor.gerar_balancetes()
    rfb = balancetes.get("04_Balancetes_RFB")

    if rfb is not None and not rfb.empty:
        # Garante existência da pasta de análise
        os.makedirs(analysis_dir, exist_ok=True)
        output_file = os.path.join(analysis_dir, "audit_bridging.xlsx")

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            rfb.to_excel(writer, index=False, sheet_name=f"RFB_{ano_alvo}")

        print("\n[SUCESSO] Auditoria concluída!")
        print("    > Exportado para: data/analysis/audit_bridging.xlsx")

        # Resumo de Mapeamento
        df_plano = processor.processar_plano_contas()
        if not df_plano.empty:
            analiticas = df_plano[df_plano["IND_CTA"] == "A"]
            if not analiticas.empty:
                print("\nRESUMO DE MAPEAMENTO (Contas Analíticas):")
                # Converter explicitamente para Series para satisfazer o Pyright
                counts = pd.Series(analiticas["ORIGEM_MAP"]).value_counts()
                print(counts)
    else:
        print("\n[FALHA] Não foi possível gerar o balancete referencial.")


if __name__ == "__main__":
    run_dynamic_bridging()
