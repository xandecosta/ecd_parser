import os
import sys
import pandas as pd

# Ajusta o path para permitir execução de dentro da pasta tests/
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from core.reader_ecd import ECDReader  # noqa: E402
from core.processor import ECDProcessor  # noqa: E402
from core.auditor import ECDAuditor  # noqa: E402
from exporters.audit_exporter import AuditExporter  # noqa: E402


def test_single_file_audit():
    # 1. Caminhos
    # Selecionando o arquivo de 2021 (o mais recente da lista do usuário)
    filename = "87278305000148-43200310670-20210101-20211231-G-7CE9B34A61E0FBDD1C45A7ED0FB73446B270566B-1-SPED-ECD Thiago Mattei 20220901.txt"
    test_file = os.path.join(project_root, "data", "input", filename)
    output_dir = os.path.join(project_root, "data", "test_output")

    if not os.path.exists(test_file):
        print(f"[ERRO] Arquivo não encontrado: {test_file}")
        return

    os.makedirs(output_dir, exist_ok=True)

    print(
        f"\n[TEST] Iniciando teste rápido de auditoria para: {os.path.basename(test_file)}"
    )

    try:
        # --- PASSO 1: LEITURA ---
        reader = ECDReader(test_file)
        registros = list(reader.processar_arquivo())

        # --- PASSO 2: PROCESSAMENTO ---
        processor = ECDProcessor(
            registros,
            cnpj=getattr(reader, "cnpj", ""),
            layout_versao=reader.layout_versao or "",
        )

        df_plano = processor.processar_plano_contas()
        df_lancamentos = processor.processar_lancamentos(df_plano)
        dict_balancetes = processor.gerar_balancetes()
        df_balancete_mensal = dict_balancetes.get("04_Balancetes_Mensais")
        if df_balancete_mensal is None:
            df_balancete_mensal = pd.DataFrame()

        # --- PASSO 3: AUDITORIA ---
        print("[TEST] Executando Auditor...")
        raw_i050 = processor.blocos.get("dfECD_I050")
        raw_i051 = processor.blocos.get("dfECD_I051")

        auditor = ECDAuditor(
            df_diario=df_lancamentos,
            df_balancete=df_balancete_mensal,
            df_plano=df_plano,
            df_naturezas=raw_i050,
            df_mapeamento=raw_i051,
        )

        resultados = auditor.executar_auditoria_completa()

        # --- PASSO 4: EXPORTAÇÃO ---
        print("[TEST] Gerando Relatórios...")
        exporter = AuditExporter(output_dir)
        exporter.exportar_dashboard(resultados, "TESTE_AUDITORIA_RAPIDO")
        exporter.exportar_detalhes_parquet(resultados)

        print(f"\n[SUCESSO] Auditoria concluída. Relatório gerado em: {output_dir}")
        print("-" * 50)
        for teste, res in resultados.items():
            status = res.get("status", "N/A")
            print(f" - {teste}: {status}")

    except Exception as e:
        print(f"\n[FALHA] Erro durante o teste: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_single_file_audit()
