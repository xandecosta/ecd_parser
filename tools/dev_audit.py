import logging
import os
import sys
from typing import Any, cast

# Ajusta o path para permitir execução de dentro da pasta scripts/
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if base_dir not in sys.path:
    sys.path.append(base_dir)

import pandas as pd  # noqa: E402

from core.auditor import ECDAuditor  # noqa: E402
from core.processor import ECDProcessor  # noqa: E402
from core.reader_ecd import ECDReader  # noqa: E402
from exporters.audit_exporter import AuditExporter  # noqa: E402

if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        cast(Any, sys.stdout).reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        cast(Any, sys.stderr).reconfigure(encoding="utf-8")

# Configuração de logging mínima
logging.basicConfig(level=logging.ERROR)


def carregar_dados_teste():
    """Carrega dados de apenas um arquivo para acelerar o desenvolvimento."""
    # Usando o arquivo de 2020
    filename = "87278305000148-43200310670-20200101-20201231-G-B258C7831DDE96C2E79291935C287AD7BF2A56FB-1-SPED-ECD Thiago Mattei 20220901.txt"
    test_file = os.path.join(base_dir, "data", "input", filename)

    if not os.path.exists(test_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {test_file}")

    print(f"--- LENDO ARQUIVO: {filename} ---")
    reader = ECDReader(test_file)
    registros = list(reader.processar_arquivo())

    processor = ECDProcessor(
        registros,
        cnpj=getattr(reader, "cnpj", ""),
        layout_versao=reader.layout_versao or "",
    )

    # Processamento base necessário para o auditor
    df_plano = processor.processar_plano_contas()
    df_lancamentos = processor.processar_lancamentos(df_plano)
    dict_balancetes = processor.gerar_balancetes()
    df_balancete_mensal = dict_balancetes.get("04_Balancetes_Mensais")

    raw_i050 = processor.blocos.get("dfECD_I050")
    raw_i051 = processor.blocos.get("dfECD_I051")

    return {
        "df_diario": df_lancamentos,
        "df_balancete": df_balancete_mensal,
        "df_plano": df_plano,
        "raw_i050": raw_i050,
        "raw_i051": raw_i051,
    }


if __name__ == "__main__":
    dados = carregar_dados_teste()

    auditor = ECDAuditor(
        df_diario=dados["df_diario"],
        df_balancete=dados["df_balancete"],
        df_plano=dados["df_plano"],
        df_naturezas=dados["raw_i050"],
        df_mapeamento=dados["raw_i051"],
    )

    print("\n--- EXECUTANDO AUDITORIA FOCADA ---")
    resultados = auditor.executar_auditoria_completa()

    # --- EXPORTAÇÃO (Auditabilidade) ---
    # O output_dir também deve subir um nível para sair de scripts/ e entrar em data/
    output_dir = os.path.join(base_dir, "data", "test_output")
    exporter = AuditExporter(output_dir)

    # Tenta obter o período (DATA) dos dados carregados
    df_balancete = dados.get("df_balancete")
    prefixo = ""
    if (
        df_balancete is not None
        and not df_balancete.empty
        and "DT_FIN" in df_balancete.columns
    ):
        prefixo = pd.to_datetime(df_balancete["DT_FIN"].iloc[0]).strftime("%Y%m%d")

    exporter.exportar_dashboard(resultados, "DEV_TEST_SINGLE_FILE", prefixo=prefixo)
    exporter.exportar_detalhes_parquet(resultados, prefixo=prefixo)

    print(f"\n[SUCESSO] Relatório gerado em: {output_dir}")
    print("-" * 50)
    for teste, res in resultados.items():
        status = res.get("status", "N/A")
        impacto = res.get("impacto", 0)
        print(f" - {teste}: {status} (Impacto: {impacto:,.2f})")
