import pytest
import os
import pandas as pd
from decimal import Decimal
from exporters.exporter import ECDExporter


@pytest.fixture
def pasta_teste(tmp_path):
    """Cria uma pasta temporária para os testes de exportação."""
    d = tmp_path / "output_teste"
    d.mkdir()
    return str(d)


def test_criacao_de_pasta_base(pasta_teste):
    """Verifica se o exportador garante a existência da pasta base."""
    ECDExporter(pasta_teste)
    assert os.path.exists(pasta_teste)


def test_exportacao_parquet_e_excel_na_raiz(pasta_teste):
    """Garante que Parquet e Excel são gerados na raiz conforme a nova preferência."""
    exporter = ECDExporter(pasta_teste)

    # Dados Mock
    df_teste = pd.DataFrame(
        {"COD_CTA": ["1", "1.1"], "VL_SINAL": [Decimal("100.00"), Decimal("100.00")]}
    )

    # Dicionário de teste
    dicionario_teste = {"Balancete_Mensal": df_teste, "Lancamentos_Gerais": df_teste}

    exporter.exportar_lote(dicionario_teste, "Teste_Unitario")

    # Verificações Parquet (Devem estar na raiz)
    assert os.path.exists(os.path.join(pasta_teste, "Balancete_Mensal.parquet"))
    assert os.path.exists(os.path.join(pasta_teste, "Lancamentos_Gerais.parquet"))

    # Verificações Excel (Apenas Balancete na raiz)
    assert os.path.exists(os.path.join(pasta_teste, "Balancete_Mensal.xlsx"))
    assert not os.path.exists(os.path.join(pasta_teste, "Lancamentos_Gerais.xlsx"))


def test_geracao_de_log(pasta_teste):
    """Verifica se o arquivo de log TXT está sendo atualizado."""
    exporter = ECDExporter(pasta_teste)
    df = pd.DataFrame({"A": [1]})

    exporter.exportar_lote({"Tabela_Log": df}, "Teste_Log")

    caminho_log = os.path.join(pasta_teste, "Arquivos_Gerados.txt")
    assert os.path.exists(caminho_log)

    with open(caminho_log, "r", encoding="utf-8") as f:
        conteudo = f.read()
        assert "Tabela_Log.parquet" in conteudo
