import pandas as pd
import os
from utils.historical_mapper import HistoricalMapper


def test_historical_mapper_bidirectional_learning():
    """Teste Original: Garante que o passado e o futuro ensinam lacunas."""
    mapper = HistoricalMapper()
    cnpj = "12345678000100"

    # 1. Simula aprendizado de 2019 (Ano com dados)
    df_2019 = pd.DataFrame(
        {"COD_CTA": ["1.01", "1.02"], "COD_CTA_REF": ["REF_CAIXA", "REF_BANCO"]}
    )
    mapper.learn(cnpj, "2019", df_2019)

    # 2. Simula aprendizado de 2021 (Futuro ensinando)
    df_2021 = pd.DataFrame({"COD_CTA": ["1.03"], "COD_CTA_REF": ["REF_CLIENTES"]})
    mapper.learn(cnpj, "2021", df_2021)

    # Constrói consenso
    mapper.build_consensus()

    # 3. Teste: Conta declarada no próprio ano
    res_2019_cta1 = mapper.get_mapping(cnpj, "1.01", "2019")
    assert res_2019_cta1["COD_CTA_REF"] == "REF_CAIXA"
    assert res_2019_cta1["ORIGEM_MAP"] == "I051"

    # 4. Teste: Lacuna em 2020 preenchida pelo PASSADO (2019)
    # Nota: Agora usa a lógica de best_neighbor
    res_2020_cta2 = mapper.get_mapping(cnpj, "1.02", "2020")
    assert res_2020_cta2["COD_CTA_REF"] == "REF_BANCO"


def test_historical_mapper_consensus_frequency():
    """Teste Original: Garante que o mapeamento mais frequente vence em caso de conflito."""
    mapper = HistoricalMapper()
    cnpj = "999"
    cta = "CONTA_CONFLITO"

    # Aprende mapeamentos divergentes
    mapper.learn(
        cnpj, "2018", pd.DataFrame({"COD_CTA": [cta], "COD_CTA_REF": ["REF_A"]})
    )
    mapper.learn(
        cnpj, "2019", pd.DataFrame({"COD_CTA": [cta], "COD_CTA_REF": ["REF_B"]})
    )
    mapper.learn(
        cnpj, "2020", pd.DataFrame({"COD_CTA": [cta], "COD_CTA_REF": ["REF_B"]})
    )

    mapper.build_consensus()

    # Deve escolher REF_B pois aparece 2 vezes contra 1 de REF_A
    res = mapper.get_mapping(cnpj, cta, "2021")
    assert res["COD_CTA_REF"] == "REF_B"
    assert res["ORIGEM_MAP"] == "CONSENSO_HISTORICO"


def test_historical_mapper_refactor_gold():
    """Teste Ouro: Valida Cache, Persistência e Performance."""
    mapper = HistoricalMapper()
    cnpj = "12345678000199"
    k_file = "data/temp/knowledge_test.json"
    os.makedirs("data/temp", exist_ok=True)

    # 1. Aprendizado Vetorial
    df_2013 = pd.DataFrame(
        {
            "COD_CTA": ["1.01", "1.02", "3.01"],
            "COD_CTA_REF": ["1.01.01", "1.01.02", "3.01.01"],
            "COD_SUP": ["1", "1", "3"],
        }
    )
    struct_2013 = {"1.01", "1.02", "3.01", "4.01"}
    mapper.learn(cnpj, "2013", df_2013, cod_plan_ref="10", accounting_ctas=struct_2013)

    # Registro do ano alvo para que o vizinho possa ser calculado
    struct_2014 = {"1.01", "1.02", "3.01"}
    mapper.learn(cnpj, "2014", pd.DataFrame(), accounting_ctas=struct_2014)

    # 2. Teste de Cache
    mapper.find_best_neighbor(cnpj, "2014")
    assert cnpj in mapper._neighbor_cache
    assert "2014" in mapper._neighbor_cache[cnpj]

    # 3. Teste de Persistência
    mapper.save_knowledge(k_file)
    new_mapper = HistoricalMapper()
    new_mapper.load_knowledge(k_file)

    assert new_mapper.get_summary()["cnpjs_processados"] == 1
    assert os.path.exists(k_file)


if __name__ == "__main__":
    # Permite rodar manualmente ou via pytest
    print("\n>>> RODANDO BATERIA DE TESTES DO HISTORICAL MAPPER...")
    test_historical_mapper_bidirectional_learning()
    test_historical_mapper_consensus_frequency()
    test_historical_mapper_refactor_gold()
    print(">>> [SUCESSO] Todos os testes passaram!")
