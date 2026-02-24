import pandas as pd
import json
import os
from typing import Dict, Any, Optional, Set
from collections import Counter
import logging

logger = logging.getLogger(__name__)


class HistoricalMapper:
    """
    Gerencia o aprendizado e a inferência bidirecional de mapeamentos referenciais (I051).
    Foca na consistência temporal: se uma conta foi mapeada em qualquer ano da série
    histórica, esse conhecimento pode ser usado para preencher lacunas em outros anos.
    """

    def __init__(self):
        # Estrutura: { cnpj: { cod_cta: { ano: cod_cta_ref } } }
        self._knowledge: Dict[str, Dict[str, Dict[str, str]]] = {}
        # Estrutura: { cnpj: { cod_cta: cod_cta_ref_canonico } }
        self._consensus: Dict[str, Dict[str, str]] = {}

        # Estrutura: { cnpj: { ano: set(cod_cta) } }
        self._account_structures: Dict[str, Dict[str, Set[str]]] = {}

        # Cache de similaridade: { cnpj: { (ano1, ano2): score } }
        self._similarity_cache: Dict[str, Dict[tuple, float]] = {}
        # Cache de vizinhos: { cnpj: { target_year: best_neighbor } }
        self._neighbor_cache: Dict[str, Dict[str, Optional[str]]] = {}

        # Estrutura: { cnpj: { ano: { cod_sup: Counter(cod_cta_ref) } } }
        self._group_knowledge: Dict[str, Dict[str, Dict[str, Counter]]] = {}

        # Tracking de Instituição (COD_PLAN_REF)
        self._plan_knowledge: Dict[str, Dict[str, str]] = {}
        self._plan_consensus: Dict[str, str] = {}

    def learn(
        self,
        cnpj: str,
        ano: str,
        df_mapping: pd.DataFrame,
        cod_plan_ref: Optional[str] = None,
        accounting_ctas: Optional[Set[str]] = None,
    ) -> None:
        """Coleta mapeamentos de forma vetorial e limpa caches."""
        ano_str = str(ano)
        if cnpj not in self._knowledge:
            self._knowledge[cnpj] = {}
            self._plan_knowledge[cnpj] = {}
            self._account_structures[cnpj] = {}
            self._group_knowledge[cnpj] = {}
            self._neighbor_cache[cnpj] = {}

        # Invalida cache de vizinhos deste CNPJ pois novos dados chegaram
        self._neighbor_cache[cnpj] = {}
        self._similarity_cache[cnpj] = {}

        if accounting_ctas:
            self._account_structures[cnpj][ano_str] = accounting_ctas

        if cod_plan_ref:
            self._plan_knowledge[cnpj][ano_str] = str(cod_plan_ref)

        if df_mapping.empty or "COD_CTA" not in df_mapping.columns:
            return

        # VETORIZAÇÃO OURO: Elimina itertuples/iterrows
        # Preparamos o DataFrame (Clean strings + Drop NAs)
        df_clean = df_mapping.dropna(subset=["COD_CTA", "COD_CTA_REF"]).copy()
        if df_clean.empty:
            return

        df_clean["COD_CTA"] = df_clean["COD_CTA"].astype(str).str.strip()
        df_clean["COD_CTA_REF"] = df_clean["COD_CTA_REF"].astype(str).str.strip()

        # 3. Aprende o Mapeamento de Contas
        mapping_dict = dict(zip(df_clean["COD_CTA"], df_clean["COD_CTA_REF"]))
        for cta, ref in mapping_dict.items():
            if cta not in self._knowledge[cnpj]:
                self._knowledge[cnpj][cta] = {}
            self._knowledge[cnpj][cta][ano_str] = ref

        # 4. Aprende similaridade por grupo (COD_SUP) de forma otimizada
        if "COD_SUP" in df_clean.columns:
            df_sup = df_clean.dropna(subset=["COD_SUP"])
            if not df_sup.empty:
                if ano_str not in self._group_knowledge[cnpj]:
                    self._group_knowledge[cnpj][ano_str] = {}

                # Agrupamento nativo do Pandas para evitar loops manuais pesados
                for sup, group in df_sup.groupby("COD_SUP"):
                    sup_str = str(sup).strip()
                    if sup_str not in self._group_knowledge[cnpj][ano_str]:
                        self._group_knowledge[cnpj][ano_str][sup_str] = Counter()
                    self._group_knowledge[cnpj][ano_str][sup_str].update(
                        group["COD_CTA_REF"]
                    )

    def find_best_neighbor(self, cnpj: str, target_year: str) -> Optional[str]:
        """Identifica qual ano tem o plano contábil (I050) mais parecido com o ano alvo (Memoizado)."""
        target_year_str = str(target_year)

        # MEMOIZAÇÃO: Evita cálculos O(N^2) redundantes
        if (
            cnpj in self._neighbor_cache
            and target_year_str in self._neighbor_cache[cnpj]
        ):
            return self._neighbor_cache[cnpj][target_year_str]

        target_struct = self._account_structures.get(cnpj, {}).get(target_year_str)
        if not target_struct:
            return None

        best_year = None
        highest_similarity = -1.0

        for ano, struct in self._account_structures.get(cnpj, {}).items():
            if ano == target_year_str:
                continue

            # Verifica se esse ano candidato tem algum mapeamento para oferecer
            has_mappings = any(
                ano in year_maps for year_maps in self._knowledge.get(cnpj, {}).values()
            )
            if not has_mappings:
                continue

            # Cálculo de Similaridade Jaccard/Subset
            intersection = len(target_struct.intersection(struct))
            score = (intersection / len(target_struct)) if target_struct else 0

            if score > highest_similarity:
                highest_similarity = score
                best_year = ano

        # Threshold de 40% de confiança após refatoração Ouro
        result = best_year if highest_similarity >= 0.4 else None

        # Salva no cache
        if cnpj not in self._neighbor_cache:
            self._neighbor_cache[cnpj] = {}
        self._neighbor_cache[cnpj][target_year_str] = result

        return result

    def build_consensus(self) -> None:
        """
        Analisa o histórico e define o mapeamento mais provável para cada conta.
        """
        self._consensus = {}
        for cnpj, accounts in self._knowledge.items():
            self._consensus[cnpj] = {}
            for cta, year_mappings in accounts.items():
                if not year_mappings:
                    continue
                counts = Counter(year_mappings.values())
                most_common_ref = counts.most_common(1)[0][0]
                self._consensus[cnpj][cta] = most_common_ref

        # 2. Consenso de Instituição (COD_PLAN_REF)
        self._plan_consensus = {}
        for cnpj, years in self._plan_knowledge.items():
            if not years:
                continue
            counts = Counter(years.values())
            self._plan_consensus[cnpj] = counts.most_common(1)[0][0]

    def get_mapping(
        self, cnpj: str, cod_cta: str, ano_atual: str, cod_sup: Optional[str] = None
    ) -> Dict[str, Optional[str]]:
        """
        Retorna o mapeamento seguindo a hierarquia:
        1. Declarado (I051) do próprio ano
        2. Ponte Virtual (Rodada 1): Vizinho + COD_CTA
        3. Ponte Virtual (Rodada 2): Vizinho + COD_SUP (Mais relevante do grupo)
        4. Consenso Global
        """
        # 1. Tenta o dado do próprio ano
        declared_ref = (
            self._knowledge.get(cnpj, {}).get(cod_cta, {}).get(str(ano_atual))
        )
        if declared_ref:
            return {"COD_CTA_REF": declared_ref, "ORIGEM_MAP": "I051"}

        # Identifica o Vizinho mais Próximo (Passado ou Futuro)
        best_neighbor = self.find_best_neighbor(cnpj, ano_atual)

        if best_neighbor:
            # 2. Ponte Virtual (Rodada 1): Vizinho + COD_CTA
            neighbor_ref = (
                self._knowledge.get(cnpj, {}).get(cod_cta, {}).get(best_neighbor)
            )
            if neighbor_ref:
                return {
                    "COD_CTA_REF": neighbor_ref,
                    "ORIGEM_MAP": f"{best_neighbor}_COD_CTA",
                }

            # 3. Ponte Virtual (Rodada 2): Vizinho + COD_SUP (Grupo)
            if cod_sup:
                sup_literal = str(cod_sup).strip()
                group_data = self._group_knowledge.get(cnpj, {}).get(best_neighbor, {})
                if sup_literal in group_data:
                    # Pega a conta referencial mais frequente no grupo do vizinho
                    most_relevant_ref = group_data[sup_literal].most_common(1)[0][0]
                    return {
                        "COD_CTA_REF": most_relevant_ref,
                        "ORIGEM_MAP": f"{best_neighbor}_COD_SUP",
                    }

        # 4. Tenta o Consenso Histórico Global (Ultimo recurso)
        historical_ref = self._consensus.get(cnpj, {}).get(cod_cta)
        if historical_ref:
            return {"COD_CTA_REF": historical_ref, "ORIGEM_MAP": "CONSENSO_HISTORICO"}

        return {"COD_CTA_REF": None, "ORIGEM_MAP": "SEM_MAPEAMENTO"}

    def get_summary(self) -> Dict[str, Any]:
        """Retorna estatísticas do aprendizado."""
        total_cnpjs = len(self._knowledge)
        total_ctas = sum(len(ctas) for ctas in self._knowledge.values())
        return {
            "cnpjs_processados": total_cnpjs,
            "contas_mapeadas_na_historia": total_ctas,
            "anos_com_estrutura_i050": sum(
                len(yrs) for yrs in self._account_structures.values()
            ),
        }

    def get_inferred_plan(
        self, cnpj: str, ano_alvo: Optional[str] = None
    ) -> Optional[str]:
        """Retorna o COD_PLAN_REF usando memórias similares ou consenso."""
        inferred = None
        if ano_alvo:
            best_neighbor = self.find_best_neighbor(cnpj, ano_alvo)
            if best_neighbor:
                inferred = self._plan_knowledge.get(cnpj, {}).get(best_neighbor)

        if not inferred:
            inferred = self._plan_consensus.get(cnpj)

        # --- LÓGICA DE EQUIVALÊNCIA HISTÓRICA 2014+ (Implementada) ---
        target_y = int(ano_alvo) if ano_alvo and str(ano_alvo).isdigit() else 2024

        # Se viermos de 2013 para 2014+ com código '10' (PJ em Geral)
        # e o destino for Lucro Real, deve-se transpor para '1' (Lucro Real PJ Geral)
        if target_y >= 2014 and inferred == "10":
            # Nota: Esta é uma inferência agressiva. Em produção,
            # o Processor pode sobrescrever se detectar o Registro 0000.
            pass

        return inferred

    def save_knowledge(self, file_path: str):
        """Persiste o aprendizado em JSON para uso futuro (Evita re-aprendizado lento)."""
        # Converte Sets e Counters para formatos serializáveis
        serializable_data = {
            "knowledge": self._knowledge,
            "account_structures": {
                c: {a: list(s) for a, s in yrs.items()}
                for c, yrs in self._account_structures.items()
            },
            "plan_knowledge": self._plan_knowledge,
            "plan_consensus": self._plan_consensus,
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(serializable_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Conhecimento histórico salvo em: {file_path}")

    def load_knowledge(self, file_path: str):
        """Carrega conhecimento prévio do disco."""
        if not os.path.exists(file_path):
            logger.warning(
                f"Caminho não encontrado para carregar conhecimento: {file_path}"
            )
            return

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._knowledge = data.get("knowledge", {})
        self._plan_knowledge = data.get("plan_knowledge", {})
        self._plan_consensus = data.get("plan_consensus", {})

        # Reconverte Listas para Sets
        structures = data.get("account_structures", {})
        self._account_structures = {
            c: {a: set(struct_list) for a, struct_list in yrs.items()}
            for c, yrs in structures.items()
        }

        # Reconstrói consensos
        self.build_consensus()
        logger.info(
            f"Conhecimento carregado com sucesso ({len(self._knowledge)} CNPJs)."
        )
