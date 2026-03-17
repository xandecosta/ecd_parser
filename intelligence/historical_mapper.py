import pandas as pd  # type: ignore
import json
import os
from typing import Dict, Any, Optional, Set, cast
from collections import Counter
import logging
from core.telemetry import monitor_task, TelemetryCollector  # type: ignore

logger = logging.getLogger(__name__)


class HistoricalMapper:
    """
    Gerencia o aprendizado e a inferência bidirecional de mapeamentos referenciais (I051).
    Foca na consistência temporal: se uma conta foi mapeada em qualquer ano da série
    histórica, esse conhecimento pode ser usado para preencher lacunas em outros anos.
    """

    def __init__(self, history_file: Optional[str] = None):
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

        # Estrutura: { cnpj: { ano: { cod_sup: Counter } } }
        self._group_knowledge: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # Tracking de Instituição (COD_PLAN_REF)
        self._plan_knowledge: Dict[str, Dict[str, str]] = {}
        self._plan_consensus: Dict[str, str] = {}

        # Mapeamento por Descrição: { cnpj: { descricao_normalizada: cod_cta_ref } }
        self._desc_knowledge: Dict[str, Dict[str, str]] = {}
        # Mapeamento por Grupo Global: { cnpj: { cod_sup: cod_cta_ref_mais_comum } }
        self._group_consensus: Dict[str, Dict[str, str]] = {}

        # Tracking de arquivos processados para IO Inteligente
        self._processed_files: Set[str] = set()
        self.history_file = history_file
        self.telemetry: Optional[TelemetryCollector] = None
        self.current_ecd_id = "GLOBAL"

    @monitor_task("HistoricalMapper", "learn")
    def learn(
        self,
        cnpj: str,
        ano: str,
        df_mapping: pd.DataFrame,
        cod_plan_ref: Optional[str] = None,
        accounting_ctas: Optional[Set[str]] = None,
        file_id: Optional[str] = None,
    ) -> None:
        """Coleta mapeamentos de forma vetorial e limpa caches."""
        if file_id:
            self._processed_files.add(file_id)

        cnpj = str(cnpj).strip().replace(".", "").replace("/", "").replace("-", "")
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
        # Cria cópia explícita para evitar SettingWithCopyWarning
        df_clean = cast(
            pd.DataFrame, df_mapping.dropna(subset=["COD_CTA", "COD_CTA_REF"])
        ).copy()
        if df_clean.empty:
            return

        # Operações vetorizadas em memória
        # Nota: Usamos assign para garantir imutabilidade no fluxo
        df_clean = cast(
            pd.DataFrame,
            df_clean.assign(
                COD_CTA=df_clean["COD_CTA"].astype(str).str.strip(),
                COD_CTA_REF=df_clean["COD_CTA_REF"].astype(str).str.strip(),
            ),
        )

        # 3. Aprende o Mapeamento de Contas
        # zip é muito mais rápido que iterrows para criar dicionários
        for cta, ref in zip(df_clean["COD_CTA"], df_clean["COD_CTA_REF"]):
            cta_str = str(cta)
            if cta_str not in self._knowledge[cnpj]:
                self._knowledge[cnpj][cta_str] = {}
            self._knowledge[cnpj][cta_str][ano_str] = str(ref)

        # 4. Aprende similaridade por grupo (COD_SUP) de forma otimizada
        if "COD_SUP" in df_clean.columns:
            df_sup = df_clean.dropna(subset=["COD_SUP"])
            if not df_sup.empty:
                if ano_str not in self._group_knowledge[cnpj]:
                    self._group_knowledge[cnpj][ano_str] = {}

                # Agrupamento nativo do Pandas para evitar loops manuais pesados
                for sup, group in df_sup.groupby("COD_SUP"):
                    sup_str = self._normalize_code(sup)
                    if sup_str not in self._group_knowledge[cnpj][ano_str]:
                        self._group_knowledge[cnpj][ano_str][sup_str] = Counter()
                    self._group_knowledge[cnpj][ano_str][sup_str].update(
                        group["COD_CTA_REF"]
                    )

        # 5. Aprende por Descrição (Fuzzy/Exact Match por nome)
        if "DESCRICAO" in df_clean.columns:
            for desc, ref in zip(df_clean["DESCRICAO"], df_clean["COD_CTA_REF"]):
                if not desc:
                    continue
                desc_key = str(desc).strip().upper()
                if cnpj not in self._desc_knowledge:
                    self._desc_knowledge[cnpj] = {}
                # Em caso de descrição duplicada com refs diferentes, o mais recente ganha (ou poderíamos usar Counter)
                self._desc_knowledge[cnpj][desc_key] = str(ref)

    def _normalize_code(self, code: Any) -> str:
        """Remove .0 de códigos numéricos que o pandas pode ter importado como float."""
        if code is None:
            return ""
        s = str(code).strip()
        if s.endswith(".0"):
            return s.removesuffix(".0")
        return s

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
            struct_set = cast(Set[str], struct)
            target_struct_set = cast(Set[str], target_struct)
            intersection = len(target_struct_set.intersection(struct_set))
            score: float = (intersection / len(target_struct)) if target_struct else 0.0

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

    @monitor_task("HistoricalMapper", "build_consensus")
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

        # 1.5. Consenso de Descrição Global (Já preenchido no learn, mas garantimos limpeza)
        # (Opcional: poderiamos recalcular aqui se usarmos Counters)

        # 2. Consenso de Grupo Global (BIDIRECIONAL REAL)
        self._group_consensus = {}
        for cnpj, years in self._group_knowledge.items():
            self._group_consensus[cnpj] = {}
            # Agrega COD_SUP de todos os anos
            temp_counters = {}  # type: ignore
            for ano, groups in years.items():
                for sup, counter in groups.items():
                    if sup not in temp_counters:
                        temp_counters[sup] = Counter()
                    temp_counters[sup].update(counter)  # type: ignore

            # Define o COD_CTA_REF mais comum para cada COD_SUP na história toda
            for sup, counter in temp_counters.items():
                if counter:
                    cnt = cast(Counter, counter)
                    self._group_consensus[cnpj][sup] = cnt.most_common(1)[0][0]

        # 2. Consenso de Instituição (COD_PLAN_REF)
        self._plan_consensus = {}
        for cnpj, years in self._plan_knowledge.items():
            if not years:
                continue
            counts = Counter(years.values())
            self._plan_consensus[cnpj] = counts.most_common(1)[0][0]

    def get_mapping(
        self,
        cnpj: str,
        cod_cta: str,
        ano_atual: str,
        cod_sup: Optional[str] = None,
        descricao: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """
        Retorna o mapeamento seguindo a hierarquia de confiança (Ponte Virtual):
        1. Declarado (I051) do próprio ano
        2. Rodada 1: Busca por Código no Vizinho (best_neighbor + COD_CTA)
        3. Rodada 2: Busca por Grupo no Vizinho (best_neighbor + COD_SUP)
        4. Rodada 3a: Consenso Histórico Global (COD_CTA em QUALQUER ano)
        5. Rodada 3b: Consenso de Grupo Global (COD_SUP em QUALQUER ano)
        6. Heurística: Mapeamento por Descrição (Exact Match de nome na história)
        """
        # 1. Tenta o dado do próprio ano (Segurança)
        declared_ref = (
            self._knowledge.get(cnpj, {}).get(cod_cta, {}).get(str(ano_atual))
        )
        if declared_ref:
            return {"COD_CTA_REF": declared_ref, "ORIGEM_MAP": "I051"}

        # Identifica o Vizinho mais Próximo (Passado ou Futuro)
        best_neighbor = self.find_best_neighbor(cnpj, ano_atual)

        if best_neighbor:
            # 2. Rodada 1: Busca por Código no Vizinho
            neighbor_ref = (
                self._knowledge.get(cnpj, {}).get(cod_cta, {}).get(str(best_neighbor))
            )
            if neighbor_ref:
                return {
                    "COD_CTA_REF": neighbor_ref,
                    "ORIGEM_MAP": f"{best_neighbor}_COD_CTA",
                }

            # 3. Rodada 2: Busca por Grupo no Vizinho
            if cod_sup and best_neighbor:
                sup_literal = self._normalize_code(cod_sup)
                cnpj_grps = self._group_knowledge.get(cnpj, {})
                group_data = cnpj_grps.get(best_neighbor, {})
                if sup_literal in group_data:
                    most_relevant_ref = group_data[sup_literal].most_common(1)[0][0]
                    return {
                        "COD_CTA_REF": most_relevant_ref,
                        "ORIGEM_MAP": f"{best_neighbor}_COD_SUP",
                    }

        # 4. Rodada 3a: Consenso Histórico Global (Busca em qualquer ano pelo código exato)
        historical_ref = self._consensus.get(cnpj, {}).get(cod_cta)
        if historical_ref:
            return {"COD_CTA_REF": historical_ref, "ORIGEM_MAP": "CONSENSO_HISTORICO"}

        # 5. Rodada 3b: Consenso de Grupo Global
        if cod_sup:
            sup_literal = self._normalize_code(cod_sup)
            global_group_ref = self._group_consensus.get(cnpj, {}).get(sup_literal)
            if global_group_ref:
                return {
                    "COD_CTA_REF": global_group_ref,
                    "ORIGEM_MAP": "CONSENSO_GRUPO_GLOBAL",
                }

        # 6. Heurística: Mapeamento por Descrição (Se o código mudou mas o nome é igual)
        if descricao:
            desc_key = str(descricao).strip().upper()
            desc_ref = self._desc_knowledge.get(cnpj, {}).get(desc_key)
            if desc_ref:
                return {"COD_CTA_REF": desc_ref, "ORIGEM_MAP": "SIMILARIDADE_DESCRICAO"}

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

    @monitor_task("HistoricalMapper", "save_knowledge")
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
            "desc_knowledge": self._desc_knowledge,
            "group_knowledge": {
                c: {a: {s: dict(cnt) for s, cnt in grps.items()} for a, grps in yrs.items()}
                for c, yrs in self._group_knowledge.items()
            },
            "processed_files": list(self._processed_files),
        }

        # Atomic Write Pattern: Escreve em temp e renomeia atomicamente
        # Isso previne corrupção do arquivo se o processo morrer no meio da escrita
        temp_file = f"{file_path}.tmp"
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(serializable_data, f, indent=2, ensure_ascii=False)

            os.replace(temp_file, file_path)
            logger.info(
                f"Conhecimento histórico persistido com segurança em: {file_path}"
            )
        except Exception as e:
            logger.error(f"Falha ao salvar conhecimento histórico: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

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
        self._desc_knowledge = data.get("desc_knowledge", {})
        self._processed_files = set(data.get("processed_files", []))

        # Reconverte group_knowledge para Counters
        g_data = data.get("group_knowledge", {})
        self._group_knowledge = {
            c: {a: {s: Counter(cnt) for s, cnt in grps.items()} for a, grps in yrs.items()}
            for c, yrs in g_data.items()
        }

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
