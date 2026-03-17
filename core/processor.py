import os
import json
import pandas as pd # type: ignore
import logging
import numpy as np # type: ignore

from typing import Dict, List, Any, Optional, cast
from core.telemetry import monitor_task, TelemetryCollector

# Logger local para uso interno do módulo (não configura nível globalmente)
logger = logging.getLogger(__name__)


class ECDProcessor:
    """
    Motor de Processamento de Dados ECD (SPED-Contábil) com Auditoria Integrada.
    """

    def __init__(
        self,
        registros: List[Dict[str, Any]],
        cnpj: str = "",
        layout_versao: str = "",
        knowledge_base: Optional[Any] = None,
    ):
        self.df_bruto = pd.DataFrame(registros) if registros else pd.DataFrame()
        self.cnpj = cnpj
        self.layout_versao = layout_versao
        self.knowledge_base = knowledge_base
        self.blocos: Dict[str, pd.DataFrame] = {}
        self.cod_plan_ref: Optional[str] = None
        self.ano_vigencia: Optional[int] = None
        self.telemetry: Optional[TelemetryCollector] = None
        self.current_ecd_id = ""

        # --- Cache interno (evita reprocessamento dentro do mesmo ECD) ---
        self._cache_plano: Optional[pd.DataFrame] = None
        self._cache_lancamentos: Optional[pd.DataFrame] = None

        # Path para o catálogo de planos referenciais
        self.catalog_path = os.path.normpath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "schemas",
                "ref_plans",
                "ref_catalog.json",
            )
        )

        if not self.df_bruto.empty:
            self._separar_blocos()
            self._identificar_metadados_referenciais()

    def _obter_arquivos_referenciais(self) -> List[str]:
        """
        Localiza todos os arquivos CSV (Balanço, DRE, etc) no ref_catalog.json
        para a instituição e ano vigentes.
        """
        if not self.cod_plan_ref or not self.ano_vigencia:
            return []

        if not os.path.exists(self.catalog_path):
            logger.error(f"Catálogo não encontrado: {self.catalog_path}")
            return []

        try:
            with open(self.catalog_path, "r", encoding="utf-8") as f:
                catalog = json.load(f)

            # 1. Filtro Instituição
            inst = catalog.get(str(self.cod_plan_ref))
            if not inst:
                return []

            # 2. Filtro Vigência (Range)
            período_escolhido = None
            períodos_disponíveis = []

            for key, info in inst.items():
                r_min, r_max = info.get("range", [0, 0])
                períodos_disponíveis.append((r_min, r_max, info))
                if r_min <= self.ano_vigencia <= r_max:
                    período_escolhido = info
                    break

            # Se não achou vigência exata, busca o período mais próximo (abordagem cross-temporal)
            if not período_escolhido and períodos_disponíveis:
                logger.info(
                    f"Ano {self.ano_vigencia} não mapeado para plano {self.cod_plan_ref}. "
                    "Buscando período referencial compatível..."
                )
                períodos_disponíveis.sort(key=lambda x: abs(x[1] - self.ano_vigencia))
                período_escolhido = períodos_disponíveis[0][2]

            if not período_escolhido:
                return []

            # 3. Coleta todos os arquivos de planos disponíveis para o período
            arquivos = []
            plans = período_escolhido.get("plans", {})
            for alias in plans:
                # Pega a maior versão disponível para cada alias (L100, L300, etc)
                versões = sorted(
                    plans[alias].keys(), key=lambda v: int(v), reverse=True
                )
                if versões:
                    v_top = versões[0]
                    filename = plans[alias][v_top].get("file")
                    if filename:
                        caminho = os.path.normpath(
                            os.path.join(
                                os.path.dirname(self.catalog_path), "data", filename
                            )
                        )
                        if os.path.exists(caminho):
                            arquivos.append(caminho)

            return arquivos

        except Exception as e:
            logger.error(f"Erro ao consultar catálogo: {e}")
            return []

    def _separar_blocos(self) -> None:
        """Divide os registros por REG e limpa prefixos redundantes."""
        versoes_reg = self.df_bruto["REG"].unique()
        for reg in versoes_reg:
            df_reg = (
                self.df_bruto[self.df_bruto["REG"] == reg]
                .dropna(axis=1, how="all")
                .copy()
            )
            if "REG" in df_reg.columns:
                df_reg.drop(columns=["REG"], inplace=True)

            prefixo = f"{reg}_"
            df_reg = df_reg.rename(columns=lambda c: str(c).removeprefix(prefixo) if str(c).startswith(prefixo) else c)

            # Remove duplicatas de colunas que possam surgir na renomeação
            self.blocos[f"dfECD_{reg}"] = df_reg.loc[
                :, ~df_reg.columns.duplicated()
            ]

    @monitor_task("ECDProcessor", "_identificar_metadados_referenciais")
    def _identificar_metadados_referenciais(self) -> None:
        """Determina o Ano e o Código da Instituição (Funil de Metadados)."""
        df_0000 = self.blocos.get("dfECD_0000")
        if df_0000 is None or df_0000.empty:
            return

        # 1. Identificação do Ano (DT_FIN) - Comum a todas as versões
        val_0000 = df_0000.iloc[0]
        dt_fin = val_0000.get("DT_FIN")
        
        try:
            if isinstance(dt_fin, pd.Timestamp) or hasattr(dt_fin, "year"):
                self.ano_vigencia = int(getattr(dt_fin, "year"))
            elif isinstance(dt_fin, str) and len(dt_fin) >= 8:
                # Tenta DDMMYYYY ou YYYYMMDD
                s_dt_fin: str = str(dt_fin)
                self.ano_vigencia = (
                    int(s_dt_fin[:4])
                    if int(s_dt_fin[:4]) > 1900
                    else int(s_dt_fin[-4:])
                )
        except (ValueError, IndexError):
            pass

        # 1.5. Sincronização de CNPJ Ouro: Se self.cnpj está vazio, tenta buscar no Bloco 0000
        if not self.cnpj or str(self.cnpj).strip() == "":
            val_cnpj = df_0000.iloc[0].get("CNPJ") or df_0000.iloc[0].get("0000_CNPJ")
            if val_cnpj and str(val_cnpj).strip() != "":
                self.cnpj = str(val_cnpj).strip()
                logger.info(f"CNPJ recuperado via Bloco 0000: {self.cnpj}")

        # 2. Identificação do COD_PLAN_REF (Condicional por Versão)
        try:
            versao_num = float(str(self.layout_versao).replace(",", ".")) if self.layout_versao else 0.0
        except ValueError:
            versao_num = 0.0

        if versao_num >= 8.0:
            # Moderno: Está no 0000
            df_ref = cast(pd.DataFrame, df_0000)
            self.cod_plan_ref = str(df_ref.iloc[0].get("COD_PLAN_REF", "")) # type: ignore
        else:
            # Legado: Está no primeiro I051
            df_i051 = self.blocos.get("dfECD_I051")
            if df_i051 is not None and not df_i051.empty:
                self.cod_plan_ref = str(df_i051.iloc[0].get("COD_PLAN_REF", ""))

        if not self.cod_plan_ref:
            # --- NÍVEL 1.5: Inferência de Instituição ---
            kb = self.knowledge_base
            if kb is not None and hasattr(kb, "get_inferred_plan"):
                inferred = kb.get_inferred_plan(
                    self.cnpj, ano_alvo=str(self.ano_vigencia)
                )
                if inferred:
                    self.cod_plan_ref = str(inferred)
                    logger.info(
                        f"COD_PLAN_REF inferido via histórico: {self.cod_plan_ref}"
                    )

        if not self.cod_plan_ref:
            logger.warning(
                f"COD_PLAN_REF não localizado (Versão: {self.layout_versao}). "
                "O mapeamento RFB pode falhar."
            )

    @staticmethod
    def _to_float(valor: Any) -> float:
        """
        Converte um valor para float64 de forma segura.
        Substitui _converter_decimal para operações vetoriais internas.
        Precision: float64 tem 15-17 dígitos significativos — suficiente
        para qualquer valor contábil real (até ~R$ 100 trilhões com centavos).
        """
        if valor is None:
            return 0.0
        try:
            f = float(str(valor).replace(",", ".").strip())
            return 0.0 if np.isnan(f) else f
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _series_to_float(s: Any) -> pd.Series:
        """Converte uma Series inteira para float64 vetorialmente (sem .apply)."""
        return pd.to_numeric(s, errors="coerce").fillna(0.0) # type: ignore

    @monitor_task("ECDProcessor", "processar_plano_contas")
    def processar_plano_contas(self) -> pd.DataFrame:
        """Processa o Plano de Contas da Empresa (I050) integrado com o Referencial (I051)."""
        # --- Cache: retorna imediatamente se já processado neste ECD ---
        if self._cache_plano is not None:
            return self._cache_plano

        df_i050 = self.blocos.get("dfECD_I050")
        df_i051 = self.blocos.get("dfECD_I051")

        if df_i050 is None:
            return pd.DataFrame()

        # Seleciona colunas básicas do I050 e NORMALIZA (remove prefixos se houver)
        map_cols = {c: c.replace("I050_", "") for c in df_i050.columns}
        df_res = df_i050.rename(columns=map_cols).copy()

        cols_essenciais = [
            "PK",
            "COD_NAT",
            "IND_CTA",
            "NIVEL",
            "COD_CTA",
            "COD_CTA_SUP",
            "CTA",
        ]
        df_res = df_res[[c for c in cols_essenciais if c in df_res.columns]]

        # Integração com I051 (Mapeamento Referencial)
        if df_i051 is not None and not df_i051.empty:
            # Normaliza colunas do I051 também
            df_ref = df_i051.rename(columns=lambda x: x.replace("I051_", "")).copy()
            df_ref = df_ref[["FK_PAI", "COD_CTA_REF"]]

            # Left join para garantir que não perdemos contas sintéticas do I050
            df_res = pd.merge(
                df_res, df_ref, left_on="PK", right_on="FK_PAI", how="left"
            )
            df_res.drop(columns=["FK_PAI"], inplace=True, errors="ignore")
        else:
            df_res["COD_CTA_REF"] = None

        df_res["CNPJ"] = self.cnpj

        # --- NOVO FLUXO DE ROTULAGEM (Anti-NaN) ---
        # 1. Inicializamos a coluna para evitar nulos em arquivos híbridos
        df_res["ORIGEM_MAP"] = "SEM_MAPEAMENTO"

        # 2. Marcamos quem já veio declarado via I051 no arquivo atual
        s_ref_raw = cast(pd.Series, df_res["COD_CTA_REF"])
        mask_declarado_original = s_ref_raw.notna() & (
            s_ref_raw.astype(str).str.strip() != ""
        )
        df_res.loc[mask_declarado_original, "ORIGEM_MAP"] = "I051"

        # 3. Executamos a Inferência Histórica apenas para as lacunas remanescentes
        if self.knowledge_base is not None:
            s_ref = cast(pd.Series, df_res["COD_CTA_REF"])
            mask_vazio = s_ref.isna() | (
                s_ref.astype(str).str.strip() == ""
            )
            s_ind = cast(pd.Series, df_res["IND_CTA"])
            mask_analitica = (
                s_ind.astype(str).str.upper() == "A"
            )
            mask_alvo = mask_vazio & mask_analitica

            if mask_alvo.any():
                ano_str = str(self.ano_vigencia) if self.ano_vigencia else ""

                # OTIMIZAÇÃO VETORIAL OURO: Zipping lists >> DataFrame.apply()
                cod_ctas = df_res.loc[mask_alvo, "COD_CTA"].astype(str).tolist()
                cod_sups = df_res.loc[mask_alvo, "COD_CTA_SUP"].astype(str).tolist()
                descs = df_res.loc[mask_alvo, "CTA"].astype(str).tolist() if "CTA" in df_res.columns else [None] * len(cod_ctas)
                
                refs = []
                origens = []
                kb = self.knowledge_base
                
                if kb is not None and hasattr(kb, "get_mapping"):
                    for cta, sup, desc in zip(cod_ctas, cod_sups, descs):
                        if not cta:
                            refs.append(None)
                            origens.append("SEM_COD_CTA")
                        else:
                            vinculo = kb.get_mapping(self.cnpj, cta, ano_str, cod_sup=sup, descricao=desc)
                            refs.append(vinculo.get("COD_CTA_REF"))
                            origens.append(vinculo.get("ORIGEM_MAP"))

                df_res.loc[mask_alvo, "COD_CTA_REF"] = refs
                df_res.loc[mask_alvo, "ORIGEM_MAP"] = origens

        # 4. Limpeza final: ORIGEM_MAP deve ser vazio para contas SINTÉTICAS
        s_ind_final = cast(pd.Series, df_res["IND_CTA"])
        mask_sintetica = (
            s_ind_final.astype(str).str.upper() != "A"
        )
        df_res.loc[mask_sintetica, "ORIGEM_MAP"] = ""

        if "CTA" in df_res.columns:
            df_res["CONTA"] = (
                df_res["COD_CTA"].astype(str)
                + " - "
                + df_res["CTA"].astype(str).str.strip().str.upper() # type: ignore
            )
        # --- Cache: salva resultado para reuso dentro do mesmo ECD ---
        self._cache_plano = df_res # type: ignore
        return self._cache_plano # type: ignore

    @monitor_task("ECDProcessor", "processar_lancamentos")
    def processar_lancamentos(self, df_plano: pd.DataFrame) -> pd.DataFrame:
        """Processa Lançamentos Contábeis (I200/I250)."""
        # --- Cache: retorna imediatamente se já processado neste ECD ---
        if self._cache_lancamentos is not None:
            return self._cache_lancamentos

        df_i200 = self.blocos.get("dfECD_I200")
        df_i250 = self.blocos.get("dfECD_I250")
        if df_i200 is None or df_i250 is None:
            return pd.DataFrame()

        df_lctos = pd.merge(
            df_i200[["PK", "NUM_LCTO", "DT_LCTO", "IND_LCTO"]],
            df_i250,
            left_on="PK",
            right_on="FK_PAI",
        )

        df_lctos["CNPJ"] = self.cnpj

        # --- Otimização: substituição de .apply(Decimal) por operações vetoriais ---
        vl_dc = self._series_to_float(df_lctos["VL_DC"])
        ind_d = df_lctos["IND_DC"] == "D"
        ind_c = df_lctos["IND_DC"] == "C"

        df_lctos["VL_D"] = np.where(ind_d, vl_dc, 0.0)
        df_lctos["VL_C"] = np.where(ind_c, vl_dc, 0.0)
        df_lctos["VL_SINAL"] = df_lctos["VL_D"] - df_lctos["VL_C"]

        if not df_plano.empty:
            df_lctos = pd.merge(
                df_lctos, df_plano[["COD_CTA", "CONTA"]], on="COD_CTA", how="left"
            )

        self._cache_lancamentos = df_lctos
        return df_lctos

    @monitor_task("ECDProcessor", "gerar_balancetes")
    def gerar_balancetes(self) -> Dict[str, pd.DataFrame]:
        """
        Gera balancetes com Forward Roll, Reversão de Encerramento.
        """
        df_plano = self.processar_plano_contas()
        df_i150 = self.blocos.get("dfECD_I150")
        df_i155 = self.blocos.get("dfECD_I155")
        df_i157 = self.blocos.get("dfECD_I157")  # Transferência de Plano de Contas

        if df_plano.empty or df_i150 is None or df_i155 is None:
            return {}

        # 1. Base Unificada de Saldos
        df_base = pd.merge(
            df_i150[["PK", "DT_FIN"]], df_i155, left_on="PK", right_on="FK_PAI"
        )
        df_base["CNPJ"] = self.cnpj

        # 2. Sinais e Tipagem — Vetorizado com float64
        vl_ini = self._series_to_float(df_base["VL_SLD_INI"])
        vl_fin = self._series_to_float(df_base["VL_SLD_FIN"])
        vl_deb = self._series_to_float(df_base["VL_DEB"])
        vl_cred = self._series_to_float(df_base["VL_CRED"])

        df_base["VL_SLD_INI_SIG"] = np.where(
            df_base["IND_DC_INI"] == "D", vl_ini, -vl_ini
        )
        df_base["VL_SLD_FIN_SIG"] = np.where(
            df_base["IND_DC_FIN"] == "D", vl_fin, -vl_fin
        )
        df_base["VL_DEB"] = vl_deb
        df_base["VL_CRED"] = vl_cred

        # 3. Reversão de Encerramento (Indicator 'E')
        df_lctos = self.processar_lancamentos(df_plano)
        if not df_lctos.empty and "IND_LCTO" in df_lctos.columns:
            df_e = df_lctos[df_lctos["IND_LCTO"] == "E"].copy()
            if not df_e.empty:
                ajustes = (
                    df_e.groupby(["COD_CTA", "DT_LCTO"])
                    .agg({"VL_SINAL": "sum", "VL_D": "sum", "VL_C": "sum"})
                    .reset_index()
                )
                ajustes.rename(
                    columns={
                        "VL_SINAL": "VL_AJ_SINAL",
                        "VL_D": "VL_AJ_D",
                        "VL_C": "VL_AJ_C",
                        "DT_LCTO": "DT_FIN",
                    },
                    inplace=True,
                )

                df_base = pd.merge(
                    df_base, ajustes, on=["COD_CTA", "DT_FIN"], how="left"
                ).fillna(0.0)
                df_base["VL_SLD_FIN_SIG"] = cast(
                    pd.Series, df_base["VL_SLD_FIN_SIG"]
                ) - self._series_to_float(df_base["VL_AJ_SINAL"])
                df_base["VL_DEB"] = cast(
                    pd.Series, df_base["VL_DEB"]
                ) - self._series_to_float(df_base["VL_AJ_D"])
                df_base["VL_CRED"] = cast(
                    pd.Series, df_base["VL_CRED"]
                ) - self._series_to_float(df_base["VL_AJ_C"])

        # 4. Forward Roll (Continuidade Histórica) & I157
        df_base = df_base.sort_values(["COD_CTA", "DT_FIN"])
        df_base["VL_SLD_FIN_ANT"] = df_base.groupby("COD_CTA")["VL_SLD_FIN_SIG"].shift(
            1
        )

        # Se houver I157, aplica o mapeamento de saldos iniciais transferidos
        if df_i157 is not None:
            df_base = pd.merge(
                df_base,
                df_i157[["COD_CTA", "VL_SLD_INI", "IND_DC_INI"]],
                on="COD_CTA",
                how="left",
                suffixes=("", "_I157"),
            )
            vl_i157 = self._series_to_float(df_base["VL_SLD_INI_I157"])
            df_base["VL_I157_SIG"] = np.where(
                df_base["IND_DC_INI_I157"] == "D", vl_i157, -vl_i157
            )
            # Aplica o saldo do I157 apenas se não houver saldo anterior detectado (início da conta no novo plano)
            mask_primeiro_mes = df_base["VL_SLD_FIN_ANT"].isna()
            df_base.loc[
                mask_primeiro_mes & df_base["VL_I157_SIG"].notna(),
                "VL_SLD_INI_SIG",
            ] = df_base["VL_I157_SIG"]

        # Forward Roll: usa np.where vetorial em vez de apply por linha
        vl_ant = df_base["VL_SLD_FIN_ANT"]
        df_base["VL_SLD_INI_SIG"] = np.where(
            vl_ant.notna(),
            vl_ant,
            df_base["VL_SLD_INI_SIG"],
        )

        # 5. Propagação Hierárquica (Plano da Empresa)
        balancete_empresa = self._propagar_hierarquia(df_base, df_plano)
        if not balancete_empresa.empty:
            for col in ["VL_SLD_INI_SIG", "VL_DEB", "VL_CRED", "VL_SLD_FIN_SIG"]:
                balancete_empresa[col] = balancete_empresa[col].round(2)

        # 6. Balancete Referencial (baseRFB)
        balancete_rfb = self.gerar_balancete_referencial(df_base)
        if not balancete_rfb.empty:
            for col in ["VL_SLD_INI_SIG", "VL_DEB", "VL_CRED", "VL_SLD_FIN_SIG"]:
                balancete_rfb[col] = balancete_rfb[col].round(2)

        # 7. Limpeza e Ordenação Ouro
        def _finalizar(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return df
            # Remove colunas técnicas e duplicatas de merges
            drop_cols = ["PK", "FK_PAI", "CNPJ_x", "CNPJ_y"]
            d = df.drop(columns=[c for c in drop_cols if c in df.columns]).copy()
            
            # Limpeza Ouro: Remove separadores que quebram o CSV
            obj_cols = d.select_dtypes(include=["object"]).columns
            d[obj_cols] = d[obj_cols].fillna("").astype(str).replace({";": " ", "\n": " ", "\r": " "}, regex=True)
            
            # Garante CNPJ se estiver faltando
            if "CNPJ" not in d.columns:
                d["CNPJ"] = self.cnpj
                
            # Reordena: DT_FIN e CNPJ primeiro
            cols = ["DT_FIN", "CNPJ"] + [c for c in d.columns if c not in ["DT_FIN", "CNPJ"]]
            return d.reindex(columns=cols)

        return {
            "03_Balancetes_Mensais": _finalizar(balancete_empresa),
            "04_Balancete_baseRFB": _finalizar(balancete_rfb),
        }

    @monitor_task("ECDProcessor", "gerar_balancete_referencial")
    def gerar_balancete_referencial(self, df_saldos: pd.DataFrame) -> pd.DataFrame:
        """
        Gera o balancete na visão do Plano Referencial da RFB.
        """
        # 1. Localiza e unifica todos os blocos do Plano Referencial (Balanço + Resultado)
        caminhos = self._obter_arquivos_referenciais()
        if not caminhos:
            logger.warning(
                "Nenhum arquivo de plano referencial localizado no catálogo."
            )
            return pd.DataFrame()

        dfs_schemas = []
        for p in caminhos:
            try:
                dfs_schemas.append(pd.read_csv(p, sep="|", dtype=str, encoding="utf-8"))
            except Exception as e:
                logger.error(f"Erro ao carregar CSV referencial {p}: {e}")

        if not dfs_schemas:
            return pd.DataFrame()

        df_ref_schema = pd.concat(dfs_schemas, ignore_index=True)

        # 2. Prepara os saldos analíticos da empresa mapeados para o referencial
        df_plano = self.processar_plano_contas()
        if "COD_CTA_REF" not in df_plano.columns:
            return pd.DataFrame()

        # Join dos saldos com o mapeamento referencial
        cols_valores = ["VL_SLD_INI_SIG", "VL_DEB", "VL_CRED", "VL_SLD_FIN_SIG"]
        df_mapeado = pd.merge(
            df_saldos[cols_valores + ["COD_CTA", "DT_FIN"]],
            df_plano[["COD_CTA", "COD_CTA_REF"]],
            on="COD_CTA",
            how="inner",
        )

        # Filtra apenas registros que possuem mapeamento referencial
        df_mapeado = df_mapeado[
            df_mapeado["COD_CTA_REF"].notna()
            & (df_mapeado["COD_CTA_REF"] != "")
        ]

        if df_mapeado.empty:
            return pd.DataFrame()

        # Agrupa por Conta Referencial e Data (pois várias contas da empresa podem mapear p/ uma referencial)
        df_analitico_ref = (
            df_mapeado.groupby(["COD_CTA_REF", "DT_FIN"])[cols_valores]
            .sum()
            .reset_index()
        )

        # 3. Consolidação Hierárquica no Plano Referencial
        balancetes_rfb = []
        versoes_data = df_analitico_ref["DT_FIN"].unique()
        for data in versoes_data:
            df_mes = df_analitico_ref[df_analitico_ref["DT_FIN"] == data].copy()

            # Prepara a tabela base do mês com TODAS as contas do plano referencial
            tab = df_ref_schema.copy()
            tab = pd.merge(
                tab,
                df_mes,
                left_on="CODIGO",
                right_on="COD_CTA_REF",
                how="left",
            )

            # --- float64 vetorial: substitui apply(Decimal) ---
            for col in cols_valores:
                tab[col] = self._series_to_float(tab[col])

            # Algoritmo Bottom-Up no Plano Referencial — Vetorizado
            try:
                tab["NIVEL"] = (
                    cast(
                        pd.Series,
                        pd.to_numeric(tab["NIVEL"], errors="coerce"),
                    )
                    .fillna(0)
                    .astype(int)
                )
                niveis = sorted(tab["NIVEL"].unique(), reverse=True)

                for nivel in niveis:
                    if nivel <= 1:
                        continue

                    # Agrega filhos e renomeia COD_SUP para CODIGO (chave do pai)
                    agg = (
                        tab[tab["NIVEL"] == nivel]
                        .groupby("COD_SUP")[cols_valores]
                        .sum()
                        .reset_index()
                        .rename(columns={"COD_SUP": "CODIGO"})
                    )
                    # Sufixo para evitar conflito de colunas no merge
                    agg = agg.add_suffix("_AGG").rename(columns={"CODIGO_AGG": "CODIGO"})

                    tab = tab.merge(agg, on="CODIGO", how="left")
                    for col in cols_valores:
                        tab[col] = tab[col].add(tab[f"{col}_AGG"].fillna(0.0))
                        tab.drop(columns=[f"{col}_AGG"], inplace=True)
            except Exception as e:
                logger.error(f"Falha no rollup bottom-up do referencial: {e}")

            tab["DT_FIN"] = data
            if "COD_CTA_REF" in tab.columns:
                tab.drop(columns=["COD_CTA_REF"], inplace=True)
            balancetes_rfb.append(tab)

        return (
            pd.concat(balancetes_rfb, ignore_index=True)
            if balancetes_rfb
            else pd.DataFrame()
        )

    def _propagar_hierarquia(
        self, df_saldos: pd.DataFrame, df_plano: pd.DataFrame
    ) -> pd.DataFrame:
        """Algoritmo Bottom-Up para consolidação de níveis sintéticos."""
        balancetes = []
        cols_valores = ["VL_SLD_INI_SIG", "VL_DEB", "VL_CRED", "VL_SLD_FIN_SIG"]

        versoes_data = df_saldos["DT_FIN"].unique()
        for data in versoes_data:
            df_mes = df_saldos[df_saldos["DT_FIN"] == data].copy()
            tab = pd.merge(
                df_plano,
                df_mes[cols_valores + ["COD_CTA", "CNPJ"]], # type: ignore
                on="COD_CTA",
                how="left",
            )
            # --- float64 vetorial: substitui apply(Decimal) ---
            for col in cols_valores:
                tab[col] = self._series_to_float(tab[col])

            # Algoritmo Bottom-Up (Empresa) — Vetorizado
            niveis = sorted(tab["NIVEL"].unique(), reverse=True)
            for nivel in niveis:
                if nivel == 1:
                    continue

                agg = (
                    tab[tab["NIVEL"] == nivel]
                    .groupby("COD_CTA_SUP")[cols_valores]
                    .sum()
                    .reset_index()
                    .rename(columns={"COD_CTA_SUP": "COD_CTA"})
                )
                agg = agg.add_suffix("_AGG").rename(columns={"COD_CTA_AGG": "COD_CTA"})

                tab = tab.merge(agg, on="COD_CTA", how="left")
                for col in cols_valores:
                    tab[col] = tab[col].add(
                        tab[f"{col}_AGG"].fillna(0.0)
                    )
                    tab.drop(columns=[f"{col}_AGG"], inplace=True)

            for col in cols_valores:
                tab[col] = tab[col].round(2)
            tab["DT_FIN"] = data
            balancetes.append(tab)

        return (
            pd.concat(balancetes, ignore_index=True) if balancetes else pd.DataFrame()
        )

    @monitor_task("ECDProcessor", "processar_demonstracoes")
    def processar_demonstracoes(self) -> Dict[str, pd.DataFrame]:
        """Processa Balanço (J100) e DRE (J150)."""
        df_j100 = self.blocos.get("dfECD_J100")
        df_j150 = self.blocos.get("dfECD_J150")
        df_j005 = self.blocos.get("dfECD_J005")

        res = {"BP": pd.DataFrame(), "DRE": pd.DataFrame()}
        if df_j005 is not None:
            # Padrão Ouro: Garante CNPJ vindo do processor caso não esteja no registro J005
            cols_base = ["PK", "DT_FIN"]
            base = df_j005[[c for c in cols_base if c in df_j005.columns]].copy()
            base["CNPJ"] = self.cnpj
            
            cols_drop = ["PK_x", "PK_y", "PK", "FK_PAI"] # LINHA_ORIGEM preservada

            if df_j100 is not None:
                df_bp = pd.merge(base, df_j100, left_on="PK", right_on="FK_PAI")
                df_bp.drop(columns=[c for c in cols_drop if c in df_bp.columns], inplace=True)
                # Reordenamento dinâmico: DT_FIN e CNPJ primeiro, preservando o restante
                cols_bp = ["DT_FIN", "CNPJ"] + [c for c in df_bp.columns if c not in ["DT_FIN", "CNPJ"]]
                res["BP"] = df_bp.reindex(columns=cols_bp)

            if df_j150 is not None:
                df_dre = pd.merge(base, df_j150, left_on="PK", right_on="FK_PAI")
                df_dre.drop(columns=[c for c in cols_drop if c in df_dre.columns], inplace=True)
                # Reordenamento dinâmico: DT_FIN e CNPJ primeiro, preservando o restante
                cols_dre = ["DT_FIN", "CNPJ"] + [c for c in df_dre.columns if c not in ["DT_FIN", "CNPJ"]]
                res["DRE"] = df_dre.reindex(columns=cols_dre)

        return res
