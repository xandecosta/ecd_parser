import pandas as pd # type: ignore # type: ignore
import logging
from typing import Dict, Any, List, Optional, cast
from core.telemetry import monitor_task, TelemetryCollector # type: ignore
# Auditoria Forense Digital


logger = logging.getLogger(__name__)


class ECDAuditor:
    """
    Motor de Auditoria Forense para Arquivos SPED-ECD.
    Realiza baterias de testes de integridade, continuidade e conformidade.
    """

    def __init__(
        self,
        df_diario: pd.DataFrame,
        df_balancete: pd.DataFrame,
        df_plano: pd.DataFrame,
        df_naturezas: Optional[pd.DataFrame] = None,  # I050 original
        df_mapeamento: Optional[pd.DataFrame] = None,  # I051
    ):
        """
        Inicializa o auditor com os DataFrames processados pelo ECDProcessor.

        Args:
            df_diario: Dataframe contendo os lançamentos (I200 + I250).
            df_balancete: Dataframe contendo os saldos mensais (I155).
            df_plano: Dataframe do plano de contas (I050).
            df_naturezas: Dataframe auxiliar de naturezas (opcional).
            df_mapeamento: Dataframe de mapeamento referencial (opcional).
        """
        self.df_diario = df_diario
        self.df_balancete = df_balancete
        self.df_plano = df_plano
        self.df_naturezas = df_naturezas
        self.df_mapeamento = df_mapeamento
        self.telemetry: Optional[TelemetryCollector] = None
        self.current_ecd_id = ""

        # Armazena os resultados de todos os testes
        # Estrutura: { "Nome do Teste": { "status": "APROVADO/ALERTA/ERRO", "impacto": Decimal, "detalhes": DataFrame } }
        self.resultados: Dict[str, Any] = {}

    @monitor_task("ECDAuditor", "executar_auditoria_completa")
    def executar_auditoria_completa(self) -> Dict[str, Any]:
        """Executa todas as baterias de testes sequencialmente."""
        logger.info("Iniciando bateria de Auditoria Forense...")

        self.testar_integridade_estrutural()
        self.testar_continuidade_cronologica()
        self.testar_coerencia_referencial()
        self.analisar_padroes_forenses()
        self.testar_indicadores_profissionais()

        return self.resultados

    # -------------------------------------------------------------------------
    # GRUPO 1: Integridade Estrutural
    # -------------------------------------------------------------------------
    def testar_integridade_estrutural(self):
        """Executa testes de amarração contábil matemática."""
        self._teste_cruzamento_diario_balancete()
        self._teste_validacao_hierarquia()

    @monitor_task("ECDAuditor", "1.1_Cruzamento_Diario")
    def _teste_cruzamento_diario_balancete(self):
        """
        1.1. Cruzamento Diário vs. Balancete
        Analisa se a soma dos lançamentos (I250) bate com os movimentos do balancete (I155).
        """
        if self.df_diario.empty or self.df_balancete.empty:
            self.resultados["1.1_Cruzamento_Diario_Balancete"] = {
                "status": "SKIPPED",
                "impacto": 0.0,
                "msg": "DataFrames de Diário ou Balancete vazios.",
            }
            return

        # Preparação do Diário
        df_d = self.df_diario.copy()
        if not pd.api.types.is_datetime64_any_dtype(df_d["DT_LCTO"]):
            df_d["DT_LCTO"] = pd.to_datetime(df_d["DT_LCTO"])

        df_d["PERIODO"] = df_d["DT_LCTO"].dt.to_period("M")

        # --- AJUSTE FORENSE: Ignorar Lançamentos de Encerramento ('E') ---
        # Pois o balancete recebido do processor já teve esses saldos revertidos.
        if "IND_LCTO" in df_d.columns:
            df_d = df_d[df_d["IND_LCTO"].str.upper() != "E"].copy()

        # Agregação do Diário
        agg_diario = (
            df_d.groupby(["COD_CTA", "PERIODO"])
            .agg({"VL_D": "sum", "VL_C": "sum"})
            .reset_index()
        )

        # Preparação do Balancete
        df_b = self.df_balancete.copy()
        if not pd.api.types.is_datetime64_any_dtype(df_b["DT_FIN"]):
            df_b["DT_FIN"] = pd.to_datetime(df_b["DT_FIN"])

        df_b["PERIODO"] = df_b["DT_FIN"].dt.to_period("M")

        # --- AJUSTE FORENSE: Apenas contas Analíticas ---
        # Cruzamento sintético gera falso positivo pois não há lançamentos em grupos sintéticos
        if "IND_CTA" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b, self.df_plano[["COD_CTA", "IND_CTA"]], on="COD_CTA", how="left"
            )

        if "IND_CTA" in df_b.columns:
            df_b = df_b[df_b["IND_CTA"].str.upper() == "A"].copy()

        # Merge para confronto
        # Usamos outer join para pegar lançamentos sem saldo e saldos sem lançamentos
        df_conf = pd.merge(
            agg_diario,
            df_b[["COD_CTA", "PERIODO", "VL_DEB", "VL_CRED"]],
            on=["COD_CTA", "PERIODO"],
            how="outer",
            suffixes=("_DIARIO", "_RAZAO"),
        ).fillna(0.0) # type: ignore

        # Cálculo das Divergências
        df_conf["DIF_DEB"] = df_conf.apply(lambda x: x["VL_D"] - x["VL_DEB"], axis=1)
        df_conf["DIF_CRED"] = df_conf.apply(lambda x: x["VL_C"] - x["VL_CRED"], axis=1)

        # Adiciona nome da conta para o relatório
        if "CONTA" not in df_conf.columns and not self.df_plano.empty:
            df_conf = pd.merge(
                df_conf,
                self.df_plano[["COD_CTA", "CONTA"]],
                on="COD_CTA",
                how="left",
            )

        # Filtro de Erros (Diferença != 0)
        # Tolerância mínima para floating point issues (embora estejamos usando Decimal)
        mask_erro = (abs(df_conf["DIF_DEB"]) > 0.01) | (abs(df_conf["DIF_CRED"]) > 0.01)
        erros = df_conf[mask_erro].copy()

        impacto_total = sum(abs(x) for x in erros["DIF_DEB"]) + sum(
            abs(x) for x in erros["DIF_CRED"]
        )

        if erros.empty:
            self.resultados["1.1_Cruzamento_Diario_Balancete"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "erros": pd.DataFrame(),
            }
        else:
            # --- NOVIDADE: COMPOSIÇÃO DA PROVA (Auditabilidade) ---
            # Identificamos as chaves (Conta + Período) que falharam
            chaves_erro = cast(
                pd.DataFrame, erros[["COD_CTA", "PERIODO"]]
            ).drop_duplicates()

            # Filtramos o diário original para trazer apenas os lançamentos dessas contas nos meses com erro
            # Isso cria o "Dossiê de Lançamentos" para auditoria detalhada
            evidencia_detalhada = pd.merge(
                df_d, chaves_erro, on=["COD_CTA", "PERIODO"], how="inner"
            )

            self.resultados["1.1_Cruzamento_Diario_Balancete"] = {
                "status": "REPROVADO",
                "impacto": impacto_total,
                "msg": f"{len(erros)} divergências encontradas.",
                "erros": erros,
                "evidencia": evidencia_detalhada,  # <--- Aqui está o subsidio sólido
            }

    def _teste_validacao_hierarquia(self):
        """
        1.2. Validação da Hierarquia Nativa
        Verifica se a soma das contas analíticas fecha com as sintéticas no balancete informado.
        """
        if self.df_balancete.empty or self.df_plano.empty:
            self.resultados["1.2_Validacao_Hierarquia"] = {
                "status": "SKIPPED",
                "impacto": 0.0,
            }
            return

        # Precisamos garantir que temos a estrutura hierárquica (NIVEL, COD_CTA_SUP)
        # Se o balancete já veio do processor, ele pode ter essa info.
        # Por via das dúvidas, fazemos merge com o plano se necessário.
        df_b = self.df_balancete.copy()
        cols_missing = [
            c for c in ["NIVEL", "COD_CTA_SUP", "CONTA"] if c not in df_b.columns
        ]

        if cols_missing:
            df_b = pd.merge(
                df_b,
                self.df_plano[["COD_CTA"] + cols_missing],
                on="COD_CTA",
                how="left",
            )

        # Garante integridade de tipos
        if "NIVEL" in df_b.columns:
            s_nivel = pd.to_numeric(df_b["NIVEL"], errors="coerce")
            df_b["NIVEL"] = s_nivel.fillna(0).astype(int) # type: ignore

        # Isola contas Analíticas (assumindo que o Processor já calculou os sintéticos,
        # mas queremos testar se a MATEMÁTICA bate, caso o processor tenha propagado erro ou o input seja ruim)
        # Na verdade, o teste forense ideal é:
        # Pega as analíticas -> Recalcula tudo -> Compara com o que está lá.

        # 1. Identificar Analíticas (Input Original)
        # Como o df_balancete já pode ter sintéticas recalculadas, esse teste valida o PRÓPRIO ALGORITMO
        # ou valida se houve manipulação pós-processamento.
        # Para um teste forense "raiz", deveríamos usar apenas as linhas com IND_CTA = 'A'.

        if "IND_CTA" not in df_b.columns:
            df_b = pd.merge(
                df_b, self.df_plano[["COD_CTA", "IND_CTA"]], on="COD_CTA", how="left"
            )

        mask_analiticas = df_b["IND_CTA"].str.upper() == "A"
        df_analiticas = df_b[mask_analiticas].copy()

        # Se não tem analíticas, falha
        if df_analiticas.empty:
            return

        # 2. Recálculo "Clean Room" (Simulação Paralela)
        # Vamos reconstruir os saldos sintéticos do zero

        # Loop de Agregação (Nível Máximo até 2)

        # Dicionário para acumular saldos por conta (começa com analíticas)
        # Mas precisamos preservar a dimensão TEMPORAL (DT_FIN)
        # Estratégia: iterar por período para garantir isolamento

        divergencias = []
        periodos = df_b["DT_FIN"].unique()

        for periodo in periodos:
            df_mes = df_b[df_b["DT_FIN"] == periodo]
            mask_sinteticas = df_mes["IND_CTA"].astype(str).str.upper() == "S" # type: ignore
            df_sint = df_mes[mask_sinteticas]
            
            if df_sint.empty: # type: ignore
                continue

            # Calculate sum of all children for each parent
            df_filhos_agregados = df_mes.groupby("COD_CTA_SUP", as_index=False)["VL_SLD_FIN_SIG"].sum()
            df_filhos_agregados = df_filhos_agregados.rename(columns={"VL_SLD_FIN_SIG": "VL_CALCULADO", "COD_CTA_SUP": "COD_CTA"})
            
            # Merge with synthetics
            df_compare = pd.merge(df_sint, df_filhos_agregados, on="COD_CTA", how="left")
            
            # Filhos em branco = 0 calculado
            df_compare["VL_CALCULADO"] = df_compare["VL_CALCULADO"].fillna(0.0) # type: ignore
            
            # Tolerancia
            mask_diverge = abs(df_compare["VL_SLD_FIN_SIG"] - df_compare["VL_CALCULADO"]) > 0.01
            df_erros_mes = df_compare[mask_diverge]
            
            for _, row in df_erros_mes.iterrows():
                tipo = "Sintética sem filhos com saldo" if pd.isna(row.get("VL_CALCULADO")) and row["VL_SLD_FIN_SIG"] != 0 else "Erro de Soma Hierárquica"
                divergencias.append(
                    {
                        "COD_CTA": row["COD_CTA"],
                        "CONTA": row.get("CONTA", "SEM_NOME"),
                        "DT_FIN": periodo,
                        "TIPO": tipo,
                        "DIFERENCA": row["VL_SLD_FIN_SIG"] - row["VL_CALCULADO"],
                        "VLR_INFORMADO": row["VL_SLD_FIN_SIG"],
                        "VLR_CALCULADO": row["VL_CALCULADO"],
                    }
                )

        df_div = pd.DataFrame(divergencias)

        if df_div.empty:
            self.resultados["1.2_Validacao_Hierarquia"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "erros": pd.DataFrame(),
            }
        else:
            impacto = sum(abs(x) for x in df_div["DIFERENCA"])
            self.resultados["1.2_Validacao_Hierarquia"] = {
                "status": "REPROVADO",
                "impacto": impacto,
                "msg": f"{len(df_div)} erros de consolidação detectados.",
                "erros": df_div,
            }

    # -------------------------------------------------------------------------
    # GRUPO 2: Continuidade Cronológica
    # -------------------------------------------------------------------------
    def testar_continuidade_cronologica(self):
        """Executa testes de continuidade temporal."""
        self._teste_forward_roll()
        self._teste_auditoria_i157()

    def _teste_forward_roll(self):
        # Requer lógica multi-arquivo (pensar como injetar histórico)
        pass

    def _teste_auditoria_i157(self):
        pass

    # -------------------------------------------------------------------------
    # GRUPO 3: Coerência Referencial
    # -------------------------------------------------------------------------
    def testar_coerencia_referencial(self):
        """Executa testes de conformidade com normas da RFB."""
        self._teste_consistencia_natureza()
        self._teste_contas_orfas()

    @monitor_task("ECDAuditor", "3.1_Consist_Natureza")
    def _teste_consistencia_natureza(self):
        """
        3.1. Consistência de Natureza
        Verifica se a natureza da conta (Ativo/Passivo) bate com o mapeamento referencial.
        """
        if self.df_plano.empty:
            self.resultados["3.1_Consistencia_Natureza"] = {
                "status": "SKIPPED",
                "impacto": 0.0,
            }
            return

        # Prepara dados: COD_CTA, COD_NAT, COD_CTA_REF
        df_check = self.df_plano.copy()

        # Se COD_CTA_REF não estiver no plano, tentamos merge com mapeamento.
        if "COD_CTA_REF" not in df_check.columns:
            if self.df_mapeamento is not None:
                # Blindagem p/ IDE: garante que self.df_mapeamento é DataFrame
                tab_i051 = cast(pd.DataFrame, self.df_mapeamento).copy()
                # Normalização pode ser necessária se I051 tiver prefixos
                df_map_temp = tab_i051.copy()
                if "I051_COD_CTA_REF" in df_map_temp.columns:
                    df_map_temp.rename(
                        columns={
                            "I051_COD_CTA_REF": "COD_CTA_REF",
                            "I051_FK_PAI": "FK_PAI",
                        },
                        inplace=True,
                    )

                df_check = pd.merge(
                    df_check,
                    df_map_temp[["FK_PAI", "COD_CTA_REF"]],
                    left_on="PK",
                    right_on="FK_PAI",
                    how="left",
                )
            else:
                self.resultados["3.1_Consistencia_Natureza"] = {
                    "status": "SKIPPED",
                    "msg": "Sem mapeamento referencial disponível.",
                }
                return

        # Filtra apenas contas Mapeadas
        df_check = df_check[
            df_check["COD_CTA_REF"].notna() & (df_check["COD_CTA_REF"] != "")
        ].copy()

        if df_check.empty:
            self.resultados["3.1_Consistencia_Natureza"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "msg": "Nenhuma conta associada ao plano referencial.",
            }
            return

        # Lógica de Validação:
        # COD_NAT 01 (Ativo) -> REF deve começar com 1
        # COD_NAT 02 (Passivo) -> REF deve começar com 2
        # COD_NAT 03 (PL) -> REF deve começar com 2
        # COD_NAT 04 (Resultado) -> REF deve começar com 3

        def check_natureza(row):
            nat = str(row.get("COD_NAT", "")).strip().zfill(2)
            ref = str(row.get("COD_CTA_REF", "")).strip()

            if not ref:
                return None

            erro = None
            if nat == "01" and not ref.startswith("1"):
                erro = "Ativo mapeado fora do Grupo 1"
            elif nat == "02" and not ref.startswith("2"):
                erro = "Passivo mapeado fora do Grupo 2"
            elif nat == "03" and not ref.startswith("2"):
                erro = "Patrimônio Líquido mapeado fora do Grupo 2"
            elif nat in ["04", "05", "09"] and ref.startswith("1"):
                erro = "Resultado/Outros mapeado no Ativo"
            elif (
                nat in ["04", "05", "09"]
                and ref.startswith("2")
                and not ref.startswith("2.03")
            ):  # 2.03 as vezes é resultado acumulado? Nao, RFB Resultado é 3.
                if not ref.startswith("3"):
                    erro = "Resultado/Outros mapeado no Passivo"

            if erro:
                return {
                    "COD_CTA": row.get("COD_CTA"),
                    "CONTA": row.get("CONTA")
                    or row.get("DESCRICAO")
                    or row.get("NOME"),
                    "COD_NAT_ECD": nat,
                    "COD_CTA_REF": ref,
                    "TIPO_ERRO": erro,
                }
            return None

        # Aplica verificação
        # Iterrows é lento mas seguro para lógica complexa condicional. Vectorized seria melhor se logica fosse simples.
        erros_audit = df_check.apply(check_natureza, axis=1).dropna().tolist()

        df_erros = pd.DataFrame(erros_audit)

        if df_erros.empty:
            self.resultados["3.1_Consistencia_Natureza"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "erros": pd.DataFrame(),
            }
        else:
            self.resultados["3.1_Consistencia_Natureza"] = {
                "status": "ALERTA",  # É um alerta qualitativo, impacto financeiro difícil de medir diretamente sem saldos
                "impacto": 0.0,
                "msg": f"{len(df_erros)} contas com natureza divergente.",
                "erros": df_erros,
            }

    def _teste_contas_orfas(self):
        """
        3.2. Auditoria de Contas "Órfãs"
        Contas analíticas com saldo/movimento relevante sem mapeamento referencial.
        """
        if self.df_balancete.empty or self.df_plano.empty:
            self.resultados["3.2_Contas_Orfas"] = {
                "status": "SKIPPED",
                "impacto": 0.0,
            }
            return

        # 1. Identificar Analíticas com Mapeamento Faltante
        df_b = self.df_balancete.copy()

        # Merge com Plano para saber quem tem REF
        # Se 'COD_CTA_REF' não estiver no balancete, buscamos no plano
        cols_plano = ["COD_CTA"]
        if "COD_CTA_REF" not in df_b.columns:
            cols_plano.append("COD_CTA_REF")
        if "ORIGEM_MAP" not in df_b.columns:
            cols_plano.append(
                "ORIGEM_MAP"
            ) if "ORIGEM_MAP" in self.df_plano.columns else None

        if len(cols_plano) > 1:
            df_b = pd.merge(df_b, self.df_plano[cols_plano], on="COD_CTA", how="left")

        # Garante que temos as colunas
        if "COD_CTA_REF" not in df_b.columns:
            df_b["COD_CTA_REF"] = None  # Assumimos vazio se não achou

        # 2. Filtra Analíticas (Relevantes)
        # Analitica = (IND_CTA = A ou NIVEL mais baixo). Vamos pelo plano se possivel
        if "IND_CTA" not in df_b.columns:
            df_b = pd.merge(
                df_b, self.df_plano[["COD_CTA", "IND_CTA"]], on="COD_CTA", how="left"
            )

        # Critério: É Analítica E (Tem Saldo Inicial != 0 OU Tem Debito != 0 OU Tem Credito != 0)
        mask_relevante = (df_b["IND_CTA"].str.upper() == "A") & (
            (df_b["VL_SLD_INI_SIG"] != 0)
            | (df_b["VL_DEB"] != 0)
            | (df_b["VL_CRED"] != 0)
        )

        # Critério Órfã: COD_CTA_REF nulo ou vazio
        mask_orfa = (df_b["COD_CTA_REF"].isna()) | (
            df_b["COD_CTA_REF"].astype(str).str.strip() == ""
        )

        # Interseção
        df_orfas = df_b[mask_relevante & mask_orfa].copy()

        # Agrupa por conta (pois a mesma conta aparece em 12 meses, não queremos reportar 12x)
        # Pegamos o maior saldo do período para mostrar impacto (Otimização Baseada em Index)
        df_orfas_abs = df_orfas.copy()
        df_orfas_abs["ABS_VAL"] = df_orfas_abs["VL_SLD_FIN_SIG"].abs()
        
        idx_max = df_orfas_abs.groupby("COD_CTA")["ABS_VAL"].idxmax()
        resumo_orfas = df_orfas.loc[idx_max, ["COD_CTA", "VL_SLD_FIN_SIG"]].reset_index(drop=True)

        if resumo_orfas.empty:
            self.resultados["3.2_Contas_Orfas"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "erros": pd.DataFrame(),
            }
        else:
            impacto = sum(abs(x) for x in resumo_orfas["VL_SLD_FIN_SIG"])

            # Adiciona nome da conta para o relatório
            if "CONTA" not in resumo_orfas.columns and not self.df_plano.empty:
                resumo_orfas = pd.merge(
                    resumo_orfas,
                    self.df_plano[["COD_CTA", "CONTA"]],
                    on="COD_CTA",
                    how="left",
                )

            self.resultados["3.2_Contas_Orfas"] = {
                "status": "REPROVADO",
                "impacto": impacto,
                "msg": f"{len(resumo_orfas)} contas analíticas com movimento sem mapeamento.",
                "erros": resumo_orfas,
            }

    # -------------------------------------------------------------------------
    # GRUPO 4: Análise Forense de Padrões
    # -------------------------------------------------------------------------
    def analisar_padroes_forenses(self):
        """Executa testes estatísticos e de detecção de fraude."""
        self._teste_lei_benford()
        self._teste_duplicidades()
        self._teste_omissao_encerramento()

    def _teste_lei_benford(self):
        """
        4.1. Análise de Benford (Primeiro Dígito)
        Verifica se a distribuição dos primeiros dígitos dos valores monetários
        segue a Lei de Benford. Desvios significativos indicam manipulação.
        """
        if self.df_diario.empty:
            self.resultados["4.1_Lei_Benford"] = {
                "status": "SKIPPED",
                "impacto": 0.0,
            }
            return

        try:
            from scipy.stats import chisquare  # type: ignore
            import numpy as np
        except ImportError:
            self.resultados["4.1_Lei_Benford"] = {
                "status": "SKIPPED",
                "msg": "Biblioteca Scipy não instalada.",
            }
            return

        # 1. Preparar dados: Unificar Vl Debito e Vl Credito > 0
        vals_d = self.df_diario[self.df_diario["VL_D"] > 0]["VL_D"]
        vals_c = self.df_diario[self.df_diario["VL_C"] > 0]["VL_C"]

        # Converte para string para pegar primeiro digito
        # Importante: Precisamos de valores brutos, sem filtro.
        series_vals = pd.concat([vals_d, vals_c])

        if series_vals.empty:
            return

        # Pega primeiro digito (1-9). Ignora 0 (embora filtro >0 ja resolva).
        # Remove caracteres nao numericos se houver, mas assumimos Decimal limpo.
        digits = series_vals.astype(str).str.lstrip().str[0]
        digits = digits[digits.isin([str(i) for i in range(1, 10)])].astype(int)

        if len(digits) < 100:
            self.resultados["4.1_Lei_Benford"] = {
                "status": "SKIPPED",
                "msg": "Amostra insuficiente para teste estatístico (<100).",
            }
            return

        # 2. Calcular Frequências Observadas
        counts = digits.value_counts().sort_index()
        total = len(digits)
        f_obs = []
        for d in range(1, 10):
            f_obs.append(counts.get(d, 0))

        # 3. Calcular Frequências Esperadas (Benford)
        # P(d) = log10(1 + 1/d)
        benford_probs = [np.log10(1 + 1 / d) for d in range(1, 10)]
        f_exp = [p * total for p in benford_probs]

        # 4. Teste Qui-Quadrado
        # Retorna estatística e p-valor.
        # Null hypothesis: The data follows Benford's Law.
        # High Chi-Square -> Low p-value -> Reject Null -> Anomalous.
        chi_stat, p_val = chisquare(f_obs=f_obs, f_exp=f_exp)

        # Cria DataFrame para o relatório
        df_benford = pd.DataFrame(
            {
                "DIGITO": range(1, 10),
                "CONTAGEM_OBS": f_obs,
                "FREQ_OBS": [x / total for x in f_obs],
                "FREQ_BENFORD": benford_probs,
                "DESVIO": [
                    o - e for o, e in zip([x / total for x in f_obs], benford_probs)
                ],
            }
        )

        # 4. Cálculo de Métricas Agregadas
        mad = np.mean(np.abs(df_benford["FREQ_OBS"] - df_benford["FREQ_BENFORD"]))
        status = "ALERTA" if mad > 0.012 else "APROVADO"
        if mad > 0.020:
            status = "REPROVADO"

        # 5. Interpretação dos Resultados
        def interpretar_desvio(row):
            d = row["DESVIO"]
            if abs(d) <= 0.005:
                return "Dentro do esperado"
            if d > 0.005:
                return "Excesso de lançamentos"
            return "Déficit de lançamentos"

        df_benford["INTERPRETACAO"] = df_benford.apply(interpretar_desvio, axis=1)

        # --- DIAGNÓSTICO DE SEGUNDA CAMADA (Drill-Down Multi-Dígito) ---
        # Identifica TODOS os dígitos com anomalia positiva relevante (> 1%)
        df_benford["DESVIO_ABS"] = df_benford["FREQ_OBS"] - df_benford["FREQ_BENFORD"]

        # Coleta os dígitos suspeitos
        suspeitos_list = df_benford[df_benford["DESVIO_ABS"] > 0.01]["DIGITO"].tolist()

        # Se não houver nenhum acima de 1%, pega o maior apenas
        if not suspeitos_list:
            idx_max = df_benford["DESVIO_ABS"].idxmax()
            suspeitos_list = [df_benford.loc[idx_max, "DIGITO"]]

        # Casting explícito para garantir iterabilidade no loop (Linha 680)
        digitos_suspeitos = suspeitos_list

        df_lctos = self.df_diario.copy()
        
        # Otimização Vetorial Matemática Ouro (Extrai 1º dígito usando log10 em Numpy em vez de Str Lstrip)
        df_lctos["PRIMEIRO_DIGITO"] = None
        mask_valid = df_lctos["VL_SINAL"] != 0
        if mask_valid.any():
            abs_vals = np.abs(df_lctos.loc[mask_valid, "VL_SINAL"])
            # Extrai 1 dígito de x: floor(x / 10^{floor(log10(x))})
            log_div = 10 ** np.floor(np.log10(abs_vals))
            first_digits = np.floor(abs_vals / log_div)
            df_lctos.loc[mask_valid, "PRIMEIRO_DIGITO"] = first_digits.astype(int)

        analise_rows = []
        for digito in digitos_suspeitos:
            df_viciado = df_lctos[df_lctos["PRIMEIRO_DIGITO"] == digito].copy()
            if df_viciado.empty:
                continue

            # Top 10 valores por dígito
            top_valores = (
                df_viciado["VL_DC"]
                .value_counts()
                .head(10)
                .reset_index()
            )
            top_valores.columns = ["VALOR", "FREQUENCIA"]

            for _, row_val in top_valores.iterrows():
                val_exato = row_val["VALOR"]
                subset = cast(
                    pd.DataFrame, df_viciado[df_viciado["VL_DC"] == val_exato]
                )

                # Conta e Histórico mais comuns
                conta_top = subset["COD_CTA"].value_counts().index[0]
                if "CONTA" in subset.columns:
                    conta_nome = (
                        subset["CONTA"].value_counts().index[0]
                    )
                    conta_top = f"{conta_top} - {conta_nome}"

                hist_col = (
                    "HIST"
                    if "HIST" in subset.columns
                    else ("I250_HIST" if "I250_HIST" in subset.columns else None)
                )
                hist_top = (
                    subset[hist_col].value_counts().index[0]
                    if hist_col
                    else "N/A"
                )

                analise_rows.append(
                    {
                        "DIGITO": digito,
                        "VALOR": val_exato,
                        "FREQUENCIA": row_val["FREQUENCIA"],
                        "CONTA_PRINCIPAL": conta_top,
                        "HISTORICO_PREDOMINANTE": hist_top,
                    }
                )

        df_drilldown = pd.DataFrame(analise_rows)

        # Monta resultado final com múltiplas abas
        self.resultados["4.1_Lei_Benford"] = {
            "status": status,
            "impacto": float(mad),
            "msg": f"MAD: {mad:.5f} | Dígitos com Excesso: {digitos_suspeitos}",
            "detalhes": {
                "4.1_Benford_Frequencias": df_benford.drop(columns=["DESVIO_ABS"]),
                "4.1_Benford_Analise_Valores": df_drilldown,
            },
        }

    @monitor_task("ECDAuditor", "4.2_Duplicidades")
    def _teste_duplicidades(self):
        """
        4.2. Detecção de Lançamentos Duplicados
        """
        # --- AJUSTE FORENSE: Refinamento de Duplicidades ---
        # 1. Incluímos o Histórico (HIST) no confronto para diferenciar taxas bancárias idênticas mas de transações diferentes
        subset_cols = ["DT_LCTO", "COD_CTA", "VL_D", "VL_C"]
        if "HIST" in self.df_diario.columns:
            subset_cols.append("HIST")

        # Identifica potenciais duplicatas (mesma conta, data, valor e histórico)
        df_dupl_raw = self.df_diario[
            self.df_diario.duplicated(subset=subset_cols, keep=False)
        ].copy()

        # 2. Filtro de Materialidade e Ruído
        if not df_dupl_raw.empty:
            # Exclui valores zerados
            df_dupl_raw = df_dupl_raw[df_dupl_raw["VL_SINAL"] != 0]

            # Filtro A: Ignorar tarifas bancárias de baixo valor (< R$ 100,00)
            # Geralmente tarifas e taxas recorrentes ocorrem em massa e não são risco financeiro relevante
            termos_ruido = "BANCO|TARIFA|TAXA|TED|DOC|IOF|MANUTEN|FINANC|MENSAL|CONVEN|SERVIC|BOLETO|CADAST|PIX"

            # Checa se o termo está na Conta ou no Histórico
            mask_conta_ruido = (
                df_dupl_raw["CONTA"]
                .str.upper()
                .str.contains(termos_ruido, na=False)
            )
            mask_hist_ruido = (
                df_dupl_raw["HIST"]
                .str.upper()
                .str.contains(termos_ruido, na=False)
            )

            # Checa se a conta começa com 04.2.3 (Grupo comum de Despesas Financeiras)
            mask_grupo_financeiro = cast(
                pd.Series, df_dupl_raw["COD_CTA"]
            ).str.startswith("04.2.3")

            mask_tarifa_pequena = (
                df_dupl_raw["VL_SINAL"].apply(abs) < 100
            ) & (mask_conta_ruido | mask_hist_ruido | mask_grupo_financeiro)

            # Filtro B: Ignorar Processamento em Lote (Batch)
            # Se uma conta tem MAIS DE 5 lançamentos idênticos no mesmo dia,
            # assumimos que é um padrão operacional (ex: Folha, Tarifas em massa)
            contagem_grupo = (
                df_dupl_raw
                .groupby(subset_cols)[df_dupl_raw.columns[0]]
                .transform("count")
            )
            mask_batch = contagem_grupo > 5

            df_final_erros = df_dupl_raw[~(mask_tarifa_pequena | mask_batch)].copy()
        else:
            df_final_erros = pd.DataFrame()

        impacto = (
            df_final_erros["VL_SINAL"].apply(abs).sum()
            if not df_final_erros.empty
            else 0.0
        )

        if df_final_erros.empty:
            self.resultados["4.2_Duplicidades"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "erros": pd.DataFrame(),
            }
        else:
            # Adiciona nome da conta para o relatório se ainda não tiver
            if (
                "CONTA" not in df_final_erros.columns
                and not self.df_plano.empty
            ):
                df_final_erros = pd.merge(
                    df_final_erros,
                    self.df_plano[["COD_CTA", "CONTA"]],
                    on="COD_CTA",
                    how="left",
                )

            self.resultados["4.2_Duplicidades"] = {
                "status": "ALERTA",
                "impacto": impacto,
                "msg": f"{len(df_final_erros)} lançamentos duplicados suspeitos (filtrados por materialidade/histórico).",
                "erros": df_final_erros.sort_values(
                    ["DT_LCTO", "COD_CTA", "VL_D"]
                ),
            }

    def _teste_omissao_encerramento(self):
        """
        4.3. Verificação de Omissão de Encerramento (DRE não zerada)
        """
        if self.df_balancete.empty:
            return

        # Identificar contas de Resultado (Geralmente Natureza 04 no plano ou prefixo 3 no referencial)
        # Vamos usar a info de NATUREZA se disponível
        df_b = self.df_balancete.copy()

        # Precisa ter natureza. Se não tiver, busca.
        if "COD_NAT" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b,
                self.df_plano[["COD_CTA", "COD_NAT"]],
                on="COD_CTA",
                how="left",
            )

        if "COD_NAT" not in df_b.columns:
            self.resultados["4.3_Omissao_Encerramento"] = {
                "status": "SKIPPED",
                "msg": "Sem info de NaturezaConta",
            }
            return

        # Filtra Resultado: COD_NAT '04' (versao antiga) ou conforme tabela da versão
        # Simplificação: Contas com natureza '04'

        # Garante que é uma Series booleana para satisfazer o IDE
        has_res = (df_b["COD_NAT"] == "04").any()
        if not has_res:
            self.resultados["4.3_Omissao_Encerramento"] = {
                "status": "SKIPPED",
                "msg": "Nenhuma conta de resultado (04) identificada.",
            }
            return

        # --- AJUSTE FORENSE: Apenas contas Analíticas ---
        if "IND_CTA" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b, self.df_plano[["COD_CTA", "IND_CTA"]], on="COD_CTA", how="left"
            )

        df_res = df_b[
            (df_b["COD_NAT"] == "04")
            & (df_b["IND_CTA"].str.upper() == "A")
        ].copy()

        if df_res.empty:
            self.resultados["4.3_Omissao_Encerramento"] = {
                "status": "SKIPPED",
                "msg": "Nenhuma conta analítica de resultado identificada.",
            }
            return

        # Verifica Saldo Final no ÚLTIMO MÊS do arquivo
        ultima_data = df_res["DT_FIN"].max()
        df_final = df_res[df_res["DT_FIN"] == ultima_data].copy()

        # --- CONFRONTO COM LANÇAMENTOS 'E' DO DIÁRIO ---
        # Como nosso df_final está REVERTIDO (Pré-Encerramento),
        # a soma dele com os lançamentos 'E' do diário deve ser rigorosamente zero.

        # 1. Agrega lançamentos do tipo 'E' por conta
        df_d = self.df_diario.copy()
        mask_e = df_d["IND_LCTO"].str.upper() == "E"
        df_e_total = df_d[mask_e].groupby("COD_CTA")["VL_SINAL"].sum().reset_index()
        df_e_total.rename(columns={"VL_SINAL": "VL_ENCERRAMENTO"}, inplace=True)

        # 2. Merge com o Balancete de Dezembro
        df_confronto = pd.merge(
            df_final,
            df_e_total,
            on="COD_CTA",
            how="left",
        ).fillna(0.0)

        # 3. Cálculo da Sobra: Saldo Pré-Encerramento + Lançamentos de Zeramento
        df_confronto["SALDO_RESTANTE"] = df_confronto.apply(
            lambda x: x["VL_SLD_FIN_SIG"] + x["VL_ENCERRAMENTO"], axis=1
        )

        # Adiciona nome da conta para o relatório
        if "CONTA" not in df_confronto.columns and not self.df_plano.empty:
            df_confronto = pd.merge(
                df_confronto,
                self.df_plano[["COD_CTA", "CONTA"]],
                on="COD_CTA",
                how="left",
            )

        # Filtra onde a sobra != 0
        erros = df_confronto[
            abs(df_confronto["SALDO_RESTANTE"]) > 0.01
        ].copy()
        soma_erros = sum(abs(x) for x in erros["SALDO_RESTANTE"])

        if erros.empty:
            self.resultados["4.3_Omissao_Encerramento"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "erros": pd.DataFrame(),
            }
        else:
            self.resultados["4.3_Omissao_Encerramento"] = {
                "status": "REPROVADO",
                "impacto": soma_erros,
                "msg": f"{len(erros)} contas de resultado com encerramento falho ou omisso.",
                "erros": erros[
                    [
                        "COD_CTA",
                        "CONTA",
                        "VL_SLD_FIN_SIG",
                        "VL_ENCERRAMENTO",
                        "SALDO_RESTANTE",
                    ]
                ]
                if "CONTA" in erros.columns
                else erros[
                    ["COD_CTA", "VL_SLD_FIN_SIG", "VL_ENCERRAMENTO", "SALDO_RESTANTE"]
                ],
            }

    # -------------------------------------------------------------------------
    # GRUPO 5: Indicadores Profissionais
    # -------------------------------------------------------------------------
    def testar_indicadores_profissionais(self):
        """Executa testes de qualidade contábil avançada."""
        self._teste_estouro_caixa()
        self._teste_passivo_ficticio()
        self._teste_inversao_natureza()
        self._teste_consistencia_pl_resultado()

    @monitor_task("ECDAuditor", "5.2_Estouro_Caixa")
    def _teste_estouro_caixa(self):
        """
        5.2. Estouro de Caixa
        Saldo Credor em contas de Disponibilidade (ativos que deveriam ser devedores).
        """
        if self.df_balancete.empty:
            return

        # Identificar contas caixa (Geralmente 1.01.01... ou Pelo nome 'CAIXA')
        # Melhor usar COD_NAT = '01' e Nível 1 ou 2? Não muito preciso.
        # Vamos usar regex no nome ou COD_CTA_REF começando com 1.01.01

        df_b = self.df_balancete.copy()

        # Merge com plano se precisar de nome
        if "CONTA" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b,
                self.df_plano[["COD_CTA", "CONTA"]],
                on="COD_CTA",
                how="left",
            )

        if (
            "COD_CTA_REF" not in df_b.columns
            and self.df_plano is not None
            and "COD_CTA_REF" in self.df_plano.columns
        ):
            df_b = pd.merge(
                df_b,
                self.df_plano[["COD_CTA", "COD_CTA_REF"]],
                on="COD_CTA",
                how="left",
            )

        # --- AJUSTE FORENSE: Apenas contas Analíticas ---
        if "IND_CTA" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b, self.df_plano[["COD_CTA", "IND_CTA"]], on="COD_CTA", how="left"
            )

        # --- AJUSTE FORENSE: Apenas Contas de Ativo (Natureza 01) ---
        if "COD_NAT" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b,
                self.df_plano[["COD_CTA", "COD_NAT"]],
                on="COD_CTA",
                how="left",
            )

        # Filtro de Amostra: Apenas Analíticas de Ativo
        mask_analitica_ativo = df_b["IND_CTA"].str.upper() == "A"

        if "COD_NAT" in df_b.columns:
            # Garante comparação segura independente de ser float, int ou str
            mask_analitica_ativo &= df_b["COD_NAT"].astype(str).str.zfill(2) == "01"

        df_b = df_b[mask_analitica_ativo].copy()

        # Critério de Disponibilidades: (REF 1.01.01 ou Nome contendo CAIXA/BANCO/DISPONIBILIDADE)
        mask_disponibilidade = pd.Series([False] * len(df_b), index=df_b.index)

        # 1. Por Referencial (Plano Contas Referencial RFB - Grupo 1.01.01)
        if "COD_CTA_REF" in df_b.columns:
            mask_disponibilidade |= (
                df_b["COD_CTA_REF"]
                .astype(str)
                .str.startswith("1.01.01")
            )

        # 2. Por Nome (Fallback para quando não há mapeamento)
        if "CONTA" in df_b.columns:
            keywords = ["CAIXA", "BANCO", "DISPONIBILIDADE", "APLICAÇÃO", "CASH"]
            pattern = "|".join(keywords)
            mask_disponibilidade |= (
                df_b["CONTA"]
                .str.upper()
                .str.contains(pattern, na=False)
            )

        # Um 'Estouro' é um Ativo com saldo Credor (negativo no nosso sistema)
        mask_estouro = mask_disponibilidade & (df_b["VL_SLD_FIN_SIG"] < 0)

        erros = df_b[mask_estouro].copy()

        if erros.empty:
            self.resultados["5.2_Estouro_Caixa"] = {
                "status": "APROVADO",
                "impacto": 0.0,
            }
        else:
            # Agrupar impacto (maior estouro)
            impacto = sum(abs(x) for x in erros["VL_SLD_FIN_SIG"])
            self.resultados["5.2_Estouro_Caixa"] = {
                "status": "REPROVADO",
                "impacto": impacto,
                "msg": f"{len(erros)} ocorrências de caixa estourado (saldo credor).",
                "detalhes": erros,
            }

    def _teste_passivo_ficticio(self):
        """
        5.3. Passivo Fictício (Estaticidade)
        Contas de obrigação relevantes sem movimentação por todo o período.
        """
        if self.df_balancete.empty:
            return

        # Filtrar Passivo (COD_NAT 02 ou REF 2)
        df_b = self.df_balancete.copy()

        if (
            "COD_NAT" not in df_b.columns or "IND_CTA" not in df_b.columns
        ) and not self.df_plano.empty:
            cols_to_add = [
                c
                for c in ["COD_CTA", "COD_NAT", "IND_CTA"]
                if c in self.df_plano.columns
            ]
            df_b = pd.merge(
                df_b,
                self.df_plano[cols_to_add],
                on="COD_CTA",
                how="left",
                suffixes=("", "_extra"),
            )

        # Filtro: Apenas Passivo (02) e apenas Analíticas (A)
        mask_analitica_passivo = pd.Series([True] * len(df_b), index=df_b.index)

        if "COD_NAT" in df_b.columns:
            mask_analitica_passivo &= (
                df_b["COD_NAT"].astype(str).str.zfill(2) == "02"
            )

        if "IND_CTA" in df_b.columns:
            mask_analitica_passivo &= (
                df_b["IND_CTA"].str.upper() == "A"
            )

        df_b_filtered = df_b[mask_analitica_passivo].copy()

        if df_b_filtered.empty:
            self.resultados["5.3_Passivo_Ficticio"] = {
                "status": "APROVADO",
                "impacto": 0.0,
            }
            return

        # Contas analíticas com saldo relevante que não tiveram nem DEB nem CRED o ano todo
        agg_contas = (
            df_b_filtered
            .groupby("COD_CTA")
            .agg(
                {
                    "VL_DEB": "sum",
                    "VL_CRED": "sum",
                    "VL_SLD_FIN_SIG": "last",  # Saldo final do periodo
                }
            )
            .reset_index()
        )

        agg_contas = agg_contas[
            abs(agg_contas["VL_SLD_FIN_SIG"]) > 1000
        ]  # Filtra saldo relevante > 1000

        mask_estatico = (agg_contas["VL_DEB"] == 0) & (agg_contas["VL_CRED"] == 0)
        estaticas = agg_contas[mask_estatico].copy()

        # Adiciona nome da conta para o relatório
        if (
            "CONTA" not in estaticas.columns
            and not self.df_plano.empty
        ):
            estaticas = pd.merge(
                estaticas,
                self.df_plano[["COD_CTA", "CONTA"]],
                on="COD_CTA",
                how="left",
            )

        if estaticas.empty:
            self.resultados["5.3_Passivo_Ficticio"] = {
                "status": "APROVADO",
                "impacto": 0.0,
            }
        else:
            impacto = sum(abs(x) for x in estaticas["VL_SLD_FIN_SIG"])
            self.resultados["5.3_Passivo_Ficticio"] = {
                "status": "ALERTA",
                "impacto": impacto,
                "msg": f"{len(estaticas)} contas de passivo sem movimentação no período.",
                "detalhes": estaticas[
                    ["COD_CTA", "CONTA", "VL_DEB", "VL_CRED", "VL_SLD_FIN_SIG"]
                ]
                if "CONTA" in estaticas.columns
                else estaticas,
            }

    def _teste_inversao_natureza(self):
        """
        5.1. Inversão de Natureza
        Ativo < 0 ou Passivo > 0 (Exceto redutoras).
        """
        if self.df_balancete.empty:
            return
        df_b = self.df_balancete.copy()

        # Merge info de natureza e conta
        if "COD_NAT" not in df_b.columns and not self.df_plano.empty:
            cols = (
                ["COD_CTA", "COD_NAT", "CONTA"]
                if "CONTA" in self.df_plano.columns
                else ["COD_CTA", "COD_NAT"]
            )
            df_b = pd.merge(
                df_b,
                self.df_plano[cols],
                on="COD_CTA",
                how="left",
            )

        if "COD_NAT" not in df_b.columns:
            self.resultados["5.1_Inversao_Natureza"] = {
                "status": "SKIPPED",
                "msg": "Sem Natureza",
            }
            return

        # --- AJUSTE FORENSE: Apenas Contas Analíticas ---
        if "IND_CTA" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b,
                self.df_plano[["COD_CTA", "IND_CTA"]],
                on="COD_CTA",
                how="left",
            )

        if "IND_CTA" in df_b.columns:
            df_b = df_b[df_b["IND_CTA"].str.upper() == "A"].copy()

        # Detectar Redutoras (heuristicamente por nome ou sinal esperado?)
        # 1. Por Nome: Palavras-chave de redutoras
        mask_redutora = pd.Series([False] * len(df_b), index=df_b.index)
        if "CONTA" in df_b.columns:
            keywords_red = [
                r"\(-",
                "REDUTORA",
                "DEDUÇÃO",
                "PROVISÃO PARA DEPREC",
                "AMORTIZAÇÃO",
                "(-)",
            ]
            pattern_red = "|".join(keywords_red)
            mask_redutora = (
                df_b["CONTA"]
                .str.upper()
                .str.contains(pattern_red, na=False)
            )

        # Filtro de Inversão:
        # ATIVO (01) deve ser Devedor (> 0 no nosso sistema). Se < 0 e não for redutora -> Inversão.
        mask_ativo_errado = (
            (df_b["COD_NAT"].astype(str).str.zfill(2) == "01")
            & (df_b["VL_SLD_FIN_SIG"] < -5)
            & (~mask_redutora)
        )  # Tolerância de R$ 5

        # PASSIVO (02 ou 03) deve ser Credor (< 0 no nosso sistema). Se > 0 e não for redutora -> Inversão.
        mask_passivo_errado = (
            (
                df_b["COD_NAT"]
                .astype(str)
                .str.zfill(2)
                .isin(["02", "03"])
            )
            & (df_b["VL_SLD_FIN_SIG"] > 5)
            & (~mask_redutora)
        )

        erros_df = df_b[mask_ativo_errado | mask_passivo_errado].copy()

        if erros_df.empty:
            self.resultados["5.1_Inversao_Natureza"] = {
                "status": "APROVADO",
                "impacto": 0.0,
            }
        else:
            # --- AJUSTE FORENSE: Evitar inflar impacto com saldo mensal ---
            # Pegamos apenas a última ocorrência de inversão de cada conta para o relatório
            ultimo_erro = (
                erros_df
                .sort_values("DT_FIN")
                .groupby("COD_CTA")
                .last()
                .reset_index()
            )

            impacto = sum(abs(x) for x in ultimo_erro["VL_SLD_FIN_SIG"])

            self.resultados["5.1_Inversao_Natureza"] = {
                "status": "ALERTA",
                "impacto": impacto,
                "msg": f"{len(ultimo_erro)} contas analíticas com saldo invertido detectadas.",
                "detalhes": ultimo_erro,
            }

    def _teste_consistencia_pl_resultado(self):
        if (
            self.df_balancete.empty
            or self.df_diario.empty
        ):
            self.resultados["5.4_Consistencia_PL_Resultado"] = {
                "status": "SKIPPED",
                "impacto": 0.0,
            }
            return

        # 1. Identificar Naturezas
        df_b = self.df_balancete.copy()
        if "COD_NAT" not in df_b.columns and not self.df_plano.empty:
            df_b = pd.merge(
                df_b,
                self.df_plano[["COD_CTA", "COD_NAT"]],
                on="COD_CTA",
                how="left",
            )

        # 2. Calcular Lucro/Prejuízo Apurado (Natureza 04 - Antes do Zeramento)
        # O Processor restaura saldos pré-zeramento, então o VL_SLD_FIN_SIG de Dezembro de contas 04
        # representa o resultado acumulado do ano.
        df_dez = df_b[
            df_b["DT_FIN"].astype(str).str.contains("-12-31")
        ].copy()

        # Filtra apenas Analíticas de Resultado (04)
        if "IND_CTA" not in df_dez.columns and not self.df_plano.empty:
            df_dez = pd.merge(
                df_dez,
                self.df_plano[["COD_CTA", "IND_CTA"]],
                on="COD_CTA",
                how="left",
            )

        mask_resultado = (
            df_dez["COD_NAT"].astype(str).str.zfill(2) == "04"
        ) & (df_dez["IND_CTA"].str.upper() == "A")

        df_apura = df_dez[mask_resultado]
        lucro_esperado = df_apura["VL_SLD_FIN_SIG"].sum()

        # 3. Identificar Destino no PL (Natureza 03) via Lançamentos de Encerramento (E)
        df_d = self.df_diario.copy()
        # Merge com plano para saber quem é 03
        if "COD_NAT" not in df_d.columns and not self.df_plano.empty:
            df_d = pd.merge(
                df_d,
                self.df_plano[["COD_CTA", "COD_NAT"]],
                on="COD_CTA",
                how="left",
            )

        # Filtra lançamentos de encerramento (IND_LCTO = 'E') que atingiram o PL (03)
        mask_transferencia = (df_d["IND_LCTO"] == "E") & (
            df_d["COD_NAT"].fillna("").astype(str).str.zfill(2) == "03"
        )

        df_transf = df_d[mask_transferencia].copy()
        lucro_no_pl = df_transf["VL_SINAL"].sum()  # Saldo líquido transferido para o PL

        # 4. Confronto
        # No sistema, lucro no 04 é Credor (-) e no 03 também (-). Eles devem ser iguais.
        # Na verdade, o zeramento debita o 04 e credita o 03.
        # lucro_esperado (ex: -1000) + lucro_no_pl (ex: -1000) -> -2000?
        # Vamos comparar valores absolutos.
        divergencia = abs(abs(lucro_esperado) - abs(lucro_no_pl))

        df_resumo = pd.DataFrame(
            [
                {
                    "LUCRO_APURADO_DRE": lucro_esperado,
                    "LUCRO_TRANSF_PARA_PL": lucro_no_pl,
                    "DIVERGENCIA": divergencia,
                }
            ]
        )

        if divergencia < 100:  # Tolerância para arredondamentos ou pequenos ajustes
            self.resultados["5.4_Consistencia_PL_Resultado"] = {
                "status": "APROVADO",
                "impacto": 0.0,
                "msg": "Amarração PL vs Resultado consistente.",
                "detalhes": df_resumo,
            }
        else:
            self.resultados["5.4_Consistencia_PL_Resultado"] = {
                "status": "ALERTA",
                "impacto": divergencia,
                "msg": f"Divergência de R$ {divergencia:,.2f} entre Lucro da DRE e transferência para o PL.",
                "detalhes": {
                    "5.4_Sumario_Amarracao": df_resumo,
                    "5.4_Detalhes_Resultado": df_apura[["COD_CTA", "VL_SLD_FIN_SIG"]],
                    "5.4_Lancamentos_PL": df_transf[
                        ["DT_LCTO", "COD_CTA", "VL_SINAL", "HIST"]
                    ],
                },
            }
