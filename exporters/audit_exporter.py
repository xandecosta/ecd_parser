import pandas as pd
import os
import logging
from typing import Dict, Any, List, cast
from exporters.formatting import apply_region_format

logger = logging.getLogger(__name__)


class AuditExporter:
    """
    Exportador especializado para Relatórios de Auditoria Forense.
    Gera Dashboard Excel e Detalhes em Parquet.
    """

    DESCRITIVO_TESTES = {
        "1.1_Cruzamento_Diario_Balancete": "Confronta a soma dos lançamentos (I250) com a variação do saldo (I155). Diferenças indicam quebra de partida dobrada ou erro de transporte.",
        "1.2_Validacao_Hierarquia": "Recalcula os saldos sintéticos a partir dos analíticos. Diferenças indicam manipulação direta de saldos agregados.",
        "2.1_Forward_Roll": "Verifica se o Saldo Inicial do período atual bate com o Saldo Final do período anterior. (Requer histórico).",
        "2.2_Auditoria_I157": "Valida se a transferência de saldos (I157) justifica a diferença de saldos iniciais.",
        "3.1_Consistencia_Natureza": "Confere se contas de Ativo (01) estão mapeadas no Referencial de Ativo (1), Passivo (02) no Passivo (2), etc.",
        "3.2_Contas_Orfas": "Identifica contas analíticas com movimento relevante que não possuem mapeamento para o Plano Referencial da RFB.",
        "4.1_Lei_Benford": "Aplica teste estatístico de Benford no primeiro dígito dos valores. Desvios (MAD > 0.015) sugerem dados fabricados.",
        "4.2_Duplicidades": "Detecta lançamentos com mesma Data, Conta e Valor. Pode indicar erro de importação ou fraude para inflar números.",
        "4.3_Omissao_Encerramento": "Verifica se as contas de Resultado (Natureza 04) terminaram o exercício zeradas. Saldos remanescentes indicam erro grave de encerramento.",
        "5.1_Inversao_Natureza": "Aponta contas com saldo invertido (ex: Caixa credor, Fornecedor devedor) não identificadas como redutoras.",
        "5.2_Estouro_Caixa": "Sub-teste específico para detectar Saldo Credor em contas de Disponibilidade (Caixa/Bancos).",
        "5.3_Passivo_Ficticio": "Detecta contas de Obrigação (Passivo Circulante/Não Circulante) com saldo relevante e sem nenhuma movimentação no período.",
        "5.4_Consistencia_PL_Resultado": "Amarração do Lucro Líquido do exercício com a variação do Patrimônio Líquido.",
    }

    def __init__(self, pasta_saida: str):
        self.pasta_saida = pasta_saida

    def exportar_dashboard(
        self, resultados: Dict[str, Any], nome_projeto: str, prefixo: str = ""
    ) -> List[str]:
        """Gera o arquivo Excel consolidado com prefixo de data. Retorna lista de arquivos criados."""
        # Máscara solicitada: DATA_07_Auditoria.xlsx
        nome_final = f"{prefixo}_07_Auditoria.xlsx" if prefixo else "07_Auditoria.xlsx"
        caminho_xlsx = os.path.join(self.pasta_saida, nome_final)
        arquivos_gerados = []

        try:
            with pd.ExcelWriter(caminho_xlsx, engine="openpyxl") as writer:
                # 1. Aba Capa & Scorecard
                df_scorecard = self._gerar_scorecard(resultados)
                df_scorecard.to_excel(writer, sheet_name="Scorecard", index=False)

                # 2. Aba Descritivo
                df_desc = pd.DataFrame(
                    list(self.DESCRITIVO_TESTES.items()),
                    columns=cast(Any, ["Teste", "Descrição Metodológica"]),
                )
                df_desc.to_excel(
                    writer, sheet_name="Descritivo dos Testes", index=False
                )

                # 3. Abas Detalhadas por Teste
                for teste, res in resultados.items():
                    df_erro = (
                        res.get("erros") if "erros" in res else res.get("detalhes")
                    )

                    if isinstance(df_erro, pd.DataFrame):
                        if not df_erro.empty:
                            df_fmt = self.aplicar_formatacao_regional(df_erro)
                            aba_name = self._sanitizar_nome_aba(teste)
                            df_fmt.to_excel(writer, sheet_name=aba_name, index=False)
                    elif isinstance(df_erro, dict):
                        # Caso especial: Dicionário de DataFrames (ex: Lei de Benford)
                        for sub_nome, sub_df in df_erro.items():
                            if isinstance(sub_df, pd.DataFrame) and not sub_df.empty:
                                df_fmt = self.aplicar_formatacao_regional(sub_df)
                                aba_name = self._sanitizar_nome_aba(sub_nome)
                                df_fmt.to_excel(
                                    writer, sheet_name=aba_name, index=False
                                )

                # --- Aba de Evidências ---
                evidencias_all = []
                for teste, res in resultados.items():
                    if "evidencia" in res and isinstance(
                        res["evidencia"], pd.DataFrame
                    ):
                        df_ev = res["evidencia"].copy()
                        df_ev["TESTE_RELACIONADO"] = teste
                        evidencias_all.append(df_ev)

                if evidencias_all:
                    df_final_ev = pd.concat(evidencias_all, ignore_index=True)
                    cols = ["TESTE_RELACIONADO"] + [
                        c for c in df_final_ev.columns if c != "TESTE_RELACIONADO"
                    ]
                    df_final_ev = df_final_ev[cols]
                    # Formata PT-BR também nas evidências
                    df_final_ev = self.aplicar_formatacao_regional(
                        cast(pd.DataFrame, df_final_ev)
                    )
                    df_final_ev.to_excel(
                        writer, sheet_name="EVIDENCIAS_DETALHADAS", index=False
                    )

            self._aplicar_estilos_visuais(caminho_xlsx)
            logger.info(f"Dashboard de Auditoria gerado: {nome_final}")
            arquivos_gerados.append(f"EXCEL:   {nome_final}")
            return arquivos_gerados

        except Exception as e:
            logger.error(f"Erro ao gerar Dashboard de Auditoria: {e}")
            return []

    def exportar_detalhes_parquet(
        self, resultados: Dict[str, Any], prefixo: str = ""
    ) -> List[str]:
        """Salva os DataFrames de erro e o Scorecard em Parquet na raiz da pasta."""
        arquivos_gerados = []

        # 1. Exporta o Scorecard (Essencial para Consolidação Híbrida)
        df_scorecard = self._gerar_scorecard(resultados)
        # Adiciona o período (prefixo) ao DataFrame do scorecard para identificação no consolidado
        if prefixo:
            df_scorecard.insert(0, "PERIODO", prefixo)

        nome_score = (
            f"{prefixo}_07_Auditoria_Scorecard.parquet"
            if prefixo
            else "07_Auditoria_Scorecard.parquet"
        )
        caminho_score = os.path.join(self.pasta_saida, nome_score)
        df_scorecard.to_parquet(caminho_score, index=False)
        arquivos_gerados.append(f"PARQUET: {nome_score}")

        # 2. Exporta Detalhes de cada Teste
        for teste, res in resultados.items():
            df = res.get("erros") if "erros" in res else res.get("detalhes")
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Máscara solicitada: DATA_07_Auditoria_TESTE.parquet
                if prefixo:
                    nome_parquet = f"{prefixo}_07_Auditoria_{teste}.parquet"
                else:
                    nome_parquet = f"07_Auditoria_{teste}.parquet"

                caminho = os.path.join(self.pasta_saida, nome_parquet)
                df.to_parquet(caminho, index=False)
                arquivos_gerados.append(f"PARQUET: {nome_parquet}")

        return arquivos_gerados

    def _gerar_scorecard(self, resultados: Dict[str, Any]) -> pd.DataFrame:
        rows = []
        for teste, res in resultados.items():
            rows.append(
                {
                    "Teste": teste,
                    "Status": res.get("status", "N/A"),
                    "Impacto Financeiro Est.": res.get("impacto", 0),
                    "Mensagem": res.get("msg", ""),
                }
            )
        df_score = pd.DataFrame(rows)
        return self.aplicar_formatacao_regional(df_score)

    @staticmethod
    def aplicar_formatacao_regional(df: pd.DataFrame) -> pd.DataFrame:
        """Proxy para o utilitário centralizado (Mantém compatibilidade e DRY)."""
        return apply_region_format(df)

    def _sanitizar_nome_aba(self, nome: str) -> str:
        """Limpa o nome da aba para os limites do Excel (31 chars e sem especiais)."""
        proibidos = [":", "\\", "/", "?", "*", "[", "]"]
        limpo = str(nome)[:31]
        for p in proibidos:
            limpo = limpo.replace(p, "_")
        return limpo

    def _aplicar_estilos_visuais(self, caminho_xlsx: str):
        # Implementação futura se necessário (Beautification)
        pass
