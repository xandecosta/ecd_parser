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
        """
        Gera CSVs de auditoria (Opção D):
          1. CSV unificado: Scorecard + Descritivo dos Testes (5 colunas)
          2. CSVs individuais por teste de detalhe
        Retorna lista de arquivos criados.
        """
        arquivos_gerados = []

        try:
            # --- 1. CSV UNIFICADO: Scorecard + Descritivo ---
            df_desc = pd.DataFrame(
                list(self.DESCRITIVO_TESTES.items()),
                columns=cast(Any, ["Teste", "Descrição Metodológica"]),
            )
            df_score = self._gerar_scorecard_raw(resultados)
            df_unificado = pd.merge(df_desc, df_score, on="Teste", how="left")
            # Garante a ordem das colunas conforme solicitado
            cols_ordem = [
                "Teste",
                "Descrição Metodológica",
                "Status",
                "Impacto Financeiro Est.",
                "Mensagem",
            ]
            df_unificado = df_unificado[
                [c for c in cols_ordem if c in df_unificado.columns]
            ]
            df_unificado = self.aplicar_formatacao_regional(
                cast(pd.DataFrame, df_unificado)
            )

            nome_scorecard = (
                f"{prefixo}_07_Auditoria_Scorecard.csv"
                if prefixo
                else "07_Auditoria_Scorecard.csv"
            )
            caminho_scorecard = os.path.join(self.pasta_saida, nome_scorecard)
            df_unificado.to_csv(
                caminho_scorecard, index=False, sep=";", encoding="utf-8-sig"
            )
            arquivos_gerados.append(f"CSV:     {nome_scorecard}")

            # --- 2. CSVs INDIVIDUAIS POR TESTE ---
            for teste, res in resultados.items():
                df_erro = res.get("erros") if "erros" in res else res.get("detalhes")

                if isinstance(df_erro, pd.DataFrame):
                    if not df_erro.empty:
                        df_fmt = self.aplicar_formatacao_regional(df_erro)
                        nome_csv = self._montar_nome_csv(prefixo, teste)
                        caminho_csv = os.path.join(self.pasta_saida, nome_csv)
                        df_fmt.to_csv(
                            caminho_csv, index=False, sep=";", encoding="utf-8-sig"
                        )
                        arquivos_gerados.append(f"CSV:     {nome_csv}")

                elif isinstance(df_erro, dict):
                    # Caso especial: Dicionário de DataFrames (ex: Lei de Benford)
                    for sub_nome, sub_df in df_erro.items():
                        if isinstance(sub_df, pd.DataFrame) and not sub_df.empty:
                            df_fmt = self.aplicar_formatacao_regional(sub_df)
                            nome_csv = self._montar_nome_csv(
                                prefixo, f"{teste}_{sub_nome}"
                            )
                            caminho_csv = os.path.join(self.pasta_saida, nome_csv)
                            df_fmt.to_csv(
                                caminho_csv,
                                index=False,
                                sep=";",
                                encoding="utf-8-sig",
                            )
                            arquivos_gerados.append(f"CSV:     {nome_csv}")

            logger.info(
                f"Relatório de Auditoria CSV gerado: {len(arquivos_gerados)} arquivo(s)"
            )
            return arquivos_gerados

        except Exception as e:
            logger.error(f"Erro ao gerar CSVs de Auditoria: {e}")
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

    def _gerar_scorecard_raw(self, resultados: Dict[str, Any]) -> pd.DataFrame:
        """Retorna o scorecard sem formatação regional (para merge com Descritivo)."""
        rows = []
        for teste, res in resultados.items():
            impacto = res.get("impacto", 0.0)
            # Garantia absoluta: converte Decimal para float64
            # para evitar erro de tipo objeto no Parquet/Arrow
            try:
                impacto_fmt = float(str(impacto))
            except (ValueError, TypeError):
                impacto_fmt = 0.0

            rows.append(
                {
                    "Teste": teste,
                    "Status": res.get("status", "N/A"),
                    "Impacto Financeiro Est.": impacto_fmt,
                    "Mensagem": res.get("msg", ""),
                }
            )
        return pd.DataFrame(rows)

    def _gerar_scorecard(self, resultados: Dict[str, Any]) -> pd.DataFrame:
        """Retorna o scorecard com formatação regional (usado pelo exportar_detalhes_parquet)."""
        return self.aplicar_formatacao_regional(self._gerar_scorecard_raw(resultados))

    def _montar_nome_csv(self, prefixo: str, teste: str) -> str:
        """Monta o nome do arquivo CSV seguindo o padrão: PERIODO_07_Auditoria_TESTE.csv"""
        # Sanitiza: remove caracteres proibidos em nomes de arquivo
        proibidos = [":", "\\", "/", "?", "*", "[", "]"]
        nome_limpo = str(teste)
        for p in proibidos:
            nome_limpo = nome_limpo.replace(p, "_")
        if prefixo:
            return f"{prefixo}_07_Auditoria_{nome_limpo}.csv"
        return f"07_Auditoria_{nome_limpo}.csv"

    @staticmethod
    def aplicar_formatacao_regional(df: pd.DataFrame) -> pd.DataFrame:
        """Proxy para o utilitário centralizado (Mantém compatibilidade e DRY)."""
        return apply_region_format(df)
