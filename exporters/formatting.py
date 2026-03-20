import pandas as pd


def apply_region_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica formatação PT-BR (Datas e Valores) de forma vetorizada (Alta Performance).
    Padronizado para v2.5.0+.
    """
    if df.empty:
        return df

    df_out = df.copy()

    for col in df_out.columns:
        col_str = str(col).upper()

        # 1. Tratamento de DATAS (Prefixos DT_ ou PERIODO)
        if col_str.startswith("DT_") or col_str == "PERIODO":
            if pd.api.types.is_datetime64_any_dtype(df_out[col]):
                df_out[col] = df_out[col].dt.strftime("%d/%m/%Y")
            else:
                # Tentativa de conversão resiliente
                try:
                    df_out[col] = pd.to_datetime(
                        df_out[col], errors="coerce"
                    ).dt.strftime("%d/%m/%Y")
                except Exception:
                    pass

        # 2. Tratamento de VALORES (Prefixos VL_, DIF_, IMPACTO, VLR_)
        elif any(x in col_str for x in ["VL_", "DIF_", "IMPACTO", "VLR_"]):
            # VETORIZAÇÃO OURO: Evita .apply(lambda) lento
            # Converte para string e troca ponto por vírgula em uma única operação de série
            mask = df_out[col].notna()
            # Garante que é numérico antes de formatar para evitar quebrar strings acidentais
            if pd.api.types.is_numeric_dtype(df_out[col]):
                df_out.loc[mask, col] = (
                    df_out.loc[mask, col]
                    .round(2)
                    .astype(str)
                    .str.replace(".", ",", regex=False)
                )

    return df_out


# Prefixos de colunas que devem ser tratadas como valores numéricos
_VL_PREFIXES = ("VL_", "DIF_", "VLR_")


def ensure_numeric_vl_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas cujo nome começa com prefixos de valor (VL_, DIF_, VLR_)
    para float64, normalizando strings com vírgula decimal antes da conversão.

    Deve ser chamado ANTES de to_csv() para garantir que o parâmetro
    decimal="," do pandas funcione corretamente com dados vindos do parser
    SPED, onde valores numéricos chegam como strings (ex: "1234.56" ou "1234,56").

    Os arquivos Parquet NÃO devem passar por esta função — ela é exclusiva
    para a camada de exportação CSV.
    """
    if df.empty:
        return df

    df_out = df.copy()
    for col in df_out.columns:
        if str(col).upper().startswith(_VL_PREFIXES):
            if not pd.api.types.is_numeric_dtype(df_out[col]):
                # Normaliza separador decimal antes da conversão (vírgula → ponto)
                df_out[col] = (
                    df_out[col]
                    .astype(str)
                    .str.replace(",", ".", regex=False)
                    .pipe(lambda s: pd.to_numeric(s, errors="coerce"))
                    .fillna(0.0)
                )
    return df_out
