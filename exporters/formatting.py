import pandas as pd
from typing import cast


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

    return cast(pd.DataFrame, df_out)
