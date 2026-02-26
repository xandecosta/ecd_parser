import pandas as pd
import json
import os
import logging
import shutil
from typing import Dict, Any, cast

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Configurações de Caminho (Podem ser injetadas no futuro)
_INPUT_CAMPOS = r"data/reference/ref_plan_fields_by_register.csv"
_INPUT_REGISTROS = r"data/reference/ref_plan_registers_by_layout.csv"
_OUTPUT_DIR = r"schemas/ecd_layouts"


def _load_and_clean_csv(path: str) -> pd.DataFrame:
    """Carrega CSV com tratamento de encoding e limpeza de strings."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo de referência não encontrado: {path}")

    df = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)
    # Normaliza nomes de colunas (strip)
    df.columns = pd.Index([str(c).strip() for c in df.columns])
    # Limpa espaços em branco em todas as células de texto
    return cast(
        pd.DataFrame, df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    )


def _safe_int_convert(series: pd.Series, default: int = 0) -> pd.Series:
    """Converte série para inteiro de forma resiliente."""
    # Trata placeholders comuns da RFB antes da conversão
    s_clean = series.replace({"-": "0", "": str(default), None: str(default)})
    return (
        cast(pd.Series, pd.to_numeric(s_clean, errors="coerce"))
        .fillna(default)
        .astype(int)
    )


def compile_ecd_layouts():
    """
    Orquestra a compilação dos layouts ECD de CSV para JSON.
    Refatorado para Alta Performance (O(N)) e Modularidade.
    """
    print("\n>>> INICIANDO COMPILAÇÃO DE LAYOUTS ECD...")

    try:
        # 1. Preparação do Ambiente
        if os.path.exists(_OUTPUT_DIR):
            logging.info(f"Limpando diretório de saída: {_OUTPUT_DIR}")
            shutil.rmtree(_OUTPUT_DIR, ignore_errors=True)
        os.makedirs(_OUTPUT_DIR, exist_ok=True)

        # 2. Carga e Sanitização
        df_campos = _load_and_clean_csv(_INPUT_CAMPOS)
        df_registros = _load_and_clean_csv(_INPUT_REGISTROS)

        # 3. Tipagem de Colunas Críticas
        for col in ["Decimal", "Ordem", "Tamanho"]:
            df_campos[col] = _safe_int_convert(cast(pd.Series, df_campos[col]))

        df_registros["Nivel"] = _safe_int_convert(
            cast(pd.Series, df_registros["Nivel"])
        )

        # 4. Processamento Otimizado por Versão (Vectorized GroupBy)
        # Agrupamos por versão para evitar .loc repetitivos
        for versao, group_versao in df_campos.groupby("Versao"):
            # Verifica se a chave é nula ou vazia de forma segura para o linter
            if pd.isna(versao) is True or str(versao).strip() == "":
                continue

            versao_str = str(versao)
            logging.info(f"Processando Layout Versão: {versao_str}")

            # Mapeamento de Layout (Cruzamento com tabela de hierarquia)
            versao_num = int(float(versao_str))
            col_leiaute = f"Leiaute_{versao_num}"

            # Determina registros válidos e seus níveis para esta versão
            if col_leiaute in df_registros.columns:
                df_reg_validos = df_registros[df_registros[col_leiaute] == "S"]
                map_niveis = dict(
                    zip(df_reg_validos["Registro"], df_reg_validos["Nivel"])
                )
            else:
                logging.warning(
                    f"  Coluna {col_leiaute} ausente. Fallback para todos os registros."
                )
                map_niveis = {r: 0 for r in group_versao["REG"].unique()}

            schema_json: Dict[str, Any] = {}

            # Agrupamos campos por Registro dentro da versão (Performance O(N))
            for reg, group_reg in group_versao.groupby("REG"):
                reg_str = str(reg)
                if reg_str not in map_niveis:
                    continue

                # Monta lista de campos ordenada
                campos_ordenados = group_reg.sort_values("Ordem")
                lista_campos = [
                    {
                        "nome": row["CampoUnico"],
                        "tipo": row["Tipo"],
                        "tamanho": int(row["Tamanho"]),
                        "decimal": int(row["Decimal"]),
                    }
                    for _, row in campos_ordenados.iterrows()
                ]

                schema_json[reg_str] = {
                    "nivel": int(map_niveis[reg_str]),
                    "campos": lista_campos,
                }

            # 5. Persistência
            output_path = os.path.join(_OUTPUT_DIR, f"layout_{versao_str}.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(schema_json, f, indent=2, ensure_ascii=False)

        logging.info("Compilação de layouts finalizada com sucesso.")

    except Exception as e:
        logging.error(f"FALHA NA COMPILAÇÃO: {e}")
        raise


if __name__ == "__main__":
    compile_ecd_layouts()
