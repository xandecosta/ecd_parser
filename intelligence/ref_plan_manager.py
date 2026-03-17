import pandas as pd # type: ignore
import os
import shutil
import json
import re
from typing import Dict, List, Any, cast

# Mapeamento Estático O(1) de Tabelas para Códigos fora da inicialização
TABLE_TO_COD_REF = {
    "L100_A": "1",
    "L300_A": "1",
    "L300_R": "1",
    "L100_B": "3",
    "L300_B": "3",
    "L100_C": "4",
    "L300_C": "4",
    "P100": "2",
    "P150": "2",
    "P150_R": "2",
    "P100_B": "10",
    "P150_B": "10",
    "U100_A": "5",
    "U150_A": "5",
    "U100_B": "6",
    "U150_B": "6",
    "U100_C": "7",
    "U150_C": "7",
    "U100_D": "8",
    "U150_D": "8",
    "U100_E": "9",
    "U150_E": "9",
    "CONTASREF": "10",
    "CONTASREF_BACEN": "20",
    "CONTASREF_SUSEP": "20",
    "CONTASREF_TSE": "10",
}


class RefPlanManager:
    def __init__(self):
        # Determine paths dynamically
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_dir = os.path.dirname(self.current_dir)

        # Define key directories/files
        self.reference_dir = os.path.join(self.base_dir, "data", "reference")
        self.raw_data_dir = os.path.join(self.base_dir, "data", "raw_ref_plans")
        self.schemas_dir = os.path.join(self.base_dir, "schemas", "ref_plans", "data")
        self.analysis_dir = os.path.join(self.base_dir, "data", "analysis")
        self.catalog_path = os.path.join(
            self.base_dir, "schemas", "ref_plans", "ref_catalog.json"
        )

        # Files
        self.full_meta_path = os.path.join(self.reference_dir, "ref_plan_full.csv")
        self.filtered_meta_path = os.path.join(
            self.reference_dir, "ref_plan_filtered.csv"
        )
        self.conflicts_report_path = os.path.join(
            self.analysis_dir, "structural_conflicts_report.csv"
        )

    def get_cod_plan_ref(self, tabela: str) -> str:
        """Mapeia o nome da tabela para o respectivo COD_PLAN_REF usando tabela hash estática."""
        return TABLE_TO_COD_REF.get(tabela.upper(), "UNKNOWN")

    def _scan_raw_plans(self) -> pd.DataFrame:
        """Realiza a varredura automática dos arquivos brutos na pasta raw_ref_plans com parsing seguro."""
        print(f"\nBuscando arquivos de planos referenciais em: {self.raw_data_dir}")
        if not os.path.exists(self.raw_data_dir):
            raise FileNotFoundError(f"Diretório não encontrado: {self.raw_data_dir}")

        arquivos = os.listdir(self.raw_data_dir)
        registros = []

        # Regex para validar a estrutura do nome do arquivo (ex: SPEDCONTABIL_CONTAS_REFERENCIAIS$L100_A$1$20210515)
        pattern = re.compile(r"^(.*?)\$(.*?)\$(.*?)\$(.*?)$")

        for nome_arq in arquivos:
            caminho_completo = os.path.join(self.raw_data_dir, nome_arq)
            if os.path.isdir(caminho_completo) or nome_arq.startswith("."):
                continue

            match = pattern.match(nome_arq)
            if not match:
                continue

            agrupador, nome_tabela_bruto, versao_str, _ = match.groups()

            try:
                versao = int(versao_str)
            except ValueError:
                versao = 1

            if agrupador.startswith("SPEDCONTABIL_CONTAS_REFERENCIAIS"):
                ano = "<2014"
                if "CONTASREF" in nome_tabela_bruto:
                    tabela_codigo = nome_tabela_bruto.replace("SPEDCONTABIL_", "")
                else:
                    tabela_codigo = "UNKNOWN"
            elif agrupador.startswith("SPEDCONTABIL_DINAMICO_"):
                ano = agrupador.replace("SPEDCONTABIL_DINAMICO_", "")
                tabela_codigo = nome_tabela_bruto.replace("SPEDECF_DINAMICA_", "")
            else:
                continue

            cod_plan_ref = self.get_cod_plan_ref(tabela_codigo)
            if cod_plan_ref == "UNKNOWN":
                continue

            cabecalho = ""
            try:
                with open(caminho_completo, "r", encoding="latin1") as f:
                    cabecalho = f.readline().strip()
            except (FileNotFoundError, PermissionError) as e:
                print(f"Aviso - Não foi possível ler cabeçalho de {nome_arq}: {e}")

            registros.append(
                {
                    "TabelaDinamica": nome_arq,
                    "CodigoTabDinamica": tabela_codigo,
                    "VersaoTabDinamica": versao,
                    "Ano": ano,
                    "COD_PLAN_REF": cod_plan_ref,
                    "ESTRUTURA_COLUNAS": cabecalho,
                }
            )

        df = pd.DataFrame(registros)
        print(
            f"Arquivos válidos identificados na varredura: {len(df) if not df.empty else 0}"
        )
        return df

    def filter_metadata(self) -> pd.DataFrame:
        df = self._scan_raw_plans()

        if df.empty:
            raise ValueError("Nenhum arquivo de plano referencial válido encontrado.")

        df_sorted = df.sort_values(
            by=["CodigoTabDinamica", "Ano", "VersaoTabDinamica"],
            ascending=[True, True, False],
        )

        df_filtered = df_sorted.drop_duplicates(
            subset=["CodigoTabDinamica", "Ano"], keep="first"
        ).copy()

        df_filtered.sort_values(by=["CodigoTabDinamica", "Ano"], inplace=True)

        print(
            f"Metadados otimizados. Tabelas únicas que serão agrupadas: {len(df_filtered)}"
        )

        os.makedirs(self.reference_dir, exist_ok=True)
        df_filtered.to_csv(
            self.filtered_meta_path, sep=";", index=False, encoding="utf-8-sig"
        )

        return df_filtered

    def parse_ano_range(self, ano_str: str):
        ano_str = str(ano_str).strip()
        if ano_str == "<2014":
            return 0, 2013
        if ano_str == ">=2021":
            return 2021, 9999
        try:
            if ano_str.startswith(">="):
                val = int(ano_str.replace(">=", ""))
                return val, 9999
            if ano_str.startswith("<"):
                val = int(ano_str.replace("<", ""))
                return 0, val - 1

            ano = int(ano_str)
            return ano, ano
        except Exception:
            return 0, 9999

    def parse_year_safe(self, year_val: Any) -> int:
        try:
            year_str = str(year_val).strip()
            if year_str.startswith(">="):
                return int(year_str.replace(">=", ""))
            elif year_str.startswith("<"):
                return int(year_str.replace("<", ""))
            else:
                return int(year_str)
        except ValueError:
            return 0

    def _clean_unified_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop_duplicates(subset=["CODIGO"], keep="first", inplace=True)

        if "NATUREZA" in df.columns:
            df["NATUREZA"] = df["NATUREZA"].apply(
                lambda x: str(x).zfill(2) if pd.notna(x) and str(x).strip() else ""
            )
        if "DESCRICAO" in df.columns:
            df["DESCRICAO"] = df["DESCRICAO"].str.strip()

        return df

    def _read_raw_plan_file(self, file_path: str, estrutura: str) -> pd.DataFrame:
        if "ORDEM" in estrutura:
            cols = [
                "CODIGO",
                "DESCRICAO",
                "DT_INI",
                "DT_FIM",
                "ORDEM",
                "TIPO",
                "COD_SUP",
                "NIVEL",
                "NATUREZA",
            ]
        else:
            cols = [
                "CODIGO",
                "DESCRICAO",
                "DT_INI",
                "DT_FIM",
                "TIPO",
                "COD_SUP",
                "NIVEL",
                "NATUREZA",
                "UTILIZACAO",
            ]

        try:
            return pd.read_csv(
                file_path,
                sep="|",
                names=cols,
                header=None,
                skiprows=1,
                dtype=str,
                encoding="latin1",
                quoting=3,
                index_col=False,
            ).fillna("")
        except FileNotFoundError:
            print(f"      Erro de acesso (Arquivo não encontrado): {file_path}")
            return pd.DataFrame()
        except pd.errors.ParserError as pe:
            print(f"      Erro de parsing no Pandas ({file_path}): {pe}")
            return pd.DataFrame()
        except pd.errors.EmptyDataError:
            print(f"      Arquivo vazio encontrado ({file_path})")
            return pd.DataFrame()

    def _update_catalog_dict(
        self,
        catalog: Dict[str, Any],
        cod_ref: str,
        ano_str: str,
        info_versao: str,
        output_filename: str,
        layout_type: str,
    ):
        ano_min, ano_max = self.parse_ano_range(ano_str)
        if cod_ref not in catalog:
            catalog[cod_ref] = {}

        catalog[cod_ref][ano_str] = {
            "range": [ano_min, ano_max],
            "plans": {
                "REF": {
                    info_versao: {
                        "file": output_filename,
                        "tipo_demo": "Unificado (Balanço + Resultado)",
                        "layout": layout_type,
                    }
                }
            },
        }

    def standardize_plans(self):
        df_meta = self.filter_metadata()

        print("\n>>> INICIANDO PADRONIZAÇÃO UNIFICADA...")

        if os.path.exists(self.schemas_dir):
            print(f"Limpando pasta de schemas: {self.schemas_dir}")
            shutil.rmtree(self.schemas_dir, ignore_errors=True)
        os.makedirs(self.schemas_dir, exist_ok=True)

        catalog: Dict[str, Any] = {}
        grouped = df_meta.groupby(["COD_PLAN_REF", "Ano"])

        for keys, group in grouped:
            cod_ref_raw, ano_str_raw = keys  # type: ignore
            cod_ref = str(cod_ref_raw)
            ano_str = str(ano_str_raw)

            print(f"Unificando Planos: Instituição {cod_ref} | Ano {ano_str}")

            dfs_unificados = []
            info_versao = ""
            layout_type = "ref_standard"

            group_df = cast(pd.DataFrame, group)
            for _, row in group_df.iterrows():
                file_name = str(row["TabelaDinamica"])
                versao = str(row["VersaoTabDinamica"])
                estrutura = str(row["ESTRUTURA_COLUNAS"])

                info_versao = versao
                if "ORDEM" in estrutura:
                    layout_type = "ref_dynamic"

                file_path = os.path.join(self.raw_data_dir, file_name)
                df_part = self._read_raw_plan_file(file_path, estrutura)

                if not df_part.empty:
                    dfs_unificados.append(df_part)

            if dfs_unificados:
                df_final = pd.concat(dfs_unificados, ignore_index=True)
                df_final = self._clean_unified_dataframe(df_final)

                output_filename = f"REF_{cod_ref}_{ano_str}.csv".replace(
                    ">=", "GE"
                ).replace("<", "LT")
                output_path = os.path.join(self.schemas_dir, output_filename)

                os.makedirs(self.schemas_dir, exist_ok=True)
                df_final.to_csv(output_path, sep="|", index=False, encoding="utf-8")

                self._update_catalog_dict(
                    catalog, cod_ref, ano_str, info_versao, output_filename, layout_type
                )

        os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
        with open(self.catalog_path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)

        print(f"\nStandardization complete. Catalog saved to {self.catalog_path}")

    def audit_plans(self):
        print("\n--- Starting Consolidated Audit (Evolution + Integrity) ---")
        os.makedirs(self.analysis_dir, exist_ok=True)
        all_conflicts = []

        if not os.path.exists(self.filtered_meta_path):
            print("Filtered metadata not found. Running filter...")
            df_meta = self.filter_metadata()
        else:
            df_meta = pd.read_csv(
                self.filtered_meta_path, sep=";", encoding="utf-8-sig", dtype=str
            )

        unique_refs: List[str] = [
            str(r) for r in df_meta["COD_PLAN_REF"].unique()
            if str(r).strip() in ["1", "2"]
        ]
        print(f"Processing restricted to COD_PLAN_REF types: {unique_refs}")

        for cod_ref in unique_refs:
            print(f"\nProcessing Plan: {cod_ref}")
            df_plan = df_meta[df_meta["COD_PLAN_REF"] == cod_ref].copy()

            yearly_data: Dict[str, pd.DataFrame] = {}
            grouped = df_plan.groupby("Ano")
            
            # Sorted chronologically
            years_list = [(self.parse_year_safe(k), k) for k in grouped.groups.keys()]
            years_list.sort(key=lambda x: x[0])

            for y_val, year_key in years_list:
                ano_str = str(year_key)
                unified_filename = f"REF_{cod_ref}_{ano_str}.csv".replace(
                    ">=", "GE"
                ).replace("<", "LT")
                file_path = os.path.join(self.schemas_dir, unified_filename)

                if not os.path.exists(file_path):
                    continue

                print(f"  Lendo plano para auditoria: {unified_filename}")
                try:
                    df_full_year = pd.read_csv(file_path, sep="|", dtype=str, encoding="utf-8")

                    cols_to_keep = [
                        "CODIGO",
                        "DESCRICAO",
                        "NATUREZA",
                        "NIVEL",
                        "COD_SUP",
                        "TIPO",
                    ]
                    existing_cols = [c for c in cols_to_keep if c in df_full_year.columns]
                    df_full_year = df_full_year[existing_cols]  # type: ignore
                    df_full_year.drop_duplicates(subset=["CODIGO"], inplace=True)  # type: ignore

                    yearly_data[ano_str] = df_full_year  # type: ignore

                except Exception as e:
                    print(f"  Error reading {file_path}: {e}")

            # 1. Integrity Check via Vectorized Ops
            plan_conflicts = self._run_vectorized_integrity_check(cod_ref, yearly_data)
            all_conflicts.extend(plan_conflicts)

            # 2. Store for Evolution Report
            self._generate_evolution_report(cod_ref, yearly_data)

        # Saves all aggregated conflicts
        self._save_conflict_report(all_conflicts)

    def _run_vectorized_integrity_check(self, cod_ref: str, yearly_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Verificação vetorizada de O(N) para detectar mudanças em contas ao longo dos anos."""
        if not yearly_data:
            return []

        all_dfs = []
        for ano, df_year in yearly_data.items():
            df_part = df_year.copy()
            df_part["ANO_STR"] = ano
            df_part["ANO_REF"] = self.parse_year_safe(ano)
            all_dfs.append(df_part)

        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all.sort_values(by=["CODIGO", "ANO_REF"], inplace=True)

        conflicts = []
        cols_check = ["COD_SUP", "NATUREZA", "TIPO"]

        # Garante as colunas comparativas
        for c in cols_check:
            if c not in df_all.columns:
                df_all[c] = ""
            df_all[c] = df_all[c].fillna("").astype(str).str.strip()

        # Shift the whole DataFrame by 1 within each account
        grouped = df_all.groupby("CODIGO")[cols_check + ["ANO_STR"]]
        df_shifted = grouped.shift(1)

        # Detect conflicts for each column
        for col in cols_check:
            # Shifted must not be null (e.g. first appearance) and not empty
            mask_conflict = cast(Any, (df_all[col] != df_shifted[col]) & df_shifted[col].notna() & (df_shifted[col] != ""))

            if mask_conflict.any():
                conflict_rows = df_all[mask_conflict]
                prev_rows = df_shifted[mask_conflict]

                # Iterando apenas onde a máscara vetorial encontrou conflitos
                for idx, row in conflict_rows.iterrows():
                    conflicts.append({
                        "COD_PLAN_REF": cod_ref,
                        "CODIGO": row["CODIGO"],
                        "TIPO_CONFLITO": f"MUDANCA_{col}",
                        "VALOR_ANTIGO": prev_rows.loc[idx, col],
                        "ANO_ANTIGO": prev_rows.loc[idx, "ANO_STR"],
                        "VALOR_NOVO": row[col],
                        "ANO_NOVO": row["ANO_STR"],
                    })

        return conflicts

    def _generate_evolution_report(self, cod_ref: str, yearly_data: Dict[str, pd.DataFrame]):
        if not yearly_data:
            print("  No data found for evolution report.")
            return

        years_sorted = sorted(yearly_data.keys(), key=self.parse_year_safe)

        all_dfs = []
        for y in years_sorted:
            df_y = yearly_data[y].copy()
            df_y["_ANO_REF"] = self.parse_year_safe(y)
            all_dfs.append(df_y)

        df_concat = pd.concat(all_dfs, ignore_index=True)

        print(
            f"  Generating evolution report for {df_concat['CODIGO'].nunique()} accounts..."
        )

        df_canonical = df_concat.sort_values("_ANO_REF").drop_duplicates(
            subset=["CODIGO"], keep="last"
        )

        base_cols_all = ["CODIGO", "DESCRICAO", "TIPO", "COD_SUP", "NIVEL", "NATUREZA"]
        existing_base = [c for c in base_cols_all if c in df_canonical.columns]
        df_comparison = df_canonical[existing_base].copy()  # type: ignore

        for year in years_sorted:
            # Map index required to be CODIGO for vectorized lookup
            s_desc = yearly_data[year].set_index("CODIGO")["DESCRICAO"]
            df_comparison[f"ANO_{year}"] = df_comparison["CODIGO"].map(s_desc)  # type: ignore

        base_cols = [
            "CODIGO",
            "DESCRICAO",
            "TIPO",
            "COD_SUP",
            "NIVEL",
            "NATUREZA",
        ]
        year_cols = [f"ANO_{y}" for y in years_sorted]
        final_cols = base_cols + year_cols

        existing_cols = [c for c in final_cols if c in df_comparison.columns]
        df_comparison = df_comparison[existing_cols]  # type: ignore

        safe_cod = str(cod_ref).strip()
        output_path = os.path.join(
            self.analysis_dir, f"ref_plan_evolution_{safe_cod}.csv"
        )

        df_comparison.to_csv(output_path, sep="|", index=False, encoding="utf-8-sig")  # type: ignore
        print(f"  Evolution Report saved: {output_path}")

    def _save_conflict_report(self, conflicts: List[Dict[str, Any]]):
        if not conflicts:
            print("\nSUCCESS: No structural conflicts found!")
            if os.path.exists(self.conflicts_report_path):
                os.remove(self.conflicts_report_path)
        else:
            print(f"\nWARNING: Found {len(conflicts)} structural conflicts.")
            df_conflicts = pd.DataFrame(conflicts)
            df_conflicts.to_csv(
                self.conflicts_report_path, sep="|", index=False, encoding="utf-8-sig"
            )
            print(f"Conflict report saved to: {self.conflicts_report_path}")

if __name__ == "__main__":
    manager = RefPlanManager()

    # 1. Standardize (Generate CSVs)
    manager.standardize_plans()

    # 2. Audit (Evolution + Integrity)
    manager.audit_plans()
