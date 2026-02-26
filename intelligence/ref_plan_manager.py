import pandas as pd
import os
import shutil

import json
from typing import cast, Dict, List, Any


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

        # State for auditing
        self.conflicts: List[Dict[str, Any]] = []

        # Mapeamento Estático O(1) de Tabelas para Códigos
        self.table_to_cod_ref = {
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

    def get_cod_plan_ref(self, tabela: str) -> str:
        """Mapeia o nome da tabela (ex: L100_A) para o respectivo COD_PLAN_REF usando tabela hash."""
        return self.table_to_cod_ref.get(tabela.upper(), "UNKNOWN")

    def _scan_raw_plans(self) -> pd.DataFrame:
        """Realiza a varredura automática dos arquivos brutos na pasta raw_ref_plans."""
        print(f"\nBuscando arquivos de planos referenciais em: {self.raw_data_dir}")
        if not os.path.exists(self.raw_data_dir):
            raise FileNotFoundError(f"Diretório não encontrado: {self.raw_data_dir}")

        arquivos = os.listdir(self.raw_data_dir)
        registros = []

        for nome_arq in arquivos:
            # Ignora pastas ou arquivos ocultos, foca apenas nos arquivos com delimitador $
            caminho_completo = os.path.join(self.raw_data_dir, nome_arq)
            if os.path.isdir(caminho_completo) or nome_arq.startswith("."):
                continue

            if "$" not in nome_arq:
                continue

            partes = nome_arq.split("$")
            if len(partes) != 4:
                continue

            agrupador = partes[0]
            nome_tabela_bruto = partes[1]
            try:
                versao = int(partes[2])
            except ValueError:
                versao = 1

            # Processando Ano e Tipo de Tabela
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
                continue  # Ignora tabelas que não são contabilizadas no nosso mapa

            # Lendo a primeira linha para obter ESTRUTURA_COLUNAS
            try:
                with open(caminho_completo, "r", encoding="latin1") as f:
                    cabecalho = f.readline().strip()
            except Exception:
                cabecalho = ""

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
        """
        Escaneia os arquivos dinamicamente, mantendo a maior versão de cada tabela/ano,
        e salva a versão filtrada como histórico no diretório base.
        """
        df = self._scan_raw_plans()

        if df.empty:
            raise ValueError("Nenhum arquivo de plano referencial válido encontrado.")

        # Ordena para pegar a maior versão: Ascending keys, Descending Version
        df_sorted = df.sort_values(
            by=["CodigoTabDinamica", "Ano", "VersaoTabDinamica"],
            ascending=[True, True, False],
        )

        # Remove duplicados mantendo a primeira ocorrência (que é a maior versão graças à ordenação)
        df_filtered = df_sorted.drop_duplicates(
            subset=["CodigoTabDinamica", "Ano"], keep="first"
        ).copy()

        df_filtered.sort_values(by=["CodigoTabDinamica", "Ano"], inplace=True)

        print(
            f"Metadados otimizados. Tabelas únicas que serão agrupadas: {len(df_filtered)}"
        )

        # Salva o resultado filtrado para fins de relatório/logging (como o sistema antigo fazia)
        os.makedirs(self.reference_dir, exist_ok=True)
        df_filtered.to_csv(
            self.filtered_meta_path, sep=";", index=False, encoding="utf-8-sig"
        )

        return df_filtered

    def parse_ano_range(self, ano_str: str):
        """Converts year strings to numeric ranges."""
        ano_str = str(ano_str).strip()
        if ano_str == "<2014":
            return 0, 2013
        if ano_str == ">=2021":
            return 2021, 9999
        try:
            # Handle cases like ">=2014" or "<2020" if they appear elsewhere or generic cleaning
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
        """Helper to safely extract integer year from metadata string."""
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
        """Aplica pipelines de limpeza e padronização (ETL) no dataframe."""
        # Remove duplicidade de códigos que possam existir entre planos
        df.drop_duplicates(subset=["CODIGO"], keep="first", inplace=True)

        if "NATUREZA" in df.columns:
            df["NATUREZA"] = df["NATUREZA"].apply(
                lambda x: str(x).zfill(2) if x and str(x).strip() else ""
            )
        if "DESCRICAO" in df.columns:
            df["DESCRICAO"] = df["DESCRICAO"].str.strip()

        return df

    def _read_raw_plan_file(self, file_path: str, estrutura: str) -> pd.DataFrame:
        """Determina as colunas com base na estrutura e lê o arquivo bruto otimizado."""
        # Fix 3: Desacopla as tipagens da estrutura com blocos mais limpos
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
            # Fix 4: Removed engine='python' logic to fallback into the lightning fast C-engine safely
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
        except Exception as e:
            print(f"      Erro ao ler bruto {file_path}: {e}")
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
        """
        Lê os metadados filtrados, agrupa por COD_PLAN_REF + Ano,
        concatena os arquivos TXT correspondentes (Balanço, DRE, etc)
        e gera um CSV unificado por Instituição/Ano.
        """
        # Ensure latest metadata is active
        df_meta = self.filter_metadata()

        print("\n>>> INICIANDO PADRONIZAÇÃO UNIFICADA...")

        # 1. Limpeza da pasta de schemas/data
        if os.path.exists(self.schemas_dir):
            print(f"Limpando pasta de schemas: {self.schemas_dir}")
            shutil.rmtree(self.schemas_dir, ignore_errors=True)
        os.makedirs(self.schemas_dir, exist_ok=True)

        catalog: Dict[str, Any] = {}

        # Agrupamos por Instituição e Ano para criar arquivos únicos
        grouped = df_meta.groupby(["COD_PLAN_REF", "Ano"])

        for keys, group in grouped:
            cod_ref_raw, ano_str_raw = cast(tuple, keys)
            cod_ref = str(cod_ref_raw)
            ano_str = str(ano_str_raw)

            print(f"Unificando Planos: Instituição {cod_ref} | Ano {ano_str}")

            dfs_unificados = []
            info_versao = ""
            layout_type = "ref_standard"

            for _, row in group.iterrows():
                file_name = str(row["TabelaDinamica"])
                versao = str(row["VersaoTabDinamica"])
                estrutura = str(row["ESTRUTURA_COLUNAS"])

                # Snapshot da última versão/layout encontrada no grupo
                info_versao = versao
                if "ORDEM" in estrutura:
                    layout_type = "ref_dynamic"

                file_path = os.path.join(self.raw_data_dir, file_name)
                # O arquivo já possui a nomenclatura original do disco (sem extensão obrigatória)
                if not os.path.exists(file_path):
                    continue

                # Fix 2: Função Deus extraída e mitigada (Isolamento da Responsabilidade de Leitura)
                df_part = self._read_raw_plan_file(file_path, estrutura)

                if not df_part.empty:
                    dfs_unificados.append(df_part)

            if dfs_unificados:
                df_final = pd.concat(dfs_unificados, ignore_index=True)

                # Executa a limpeza extraída
                df_final = self._clean_unified_dataframe(df_final)

                # Nome unificado: REF_{Instituicao}_{Ano}.csv
                output_filename = f"REF_{cod_ref}_{ano_str}.csv".replace(
                    ">=", "GE"
                ).replace("<", "LT")
                output_path = os.path.join(self.schemas_dir, output_filename)

                os.makedirs(self.schemas_dir, exist_ok=True)
                df_final.to_csv(output_path, sep="|", index=False, encoding="utf-8")

                # Atualiza Catálogo
                self._update_catalog_dict(
                    catalog, cod_ref, ano_str, info_versao, output_filename, layout_type
                )

        # Save Catalog JSON
        os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
        with open(self.catalog_path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)

        print(f"\nStandardization complete. Catalog saved to {self.catalog_path}")

    def audit_plans(self):
        """
        Consolidated Audit:
        1. Generates Evolution Matrix (History of accounts across years).
        2. Checks Structural Integrity (Conflicts in Parent, Nature, Type).
        """
        print("\n--- Starting Consolidated Audit (Evolution + Integrity) ---")
        os.makedirs(self.analysis_dir, exist_ok=True)
        self.conflicts = []

        # Load Metadata directly (assuming it exists or standardized first)
        if not os.path.exists(self.filtered_meta_path):
            print("Filtered metadata not found. Running filter...")
            df_meta = self.filter_metadata()
        else:
            df_meta = cast(
                pd.DataFrame,
                pd.read_csv(
                    self.filtered_meta_path, sep=";", encoding="utf-8-sig", dtype=str
                ),
            )

        # Filter only for Plan 1 (Lucro Real) and 2 (Lucro Presumido)
        unique_refs = [
            r
            for r in cast(pd.Series, df_meta["COD_PLAN_REF"]).unique()
            if str(r).strip() in ["1", "2"]
        ]
        print(f"Processing restricted to COD_PLAN_REF types: {unique_refs}")

        for cod_ref in unique_refs:
            print(f"\nProcessing Plan: {cod_ref}")
            df_plan = df_meta[df_meta["COD_PLAN_REF"] == cod_ref].copy()

            # Prepare containers
            yearly_data: Dict[str, pd.DataFrame] = {}
            knowledge_base: Dict[str, Dict[str, Any]] = {}

            # Sort groups by year
            grouped = df_plan.groupby("Ano")
            group_keys = cast(Dict[Any, Any], grouped.groups).keys()
            years_list: List[tuple[int, Any]] = []
            for y_key in group_keys:
                years_list.append((self.parse_year_safe(y_key), y_key))

            years_list.sort(key=lambda x: x[0])

            for y_val, year_key in years_list:
                # Fix 1: Permite rastreio e auditoria oficial de arquivos legados estruturais (< 2014) removendo o 'continue'
                ano_str = str(year_key)
                # Reconstrói o nome do arquivo unificado conforme lógica do standardize_plans
                unified_filename = f"REF_{cod_ref}_{ano_str}.csv".replace(
                    ">=", "GE"
                ).replace("<", "LT")
                file_path = os.path.join(self.schemas_dir, unified_filename)

                if not os.path.exists(file_path):
                    continue

                print(f"  Lendo plano para auditoria: {unified_filename}")
                try:
                    # Read CSV - Use pipe separator as generated by standardize_plans
                    df_full_year = cast(
                        pd.DataFrame,
                        pd.read_csv(file_path, sep="|", dtype=str, encoding="utf-8"),
                    )

                    cols_to_keep = [
                        "CODIGO",
                        "DESCRICAO",
                        "NATUREZA",
                        "NIVEL",
                        "COD_SUP",
                        "TIPO",
                    ]
                    # Keep available columns
                    existing_cols = [
                        c for c in cols_to_keep if c in df_full_year.columns
                    ]
                    df_full_year = cast(pd.DataFrame, df_full_year[existing_cols])
                    df_full_year.drop_duplicates(subset=["CODIGO"], inplace=True)

                    # 1. Integrity Check (On-the-fly)
                    self._check_integrity_row(
                        cod_ref, year_key, df_full_year, knowledge_base
                    )

                    # 2. Store for Evolution Report
                    yearly_data[str(year_key)] = df_full_year.set_index("CODIGO")

                except Exception as e:
                    print(f"  Error reading {file_path}: {e}")

            # After processing all years for this Plan Ref, generate the Evolution Report
            self._generate_evolution_report(cod_ref, yearly_data)

        # After processing all Plans, save the global Conflict Report
        self._save_conflict_report()

    def _check_integrity_row(
        self,
        cod_ref: str,
        year: str,
        df: pd.DataFrame,
        knowledge_base: Dict[str, Dict[str, Any]],
    ):
        """Compares current year's accounts against the knowledge base usando fast iteration."""
        for row in df.itertuples(index=False):
            code_val = getattr(row, "CODIGO", "")
            code = str(code_val).strip() if pd.notna(code_val) else ""
            if not code:
                continue

            # Normalize fields
            sup_val = getattr(row, "COD_SUP", "")
            cod_sup = str(sup_val).strip() if pd.notna(sup_val) else ""

            nat_val = getattr(row, "NATUREZA", "")
            natureza = str(nat_val).strip() if pd.notna(nat_val) else ""

            tipo_val = getattr(row, "TIPO", "")
            tipo = str(tipo_val).strip() if pd.notna(tipo_val) else ""

            if code in knowledge_base:
                prev = knowledge_base[code]

                # Check Parent
                if prev["COD_SUP"] != cod_sup:
                    self.conflicts.append(
                        {
                            "COD_PLAN_REF": cod_ref,
                            "CODIGO": code,
                            "TIPO_CONFLITO": "MUDANCA_PAI",
                            "VALOR_ANTIGO": prev["COD_SUP"],
                            "ANO_ANTIGO": prev["LAST_SEEN_YEAR"],
                            "VALOR_NOVO": cod_sup,
                            "ANO_NOVO": year,
                        }
                    )

                # Check Nature
                if prev["NATUREZA"] != natureza:
                    self.conflicts.append(
                        {
                            "COD_PLAN_REF": cod_ref,
                            "CODIGO": code,
                            "TIPO_CONFLITO": "MUDANCA_NATUREZA",
                            "VALOR_ANTIGO": prev["NATUREZA"],
                            "ANO_ANTIGO": prev["LAST_SEEN_YEAR"],
                            "VALOR_NOVO": natureza,
                            "ANO_NOVO": year,
                        }
                    )

                # Check Type
                if prev["TIPO"] != tipo:
                    self.conflicts.append(
                        {
                            "COD_PLAN_REF": cod_ref,
                            "CODIGO": code,
                            "TIPO_CONFLITO": "MUDANCA_TIPO",
                            "VALOR_ANTIGO": prev["TIPO"],
                            "ANO_ANTIGO": prev["LAST_SEEN_YEAR"],
                            "VALOR_NOVO": tipo,
                            "ANO_NOVO": year,
                        }
                    )

                # Update KB
                knowledge_base[code]["COD_SUP"] = cod_sup
                knowledge_base[code]["NATUREZA"] = natureza
                knowledge_base[code]["TIPO"] = tipo
                knowledge_base[code]["LAST_SEEN_YEAR"] = year
            else:
                # New Entry
                knowledge_base[code] = {
                    "COD_SUP": cod_sup,
                    "NATUREZA": natureza,
                    "TIPO": tipo,
                    "FIRST_SEEN_YEAR": year,
                    "LAST_SEEN_YEAR": year,
                }

    def _generate_evolution_report(
        self, cod_ref: str, yearly_data: Dict[str, pd.DataFrame]
    ):
        """Generates the wide-format CSV matrix for account evolution using Vectorized Ops."""
        if not yearly_data:
            print("  No data found for evolution report.")
            return

        years_sorted = sorted(yearly_data.keys(), key=self.parse_year_safe)

        # 1. Empilhar todos os anos para extrair o index global e dados canônicos
        all_dfs = []
        for y in years_sorted:
            df_y = yearly_data[y].copy()
            df_y["_ANO_REF"] = self.parse_year_safe(y)
            all_dfs.append(df_y)

        df_concat = pd.concat(all_dfs).reset_index()

        print(
            f"  Generating evolution report for {df_concat['CODIGO'].nunique()} accounts..."
        )

        # 2. Informações canônicas: Pega a versão mais recente e os metadados dela
        df_canonical = df_concat.sort_values("_ANO_REF").drop_duplicates(
            subset=["CODIGO"], keep="last"
        )

        # Mantém apenas as colunas base de comparação
        base_cols_all = ["CODIGO", "DESCRICAO", "TIPO", "COD_SUP", "NIVEL", "NATUREZA"]
        existing_base = [c for c in base_cols_all if c in df_canonical.columns]
        df_comparison = cast(pd.DataFrame, df_canonical[existing_base].copy())

        # 3. Adiciona as colunas anuais mapeando em escala vetorial (O(N) limpo)
        for year in years_sorted:
            s_desc = cast(pd.Series, yearly_data[year]["DESCRICAO"])
            # Mapeia valores e trata missing como NaN
            df_comparison[f"ANO_{year}"] = cast(pd.Series, df_comparison["CODIGO"]).map(
                s_desc
            )

        # Enforce Column Order requested by User
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

        # Select only existing columns to be safe
        existing_cols = [c for c in final_cols if c in df_comparison.columns]
        df_comparison = cast(pd.DataFrame, df_comparison[existing_cols])

        safe_cod = str(cod_ref).strip()
        output_path = os.path.join(
            self.analysis_dir, f"ref_plan_evolution_{safe_cod}.csv"
        )

        # Using pipe separator as requested
        df_comparison.to_csv(output_path, sep="|", index=False, encoding="utf-8-sig")
        print(f"  Evolution Report saved: {output_path}")

    def _save_conflict_report(self):
        """Saves the cumulative conflict list to CSV."""
        if not self.conflicts:
            print("\nSUCCESS: No structural conflicts found!")
        else:
            print(f"\nWARNING: Found {len(self.conflicts)} structural conflicts.")
            df_conflicts = pd.DataFrame(self.conflicts)
            # Using pipe separator for consistency
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
