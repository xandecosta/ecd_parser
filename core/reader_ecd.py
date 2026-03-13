import json
import logging
import os
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Generator, Dict, Any, Optional, Tuple
from core.telemetry import monitor_task, TelemetryCollector

# Configuração de Logs (Configurado via main.py ou __main__)


class ECDReader:
    def __init__(self, caminho_arquivo: str):
        self.caminho_arquivo = caminho_arquivo
        self.layout_versao: Optional[str] = None
        self.schema: Optional[Dict[str, Any]] = None
        self.periodo_ecd: Optional[str] = None  # YYYYMMDD do registro 0000
        self.cnpj: str = ""
        # Path dinâmico robusto (Pythonic)
        self.schemas_dir = os.path.normpath(
            os.path.join(os.path.dirname(__file__), "..", "schemas", "ecd_layouts")
        )
        self.telemetry: Optional[TelemetryCollector] = None
        self.current_ecd_id = ""

    @property
    def ano_vigencia(self) -> Optional[int]:
        """Extrai o ano do periodo_ecd (YYYYMMDD)."""
        if self.periodo_ecd and len(self.periodo_ecd) >= 4:
            try:
                return int(self.periodo_ecd[:4])
            except ValueError:
                return None
        return None

    @monitor_task("ECDReader", "_detectar_layout")
    def _detectar_layout(self) -> None:
        """
        Detecta a versão do layout lendo o registro I010.
        Carrega o schema JSON correspondente.
        """
        logging.info(f"Detectando layout do arquivo: {self.caminho_arquivo}")

        if not os.path.exists(self.caminho_arquivo):
            raise FileNotFoundError(f"Arquivo não encontrado: {self.caminho_arquivo}")

        with open(self.caminho_arquivo, "r", encoding="latin-1", errors="replace") as f:
            for linha in f:
                if linha.startswith("|I010|"):
                    partes = linha.split("|")
                    # Ex: |I010|G|9.00| -> ['', 'I010', 'G', '9.00', '...']
                    if len(partes) > 3:
                        self.layout_versao = partes[3]
                        logging.info(f"Layout detectado: {self.layout_versao}")
                        self._carregar_schema()
                        return

        if not self.layout_versao:
            raise ValueError(
                "Não foi possível detectar a versão do layout (Registro I010 não encontrado)."
            )

    def _carregar_schema(self) -> None:
        """Carrega o arquivo JSON do schema correspondente à versão detectada."""
        nome_arquivo = f"layout_{self.layout_versao}.json"
        caminho_schema = os.path.join(self.schemas_dir, nome_arquivo)

        logging.info(f"Carregando schema: {caminho_schema}")

        if not os.path.exists(caminho_schema):
            raise ValueError(
                f"Schema não encontrado para a versão {self.layout_versao}: {caminho_schema}"
            )

        with open(caminho_schema, "r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def _converter_valor(
        self, valor: str, tipo: str, decimal: int, nome_campo: str
    ) -> Any:
        """Converte o valor string para o tipo apropriado com regras de prioridade."""
        if valor is None:
            return None

        # O valor vem do split, então pode ser string vazia.
        if valor == "":
            return None

        # --- Prioridade 1: Numéricos Reais (Alta Frequência) ---
        # Verifica tipo antes de nome para performance
        if tipo == "N" and decimal > 0:
            try:
                # Substitui vírgula por ponto para o Decimal
                return Decimal(valor.replace(",", "."))
            except (InvalidOperation, ValueError):
                return None

        # --- Prioridade 2: Datas ---
        # Checa 'D' no tipo (se schema suportar) ou heurística de nome
        if tipo == "D" or "DT_" in nome_campo or "DATA" in nome_campo:
            # Sanitização: Remove caracteres não numéricos se houver
            # Garante que tenha 8 digitos com zeros a esquerda (ex: 1012020 -> 01012020)
            valor_data = valor.strip().zfill(8)

            if len(valor_data) == 8 and valor_data.isdigit():
                try:
                    return datetime.strptime(valor_data, "%d%m%Y").date()
                except ValueError:
                    # Data inválida silenciosa para não quebrar fluxo, retorna string original
                    return valor  # Retorna string original se falhar parser
            else:
                return valor

        # --- Prioridade 3: Numéricos Inteiros ou Identificadores (Decimal == 0) ---
        # Se for tipo 'N' mas sem decimais (ex: código, CNPJ),
        # mantemos como STRING para preservar zeros à esquerda (ex: '0015').

        return valor

    def _iterar_linhas_seguras(self) -> Generator[Tuple[int, str], None, None]:
        """
        Generator robusto que blinda o parser contra falhas de IO e Encoding.
        Sanitização Pré-Parser (Roadmap Item 5).
        """
        if not os.path.exists(self.caminho_arquivo):
            raise FileNotFoundError(f"Arquivo não encontrado: {self.caminho_arquivo}")

        try:
            # SPED é ISO-8859-1 (Latin-1). 'replace' evita crash por byte inválido.
            with open(self.caminho_arquivo, "r", encoding="latin-1", errors="replace") as f:
                for i, linha in enumerate(f, 1):
                    linha_limpa = linha.strip()
                    # Ignora linhas vazias ou sem pipe inicial (estrutura básica quebrada)
                    if not linha_limpa or not linha_limpa.startswith("|"):
                        continue
                    yield i, linha_limpa
        except Exception as e:
            logging.error(f"Erro crítico de IO na leitura do arquivo: {e}")
            raise

    @monitor_task("ECDReader", "processar_arquivo")
    def processar_arquivo(
        self, blocos_selecionados: Optional[list] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Lê o arquivo linha a linha, gera PK/FK e converte dados.
        Args:
            blocos_selecionados: Lista opcional de prefixos de blocos (ex: ['0', 'I']) 
                                para leitura parcial.
        """
        if not self.schema:
            self._detectar_layout()

        logging.info(f"Iniciando processamento do arquivo: {self.caminho_arquivo}")
        
        # Se filtrado, converter para tupla para startswith eficiente
        filtro_blocos = tuple(blocos_selecionados) if blocos_selecionados else None

        # Contexto de Pais: {nivel: pk_do_registro}
        contexto_pais: Dict[int, str] = {}
        
        # Controle de Logs para evitar flooding
        warnings_count = 0
        MAX_LOGS_WARNING = 50

        for numero_linha, linha in self._iterar_linhas_seguras():
            try:
                partes = linha.split("|")
                if len(partes) < 2:
                    continue

                registro = partes[1]

                # --- FILTRAGEM OURO ---
                # Se houver filtro, pula se o registro não começar com o prefixo desejado
                if filtro_blocos and not registro.startswith(filtro_blocos):
                    continue

                # Ignorar Bloco C (Geralmente desnecessário no ECD Contábil padrão)
                if registro.startswith("C"):
                    continue

                if not self.schema or registro not in self.schema:
                    continue

                # Obter definição do schema
                def_registro = self.schema[registro]
                nivel = def_registro.get("nivel", 0)
                campos_layout = def_registro.get("campos", [])

                # Validação de robustez: Número de pipes esperado
                # O arquivo SPED começa com | e termina com |
                # Ex: |0000|LECD|...| gera ['', '0000', 'LECD', ..., '']
                # len(partes) deve ser len(campos_layout) + 2 (pelo pipe inicial e o registro)
                # Mais 1 se houver o pipe final (comum no SPED).
                esperado_base = len(campos_layout) + 2
                if len(partes) < esperado_base:
                    if warnings_count < MAX_LOGS_WARNING:
                        logging.warning(
                            f"Linha {numero_linha} ({registro}): Menos campos que o esperado. "
                            f"Esperado >= {esperado_base}, Obtido {len(partes)}"
                        )
                        warnings_count += 1
                elif len(partes) > esperado_base + 1:
                    if warnings_count < MAX_LOGS_WARNING:
                        logging.warning(
                            f"Linha {numero_linha} ({registro}): Mais campos que o esperado "
                            f"(possível pipe extra). Obtido {len(partes)}"
                        )
                        warnings_count += 1

                dados_registro = {"REG": registro, "LINHA_ORIGEM": numero_linha}

                # --- Extração de Campos ---
                for i, def_campo in enumerate(campos_layout):
                    # Indexação corrigida:
                    # partes[0]='', partes[1]=REG, partes[2]=Primeiro Campo...
                    # Como o layout inclui o campo REG, campos_layout[0] mapeia para partes[1].
                    idx_real = i + 1
                    valor_bruto = partes[idx_real] if idx_real < len(partes) else ""

                    valor_final = self._converter_valor(
                        valor_bruto,
                        def_campo["tipo"],
                        def_campo["decimal"],
                        def_campo["nome"],
                    )
                    dados_registro[def_campo["nome"]] = valor_final

                # --- Captura de Período (Registro 0000) ---
                if registro == "0000":
                    dt_fin = dados_registro.get("0000_DT_FIN")

                    # Garantir que temos uma string YYYYMMDD para a PK
                    if isinstance(dt_fin, date):
                        self.periodo_ecd = dt_fin.strftime("%Y%m%d")
                    elif (
                        isinstance(dt_fin, str)
                        and len(dt_fin) == 8
                        and dt_fin.isdigit()
                    ):
                        # Converte DDMMYYYY para YYYYMMDD se necessário (o reader_ecd costuma converter para date, mas o fallback é bom)
                        self.periodo_ecd = f"{dt_fin[4:]}{dt_fin[2:4]}{dt_fin[:2]}"
                    else:
                        # Fallback crítico: se não houver data válida
                        self.periodo_ecd = "00000000"

                    # Captura CNPJ e Código do Plano Referencial (Tenta com e sem prefixo)
                    self.cnpj = str(
                        dados_registro.get("0000_CNPJ")
                        or dados_registro.get("CNPJ")
                        or ""
                    )

                # --- Geração de PK ---
                pk_prefix = self.periodo_ecd if self.periodo_ecd else "00000000"
                pk_atual = f"{pk_prefix}_{numero_linha:08d}"
                dados_registro["PK"] = pk_atual

                # --- Geração de FK (Pai) ---
                if nivel > 0:
                    fk_pai = contexto_pais.get(nivel - 1)
                    dados_registro["FK_PAI"] = fk_pai
                else:
                    fk_pai = None
                    dados_registro["FK_PAI"] = None

                # Atualizar o contexto de pais para o nível atual
                contexto_pais[nivel] = pk_atual

                yield dados_registro

            except Exception as e_linha:
                # Garante que uma linha corrompida não aborte o arquivo inteiro
                if warnings_count < MAX_LOGS_WARNING:
                    logging.error(f"Erro fatal processando linha {numero_linha}: {e_linha}")
                    warnings_count += 1


if __name__ == "__main__":
    # Configuração de log para execução direta do módulo
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    # Exemplo simples de uso do reader
    import glob

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir = os.path.join(base_dir, "data", "input")
    arquivos = glob.glob(os.path.join(input_dir, "*.txt"))

    if arquivos:
        arquivo = arquivos[0]
        print(f"--- Demonstração ECDReader: {os.path.basename(arquivo)} ---")
        reader = ECDReader(arquivo)
        for i, registro in enumerate(reader.processar_arquivo()):
            print(
                f"Linha {registro['LINHA_ORIGEM']}: {registro['REG']} | PK: {registro['PK']}"
            )
            if i >= 4:
                break
    else:
        print("Nenhum arquivo de entrada encontrado em data/input/")
