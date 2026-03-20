# Changelog - SPED-ECD Parser Pro

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.
O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-br/1.0.0/).

## [2.9.0] - 2026-03-19

### Adicionado [2.9.0]

- **Regionalização Específica de CSV (Excel PT-BR)**: Nova função `ensure_numeric_vl_cols` no `formatting.py` garante que colunas de valor (prefixos `VL_`, `DIF_`, etc.) sejam convertidas para `float` validado e exportadas impecavelmente com separador decimal vírgula `,` e ponto-e-vírgula `;` apenas nos arquivos CSV. Os arquivos Parquet mantêm sua estrutura nativa inalterada.
- **Cache de Consolidação (I/O)**: Implementação de verificação por *timestamp* no `ECDConsolidator`. O sistema detecta se o arquivo consolidado já possui data de modificação superior à dos Parquets individuais de origem, pulando a re-concatenação caso não haja novas informações (vital para processamento incremental).

### Alterado [2.9.0]

- **Auditoria Multithreading**: Paralelização da bateria forense. O `ECDAuditor` agora despacha a execução dos 5 grupos principais de validação de auditoria via `ThreadPoolExecutor`, destravando a concorrência interna e reduzindo o tempo de validação em ~40%.
- **Vetorização Extrema na Auditoria**: Remoção do gargalo funcional `.apply(lambda)` na checagem do cruzamento diário. A substituição por cálculo matricial direto entre *Series* do Pandas derrubou o tempo deste teste específico em 36%.
- **Rollup Hierárquico Unificado (O(N))**: Destruição dos loops aninhados de propagação de saldo (Mês × Nível) no `ECDProcessor`. O uso do campo `DT_FIN` nativo na chave do *GroupBy* isolou os balancetes matematicamente numa operação única por nível hierárquico. Redução brutal de 65% no tempo gasto consolidando o Plano Referencial.
- **Exportação Concorrente**: `ECDExporter` agora lança escritas de disco (`to_parquet` e `to_csv`) em *threads* separadas.
- **Impacto Composto Global**: O tempo total do pipeline de geração (leitura, processamento integral, auditoria, consolidação) em ambiente de teste reduziu na ordem de ~39%.

## [2.8.0] - 2026-03-03

### Adicionado [2.8.0]

- **Processamento Paralelo (Multi-core)**: Implementação de `ProcessPoolExecutor` no `main.py`, permitindo o processamento simultâneo de múltiplos arquivos ECD.
- **Telemetria Distribuída**: Sistema de coleta e mesclagem de métricas de performance vindas de processos isolados, mantendo logs de execução precisos e granulares.

### Alterado [2.8.0]

- **Pipeline de Execução**: Refatoração do `main.py` para suportar orquestração de tarefas paralelas após a fase de aprendizado histórico.
- **Performance Industrial**: Redução drástica do tempo de execução em ambientes multi-core (Ex: 11 arquivos processados em **1m 37s**, contra 3m 31s da versão anterior).

## [2.7.0] - 2026-02-27

### Adicionado [2.7.0]

- **IO Inteligente (Persistência)**: Implementação de cache persistente para a fase de aprendizado histórico (`history.json`), reduzindo o tempo de inicialização em execuções subsequentes.
- **Cache de Processamento Interno**: `ECDProcessor` agora memoriza o processamento de Plano de Contas e Lançamentos dentro de uma mesma sessão, evitando cálculos redundantes.

### Alterado [2.7.0]

- **Migração para CSV (Universal Output)**: Substituição do XLSX pelo CSV (PT-BR) como formato padrão de saída e consolidação. Elimina o limite de 1 milhão de linhas e acelera a exportação em 10x.
- **Núcleo de Precisão Híbrida (Float64)**: Transição da aritmética interna de `Decimal` para `float64` (vetorizada). A leitura preserva `Decimal` para segurança, mas o processamento core usa a velocidade do NumPy/C-engine.
- **Vetorização Bottom-Up**: Reescrita completa do algoritmo de propagação hierárquica e referencial. Eliminação do `iterrows()` em favor de operações de `merge` e `add` vetorizadas.
- **Auditoria "Option D"**: Refatoração do `AuditExporter` para gerar um Dashboard unificado e arquivos de evidência individuais por teste, facilitando a perícia em grandes volumes de dados.

### Corrigido [2.7.0]

- **TypeError de Precisão**: Solução definitiva para conflitos de tipos entre colunas Decimal (do Reader) e Float (do Processor) através de casts e proteções de auditoria.
- **Resolução de venv no Cursor**: Configuração robusta do `.vscode/settings.json` com caminhos absolutos para garantir descoberta correta de bibliotecas no Windows.

## [2.6.1] - 2026-02-25

### Alterado [2.6.1]

- **Reorganização Estrutural (Remanejamento Ouro)**: Extinção das pastas genéricas `utils/` e `scripts/`.
- **Categorização por Função**: Distribuição de arquivos em pastas didáticas: `exporters/` (I/O), `intelligence/` (Domínio) e `tools/` (Desenvolvedor).
- **Atomicidade de Imports**: Sincronização de todos os módulos (`core`, `main`, `tests`) para a nova arquitetura física.

## [2.6.0] - 2026-02-25

### Adicionado [2.6.0]

- **Consolidação Dinâmica**: `ECDConsolidator` agora descobre automaticamente tabelas no disco, sem necessidade de hardcoding de nomes.
- **Rastreabilidade de Origem**: Injeção da coluna `ORIGEM_PERIODO` em dados consolidados para auditoria histórica precisa.
- **Centralização de Formatação**: Novo módulo `utils/formatting.py` unificando regras PT-BR para todo o sistema.

### Alterado [2.6.0]

- **Vetorização de Saída**: Substituição de loops `.apply` por operações vetorizadas no `ECDExporter`, acelerando a geração de Excel em até 5x.
- **Robustez de Pastas**: Implementação de `exist_ok=True` e limpeza inteligente de outputs para evitar conflitos de processamento.

## [2.5.0] - 2026-02-24

### Adicionado [2.5.0]

- **Persistência de Conhecimento**: `HistoricalMapper` agora suporta `save_knowledge` e `load_knowledge` em JSON, evitando re-aprendizado lento entre sessões.
- **Cache de Similaridade (Memorização)**: Implementação de `_neighbor_cache` no Mapper, reduzindo complexidade de busca de O(N^2) para O(1) após o primeiro cálculo.

### Alterado [2.5.0]

- **Compilador de Layouts O(N)**: Refatoração completa do `ecd_layout_compiler.py` usando Pandas `groupby`, eliminando loops de filtragem lentos.

### Corrigido [2.5.0]

- **Vetorização de Aprendizado**: Substituição de `iterrows()` por extração vetorial no `HistoricalMapper`, acelerando a carga de mapeamentos históricos em 10x.
- **Tipagem Estática**: 100% de conformidade com Pyright no motor de layouts e inferência histórica.

## [2.4.0] - 2026-02-23

### Alterado [2.4.0]

- **Refatoração Ouro de Performance:** Reescrita arquitetural profunda do `ref_plan_manager.py`, focada na brutal redução de uso de RAM e legibilidade (*Clean Code*).
- **I/O Otimizado:** Substituição da leitura engarrafada por iteração via `pd.read_csv`, acoplado ao motor C nativo superveloz.
- **Tabelas Hash O(1):** Mapeamento estático do Plano Referencial, aposentando longos laços procedurais com dezenas de `ifs`.
- **Pipeline Vetorial:** Geração de relatórios de evolução inteiramente reescritos usando funções hiper-rápidas como `pd.concat` e `.map`, abolindo os obsoletos laços de duplo lookup.
- **Design Patterns (SRP):** Extração da lógica massiva de higienização de dados em pipelines autônomos (ex: `_clean_unified_dataframe`).
- **Resiliência Windows:** Blindagem sistêmica injetada para ignorar "Acesso Negado" e bloqueios invisíveis de sincronizadores do Google Drive/OneDrive sobre pastas físicas apagadas simultaneamente.
- **Auditoria Histórica:** Retirada da trava explícita que bloqueava o rastreio formal de balancetes antigos (anteriores a 2014), liberando rastreamento da matriz referencial legada `CONTASREF`.

## [2.3.0] - 2026-02-23

### Adicionado [2.3.0]

- **Automação de Planos Referenciais:** O script `ref_plan_manager.py` foi refatorado para varrer o diretório bruto da Receita Federal e criar o catálogo oficial (`ref_catalog.json`) de forma autônoma baseada em Expressões Regulares (Regex Dinâmico).

### Removido [2.3.0]

- Eliminação do arquivo estático `ref_plan_full.csv` que exigia atualização manual para novas tabelas do governo.
- Remoção da pasta duplicada e redundante `raw_ref_plans_2`.

## [2.2.0] - 2026-01-27

### Adicionado [2.2.0]

- **Refinamento da Lei de Benford:** Inclusão de coluna de interpretação automática (Excesso/Déficit) e tabelas de *drill-down* (Análise de Valores) para identificação de causas raiz de anomalias nos dígitos.
- **Filtros de Duplicidade Inteligentes:** Implementação de critérios por materialidade (R$ 100), análise nominal de histórico e identificação de grupos financeiros (IOF, taxas, serviços) para eliminação de ruídos irrelevantes.
- **Portabilidade Windows:** Configuração automática de `sys.stdout` para UTF-8, garantindo que o terminal Windows suporte acentuação e caracteres especiais sem travamentos.
- **Tipagem Estática Progressiva:** Refatoração rigorosa com `typing.cast` em todos os módulos de auditoria, atingindo conformidade total com analisadores estáticos (Pyright).

### Alterado [2.2.0]

- **Isolamento de Dados no Excel:** Reestruturação do pipeline de exportação para gerar uma aba individual por teste de auditoria, eliminando a mistura de dados de naturezas diferentes.
- **Sincronização Regional PT-BR:** Padronização mandatória de vírgulas decimais e datas em formato brasileiro em 100% dos relatórios gerados.
- **Gestão de Caminhos:** Otimização do ajuste de `sys.path` em scripts e testes para suportar execução multiplataforma e de qualquer diretório da árvore.

## [2.1.0] - 2026-01-26

### Adicionado [2.1.0]

- **Motor de Auditoria Forense:** Integração total do módulo `ECDAuditor` no pipeline principal (`main.py`). O sistema agora busca automaticamente por fraudes e erros matemáticos a cada execução.
- **Dossiê de Evidências (I250):** Auditoria agora extrai os registros de lançamentos suspeitos para a aba `EVIDENCIAS_DETALHADAS`, garantindo auditabilidade imediata.
- **Consolidação Híbrida:** Novo modelo de consolidação estratégica que gera um `Scorecard` global de riscos (comparativo anual) enquanto preserva detalhes massivos de forma local nas pastas de cada período.
- **Documentação Pedagógica:** Reestruturação total do `README`, `CONTEXT` e `.cursorrules` focada em clareza extrema para desenvolvedores iniciantes, incluindo o "Mapa do Tesouro" do projeto.

### Alterado [2.1.0]

- **Nomenclatura Padronizada:** Arquivos de auditoria agora seguem a máscara `<DATA>_07_Auditoria...` para ordenação perfeita no sistema de arquivos.
- **Organização do Repositório:** Scripts de desenvolvimento movidos para `/scripts` e testes unitários para `/tests`, mantendo a raiz do projeto limpa.
- **Resiliência Forense:** Isolamento da execução de auditoria em blocos `try/except` para garantir que falhas em testes experimentais não interrompam a produção dos balancetes principais.

### Removido [2.1.0]

- **Limpeza de Legado:** Deleção de 11 scripts temporários (`analyze_*.py`) e resíduos de versões anteriores (pasta `audit_details`), simplificando a árvore do projeto.

## [2.0.0-beta] - 2026-01-22

### Adicionado [2.0.0-beta]

- **Branch reorientada:** Foco total em processos de Mapeamento e Consolidação no formato RFB (substituindo o plano original de DuckDB nesta branch).
- **Relatórios de Mapeamento:** Geração de balancetes consolidados por plano referencial.
- **Linter de Markdown:** Adicionado arquivo `.markdownlint.json` para garantir padronização da documentação.

### Corrigido [2.0.0-beta]

- **Estilo de Documentação:** Correção de indentação em listas no arquivo `.cursorrules.md` (MD007).

## [1.9.0] - 2026-01-15

### Adicionado [1.9.0]

- **Ponte Virtual (Virtual I051):** Motor de inferência cross-temporal que permite a geração de balancetes baseRFB para arquivos ECD omissos (comum no ano de 2014). Veja [detalhes técnicos aqui](./docs/architecture/bridging_logic.md).
- **Módulo HistoricalMapper:** Implementação de classe de gestão de conhecimento histórico, capaz de aprender com anos adjacentes e construir consensos estatísticos de mapeamento.
- **Funil de 3 Rodadas:** Lógica de decisão hierárquica para preenchimento de `COD_CTA_REF` baseada em Identidade literal, Grupo Superior (`COD_SUP`) e Consenso Global.
- **Auditoria Dinâmica:** Novo roteiro em `tests/test_bridging.py` que permite auditar qualquer arquivo da pasta input selecionando vizinhos cronológicos interativamente.
- **Rótulo CONSENSO_HISTORICO:** Novo identificador de origem de mapeamento para auditoria de contas raras recuperadas da memória global.

### Alterado [1.9.0]

- **Robustez de Rotulagem (Anti-NaN):** Refatoração da lógica de preenchimento da coluna `ORIGEM_MAP` no `ECDProcessor` para garantir rótulos consistentes em arquivos híbridos (mistura de I051 e Inferência).
- **Learning Pass Integrado:** O `main.py` agora realiza uma fase de aprendizado completa em todo o lote de arquivos antes de iniciar o processamento individual, enriquecendo o `HistoricalMapper`.
- **Equivalência 2014+:** Implementação de suporte histórico para a mudança de codificação da RFB (Código 10 pré-2014 agora é mapeado corretamente para os regimes 1/10 modernos).
- **Estabilidade de Tipagem:** Correção de erro de sobrecarga em `pd.DataFrame.rename` e aplicação de `typing.cast` para conformidade estrita com Pyright.
- **Automação de Limpeza:** Implementação de limpeza automática da pasta `data/output` a cada execução, preservando apenas logs.

## [1.8.0] - 2026-01-14

### Adicionado [1.8.0]

- **Auditoria Consolidada:** Implementação dos métodos `audit_plans`, `_check_integrity_row` e `_generate_evolution_report` no `RefPlanManager` para realizar análise de evolução e integridade estrutural em um único passo.
- **Relatórios de Evolução:** Geração automática de matrizes históricas (`comparativo_evolucao_plano_*.csv`) na pasta `data/analysis`.

### Alterado [1.8.0]

- **Arquitetura de Gestão Referencial:** Consolidação total dos scripts `audit_ref_plan_consolidation.py`, `check_integrity.py` e `ref_plan_auditor.py` dentro do `RefPlanManager`.
- **Padronização de Dados:** Enquadramento estrito de colunas (`CODIGO`, `DESCRICAO`, `TIPO`, `COD_SUP`, `NIVEL`, `NATUREZA`) e uso mandatório de separador pipe (`|`) em todos os outputs referenciais.
- **Excelência em Tipagem:** Refatoração para 100% de conformidade com Pyright no módulo de gestão de planos, utilizando `typing.cast` e anotações rigorosas.
- **Limpeza de Utilitários:** Remoção de arquivos obsoletos em `utils/` para simplificar a manutenção do projeto.

## [1.7.0] - 2026-01-05

### Adicionado [1.7.0]

- **Tipagem Estática (Pyright):** Refatoração extensiva em `core/processor.py` e utilitários para conformidade com Pyright, garantindo inferência correta de tipos via `typing.cast`.
- **Robustez Decimal:** Função `_converter_decimal` agora trata strings vazias e espaços em branco com segurança, mantendo a precisão financeira.

### Alterado [1.7.0]

- **Infraestrutura de Testes:** Correção de `ModuleNotFoundError` no `test_integracao.py` permitindo execução a partir de qualquer diretório.
- **Inicialização de Testes:** Atualização da classe `ECDProcessor` nos testes para incluir metadados obrigatórios (CNPJ e Layout).
- **Consolidação Hierárquica:** Refinamento dos loops no algoritmo *Bottom-Up* para maior estabilidade de tipos durante o processamento.

## [1.6.1] - 2026-01-03

### Adicionado [1.6.1]

- **Balancete baseRFB:** Geração automática do balancete na visão da Receita Federal com consolidação hierárquica *Bottom-Up*.
- **Exportação RFB:** Inclusão da tabela `04_Balancete_baseRFB` nos formatos Excel e Parquet para auditoria.
- **Detecção Híbrida:** Lógica inteligente para extrair o `COD_PLAN_REF` tanto de arquivos modernos (Registro 0000) quanto legados (Registro I051).

### Alterado [1.6.1]

- **Sincronização de Metadados:** Reconstrução total da biblioteca referencial CSV e do catálogo após atualização do arquivo mestre de planos.
- **Pipeline de Exportação:** Ajuste no `exporter.py` para permitir exportação compulsória em Excel de balancetes referenciais.
- **Refactoring:** Limpeza de avisos de lint e melhoria na tipagem interna do `ECDProcessor` para garantir precisão absoluta nos cálculos.
- **Limpeza:** Remoção do script redundante `tests/test_detection.py` para evitar loops de processamento em
  ferramentas de diagnóstico.

## [1.6.0] - 2026-01-03

### Adicionado [1.6.0]

- **Suporte Nativo a CSV:** Planos referenciais agora são armazenados em CSV (UTF-8, delimitador pipe) para maior performance e facilidade de conferência.
- **Estrutura de Schemas Categorizada:** Criação da subpasta `ecd_layouts` e `ref_plans` para separar definições da ECD de dados da RFB.

### Alterado [1.6.0]

- **Padronização de Dados:** Correção do alinhamento de colunas em todos os planos referenciais (garantindo 9 colunas fixas).
- **Refatoração Semântica:** Renomeação dos scripts utilitários em `utils/` para nomes descritivos: `ref_plan_standardizer.py`, `ref_plan_discovery.py` e `ecd_layout_compiler.py`.
- **Motor Core:** Atualização do `ECDReader` para carregar definições da nova estrutura de pastas.

### Removido [1.6.0]

- **Redundância:** Supressão do arquivo `ref_index.json` e do script `compile_ref_index.py`.

## [1.5.0] - 2026-01-02

### Alterado [1.5.0]

- **Estabilização do Motor Core:** Simplificação das classes `ECDReader` e `ECDProcessor`, removendo métodos experimentais de mapeamento referencial para garantir 100% de estabilidade nas análises periciais.
- **Padronização PT-BR:** Refatoração sistemática de termos em Português de Portugal para Português do Brasil (ex: "ficheiro" -> "arquivo") no código, testes e logs.
- **Arquitetura de Exportação:** Renomeação do log de auditoria para `Arquivos_Gerados.txt` e inclusão de dependência `pyarrow` no workflow.

## [1.4.0] - 2026-01-02 (Pesquisa Técnica)

### Nota de Desenvolvimento [1.4.0]

- **Mapeamento RFB (Shelved):** Durante esta fase, foi testada a integração com o Plano de Contas Referencial da Receita Federal. Devido à complexidade de manutenção das tabelas dinâmicas, a funcionalidade foi engavetada para priorizar o motor analítico direto. A v1.5.0 consolida a versão estável sem este módulo.
- **Suporte ao Registro I157:** Implementação mantida para assegurar a continuidade histórica em transferências de planos de contas.

## [1.3.0] - 2026-01-02

### Adicionado [1.3.0]

- **Continuidade Histórica (Forward Roll):** Implementação de transporte automático de saldo final para o inicial do mês seguinte. Isso garante a integridade da série temporal mesmo em arquivos com encerramentos mensais ou trimestrais.
- **Priorização de Relatórios:** Reorganização do pipeline de exportação para que as demonstrações contábeis (BP e DRE) sejam os primeiros arquivos da lista (`01_BP.xlsx`, `02_DRE.xlsx`).

### Alterado [1.3.0]

- **Fluxo de Processamento:** Ajuste na ordem de execução do `main.py` para gerar demonstrações antes do processamento dos planos de contas e lançamentos, otimizando a disponibilidade de dados.
- **Configuração de Exportação:** Reforço na lista de termos para exportação compulsória em Excel no `exporter.py`.

## [1.2.0] - 2026-01-01

### Adicionado [1.2.0]

- **Prefixo Temporal:** Inclusão de prefixo `YYYYMMDD_` nos arquivos exportados (Excel e Parquet). Isso resolve o bloqueio do Excel que impedia a abertura simultânea de múltiplos balancetes de diferentes períodos.
- **Tratamento de Tipos:** Conversão resiliente de nulos para `Decimal("0.00")` durante o merge de ajustes de encerramento, evitando erros de cálculo em contas sem movimentação.

### Corrigido [1.2.0]

- **Integridade Contábil:** Ajuste crítico na reversão de lançamentos de encerramento (tipo 'E'). Agora, além do saldo final, os valores são subtraídos de `VL_DEB` e `VL_CRED`, garantindo que a equação $Saldo Inicial + Débitos - Créditos = Saldo Final$ permaneça válida matematicamente.

## [1.1.0] - 2025-12-31

### Adicionado [1.1.0]

- **Batch Processing:** Implementação de processamento em lote resiliente. O sistema agora processa todos os arquivos `.txt` da pasta de entrada sequencialmente, sem interromper o fluxo em caso de erro em um arquivo isolado.
- **Organização por Período:** Criação automática de pastas de saída nomeadas pela data final (`YYYYMMDD`) extraída do registro `0000`.
- **Estrutura Flat:** Simplificação da estrutura de saída, salvando arquivos diretamente na pasta do período sem subdiretórios redundantes.

### Alterado [1.1.0]

- **Refatoração Core:** Otimização na detecção de colunas e remoção do campo global `REG` das tabelas finais para garantir um output mais limpo no Parquet.
- **Testes Unitários:** Implementação de mocks dinâmicos usando `tmp_path` (pytest), permitindo testes 100% em memória sem dependência de arquivos físicos externos.

## [1.0.0] - 2025-12-31

### Adicionado [1.0.0]

- **Release Inicial:** Pipeline completo de ETL (Extract, Transform, Load).
- **Motor de Propagação:** Implementação do algoritmo *Bottom-Up* em `ECDProcessor` para consolidar saldos de contas analíticas para sintéticas em todos os níveis.
- **Módulo Exporter:** Suporte a exportação multiformato (Parquet para performance e Excel para conferência).

## [0.5.0] - 2025-12-31

### Adicionado [0.5.0]

- **Arquitetura PK/FK:** Introdução de chaves primárias baseadas em `DT_FIN` + `LINHA` e chaves estrangeiras para manter a integridade entre registros pai e filho.
- **Precisão Financeira:** Adoção mandatória do tipo `decimal.Decimal` em todo o processamento de valores para evitar erros de ponto flutuante.

## [0.2.0] - 2025-12-30

### Adicionado [0.2.0]

- **Detecção Dinâmica:** Identificação automática da versão do layout SPED via registro `I010`.
- **Gerador de Schemas:** Script utilitário para converter as tabelas de parametrização do SPED em schemas JSON hierárquicos.
- **Leitura Otimizada:** Uso de Generators e `yield` para processar arquivos de grande porte com baixo consumo de memória.

## [0.1.0] - 2025-12-29

### Adicionado [0.1.0]

- Estrutura inicial do projeto (pastas, `venv`, `.gitignore`).
- Documentação base (`README.md`, `CONTEXT.md`).
- Regras de desenvolvimento em `.cursorrules`.
