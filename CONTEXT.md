# Mapa de Bordo do Projeto: SPED-ECD Parser Pro

Este documento é o seu guia técnico principal. Ele explica a "anatomia" do projeto e como os componentes se comunicam.

---

## 1. O Fluxo de Dados (Caminho que a informação percorre)

```mermaid
graph LR
    A[Arquivo .txt] --> B(Reader: Leitura)
    B --> C(Processor: Inteligência)
    C --> D(Auditor: Pente Fino)
    D --> E(Exporter: Salvar)
    E --> F[CSV / Parquet]
```

1. **Reader**: Lê cada linha do arquivo TXT e identifica os campos (ex: CNPJ, Data, Valor).
2. **Processor**: faz as contas difíceis, junta tabelas e reconstrói o Balanço.
3. **Auditor**: Analisa se os dados fazem sentido ou se há suspeitas de erro/fraude.
4. **Exporter**: Transforma tudo o que foi calculado em arquivos organizados que você abre no Excel ou PowerBI sem limitação de linhas (CSV).

---

## 2. Mapa do Tesouro (O que faz cada pasta e arquivo)

Para facilitar sua jornada, aqui está a lista detalhada de cada "peça" do nosso quebra-cabeça:

### 📂 Pasta `/core/` (O Coração do Sistema)

Aqui fica a inteligência bruta que transforma texto em contabilidade.

- **`reader_ecd.py`**: O "Escriturário". Ele abre o arquivo TXT original e identifica cada linha (campos, blocos e tipos de dados).
- **`processor.py`**: O "Contador Master". É aqui que as tabelas são ligadas, as contas são somadas de baixo para cima (Bottom-Up) e os balancetes são construídos.
- **`auditor.py`**: O "Auditor Eletrônico". Contém a lógica matemática dos 11 testes forenses (consulte os detalhes em [Metodologia de Auditoria](./docs/architecture/audit_methodology.md)).

### 📂 Pasta `/exporters/` (Os Entregadores)

Arquivos que cuidam da saída, formatação e consolidação dos dados.

- **`exporter.py`**: O "Escriturário de Alta Velocidade". Garante que o CSV saia no padrão brasileiro (sep=;) sem o gargalo de memória e o limite de linhas do Excel.
- **`audit_exporter.py`**: Especialista em relatórios de auditoria, gerando Scorecard unificado e arquivos de evidência individuais para perícia detalhada.
- **`consolidator.py`**: O "Agregador de Big Data". Une dezenas de anos em arquivos CSV únicos, suportando milhões de linhas de lançamentos com rastreio de origem.
- **`formatting.py`**: O "Coração Regional". Centraliza as regras de tradução de dados técnicos para o padrão contábil brasileiro (Vírgula decimal).

### 📂 Pasta `/intelligence/` (O Cérebro do Negócio)

Aqui mora o conhecimento acumulado sobre as regras do SPED e da Receita Federal.

- **`historical_mapper.py`**: O "Cérebro da Ponte". Aprende com anos passados para preencher falhas em arquivos antigos (Ponte Virtual).
- **`ref_plan_manager.py`**: O "Bibliotecário Automático". Escaneia diretórios da RFB e constrói amarrações do plano sem metadados estáticos via motor vetorial O(1).
- **`ecd_layout_compiler.py`**: O "Compilador de Metadados". Transforma as regras da RFB em schemas JSON de alta performance.

### 📂 Pasta `/docs/` (A Enciclopédia Técnica)

Manuais detalhados sobre as metodologias aplicadas.

- **`architecture/audit_methodology.md`**: Explica o "porquê" e o "como" de cada teste de auditoria.
- **`architecture/bridging_logic.md`**: Detalha a matemática por trás da recuperação de dados históricos.

### 📂 Pasta `/tools/` (Playground de Desenvolvimento)

Lugar para ferramentas auxiliares. Substitui a antiga `/scripts/`.

- **`dev_audit.py`**: Script prático para testar a auditoria em apenas um arquivo ECD sem precisar rodar o processo inteiro.

### 📂 Pasta `/tests/` (A Prova Real)

Scripts automáticos que conferem se as alterações no código estragaram algo.

- **`test_auditoria_unit.py`**: Verifica se os cálculos de auditoria continuam precisos.
- **`test_integracao.py`**: Testa o caminho completo, do TXT ao Excel, para garantir que o sistema está saudável.

### 📂 Pasta `/data/` (Seu Armazém de Dados)

- **`input/`**: Onde você deve "jogar" os arquivos `.txt` que deseja processar.
- **`output/`**: Onde os relatórios prontos serão entregues pelo programa.
- **`analysis/`**: Guarda relatórios técnicos sobre a evolução dos planos do governo.

---

## 3. Guia de Arquivos Chave (Acesso Rápido)

- **`main.py`**: O orquestrador central. Gerencia a fase comercial, o aprendizado histórico e a execução paralela de auditoria e exportação.
- **`.cursorrules.md`**: Nossas diretrizes de desenvolvimento (as "Leis" do projeto).
- **`requirements.txt`**: Lista de bibliotecas Python que o projeto precisa para funcionar.

---

## 4. Próximos Desafios (Roadmap)

O projeto está consolidado na **v2.9.x** (Alta Performance e Concorrência). O sistema atingiu um estado da arte em velocidade, processando bases multianuais complexas com auditoria forense integrada de dezenas de milhões de lançamentos de forma eficientíssima. O foco atual direciona-se a transformar o script numa robusta e interativa plataforma pericial. Abaixo, os detalhes práticos das evoluções planejadas:

### 🚀 Performance e Escalabilidade (Consolidadas)

#### **1. Processamento Paralelo de Arquivos (Multiprocessing) - ✅ CONCLUÍDO (v2.8.0)**

- **O quê:** Migração do framework sequencial para paralelo inter-arquivos usando `ProcessPoolExecutor`.
- **Resultado:** Redução sistêmica de tempo no processamento inicial das instâncias ECD multi-core.

#### **2. Otimização Preditiva na Fase de Aprendizado - ✅ CONCLUÍDO (v2.7.0)**

- **O quê:** Base de persistência histórica no `HistoricalMapper` escrevendo estado preditivo em `data/knowledge/history.json`.
- **Resultado:** Inibe passagens sequenciais repetitivas e elimina a redundância em limpezas de conhecimento global, poupando minutos preciosos em re-execuções.

#### **3. Transição Base de Lógica Escalar para Matricial O(N) - ✅ CONCLUÍDO (v2.7.0 / v2.9.0)**

- **O quê:** Completa extinção do uso de loops em nível de dado nativos e abandono na inferência contábil procedural via lambdas (`apply`), priorizando operações vetorizadas (`merge` / `add` em Pandas). A fase v2.9.0 focou em consolidadores "Single Pass" de roll-up baseados unicamente em subchaves conjuntas aglutinadas (`DT_FIN`), abolindo repetições hierárquicas sazonais.
- **Resultado:** O processador central experimentou um ganho temporal real escalonado de reduções adicionais de até 65%, enquanto a checagem cruzada da auditoria pericial caiu ~36%.

#### **4. I/O Multithreading Assíncrono - ✅ CONCLUÍDO (v2.9.0)**

- **O quê:** Implementação profunda de concorrência com `ThreadPoolExecutor` para gravação assíncrona dos artefatos contábeis CSV e Parquet, associada ao uso multithread transversal (desacoplamento via GIL-release) dos processos da etapa de Perícia Auditorial.
- **Resultado:** Queda de 40% na camada puramente I/O bound e na gestão massiva intra-ECD, culminando de forma sistêmica numa taxa terminal de melhoria global superior à ~39% na execução inteira.

### 📊 Inteligência de Dados e Perícia

#### **3. Data Warehouse Unificado com DuckDB**

- **O quê:** Consolidar todos os Parquets em um banco de dados analítico único (`sped_pericia.db`).
- **Por quê:** Facilita análises cruzadas. Ex: "Buscar o fornecedor X em todos os anos processados" exige apenas uma consulta SQL, em vez de abrir 11 planilhas Excel.
- **Como:** Utilizar o DuckDB para criar tabelas persistentes apontando para os diretórios de saída. O DuckDB consegue ler Parquets absurdamente rápido e permite usar SQL padrão para gerar relatórios customizados.

#### **4. Dashboard Pericial Interativo (Streamlit)**

- **O quê:** Criar uma interface visual (Web Local) para navegar pelos resultados.
- **Por quê:** Gráficos de barras e dispersão são melhores para identificar "saltos" suspeitos em contas de despesa do que linhas de tabela.
- **Como:** Usar a biblioteca Streamlit para ler os arquivos Parquet e exibir dashboards dinâmicos com filtros por CNPJ, Ano e Conta. Permitir um "Drill-down" onde você clica em um saldo e ele abre a lista de lançamentos que compõem aquele valor.

### 🧹 Robustez e Conformidade

#### **5. Camada de Sanitização Pré-Parser**

- **O quê:** Um "filtro de impurezas" que roda antes do leitor principal.
- **Por quê:** Arquivos SPED gigantes costumam ter caracteres especiais órfãos ou quebras de linha indevidas no meio do histórico, o que causa erro de leitura no Pandas (`ParserError`).
- **Como:** Ler o arquivo em blocos de bytes, remover caracteres não-imprimíveis e validar se o número de delimitadores `|` por linha está correto antes de entregar os dados para o `ECDReader`.

#### **6. Auditoria Cruzada (ECD x ECF)**

- **O quê:** Módulo para ler e comparar os dados contábeis (ECD) com os fiscais (ECF).
- **Por quê:** Detectar divergências entre o que foi contabilizado e o que foi oferecido à tributação no LALUR/LACS.
- **Como:** Expandir o `core/reader_ecd.py` para suportar os registros específicos da ECF e criar uma regra no `core/auditor.py` que faça o batimento automático de saldos entre as duas escriturações.

---
**Nota Técnica**: Este projeto segue o padrão **Data Pipeline**, o que significa que cada peça do quebra-cabeça tem uma função única e isolada (Módulos).
