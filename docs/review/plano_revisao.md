# Plano de Revisão e Refatoração Ouro

Este documento detalha a estratégia "Bottom-Up" (de baixo para cima) para a refatoração completa do sistema, visando performance (vetorização Pandas), resiliência a falhas de entrada e saída (IO), tipagem forte e organização arquitetural.

O processo de revisão deverá seguir rigorosamente a ordem abaixo. Alterar o topo antes da base pode gerar falhas generalizadas e refatoração redundante.

## Fase 1: Fundações (Módulos Independentes)

Módulos base. Alterá-los primeiro é seguro porque eles afetam como o sistema rastreia os eventos no tempo, mas não quebram a lógica de negócio contábil.

1. `core/telemetry.py`
   - **Responsabilidade**: Rastreio do tempo de processamento e métricas de execução.
   - **Foco de Refatoração**: Tipagem estrita, performance bruta na gravação de tempos e garantir a confiabilidade de dados de métricas mesmo em cenários multiprocessados.

2. `intelligence/historical_mapper.py`
   - **Responsabilidade**: "Cérebro" do sistema focado no armazenamento de de-para histórico (Aprendizado Contínuo).
   - **Foco de Refatoração**: Leitura otimizada no carregamento de JSON/Conhecimento Prévio, manipulação segura (DataFrames para estruturas internas de dicionário) e ausência de métodos que causem estouro de memória (`memory leak`).

## Fase 2: O Núcleo Duro (Core Business Logic)

O coração da aplicação contábil. O Padrão Ouro deve exigir O(1) de Complexidade de Ciclo e operações massivamente vetorizadas pelo Motor Pandas Native (C++).

1. `core/reader_ecd.py`
   - **Responsabilidade**: Primeiro ponto de contato (I/O intensivo) com os gigantescos arquivos SPED.
   - **Foco de Refatoração**: Tolerância robusta sobre encodings exóticos de contribuintes.

2. `core/processor.py`
   - **Responsabilidade**: O motor e encanamento pesado (Fábrica de Balanços, DREs).
   - **Foco de Refatoração**: Exterminar loops `for` ocultos e iterações nativas do pandas. Substituição pesada e completa por vetorizações (`np.where()`, merges indexados e broadcasting numérico).

3. `core/auditor.py`
   - **Responsabilidade**: Validador forense da contabilidade entregue.
   - **Foco de Refatoração**: Reduzir uso de memória. Comparação inter-relacional veloz entre Plano de Contas Referencial (RFB) X Contribuintes X Diário.

## Fase 3: A Camada de Apresentação (Output)

Arquivos encarregados da entrega visual (arquivos tangíveis XLSX e log reports estáticos).

1. `exporters/exporter.py`
   - **Responsabilidade**: Saídas XLSX (Demonstrações unificadas e estruturadas).
   - **Foco de Refatoração**: Cumprir estritamente as regras globais PT-BR na apresentação Excel (`DD/MM/AAAA` e `,` nas colunas métricas), visibilidade padronizada e separação estrita da conversão visual vs pureza de dados.

2. `exporters/audit_exporter.py`
   - **Responsabilidade**: Painéis sumários, gravação de detalhamento forense persistente (Parquet e JSON).
   - **Foco de Refatoração**: Escrita rápida ao disco. Isolar dependências que quebram o código por pastas ou arquivos bloqueados.

3. `exporters/consolidator.py`
   - **Responsabilidade**: União do "lote" gerado em um pacotão ou panorama coeso.
   - **Foco de Refatoração**: Consumir saídas O(N log N) minimizando as chances de estourar a memória RAM quando rodarmos mais de mil arquivos simultâneos.

## Fase 4: O Topo da Pirâmide (Orquestração)

O arquivo central de tudo que foi testado com sucesso nos andares inferiores.

1. `main.py`
   - **Responsabilidade**: Chamar todos de maneira autônoma, concorrente (ThreadPool/ProcessPool) com limpeza constante dos ambientes temporários.
   - **Foco de Refatoração**: Orquestração limpa. Somente a chamada e captura de falhas graves, removendo as funções lógicas desnecessárias que habitam seu bloco condicional atual.
