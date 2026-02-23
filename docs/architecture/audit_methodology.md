# Metodologia e Algoritmos de Auditoria Forense (Módulo ECDAuditor)

Este documento descreve detalhadamente a lógica, os fundamentos contábeis e os algoritmos de processamento utilizados pelo motor de auditoria forense para arquivos SPED-ECD.

---

## 1. Visão Geral

A auditoria forense do sistema não se limita a validações de layout (já realizadas pelo PVA). Seu objetivo é identificar fraudes, manipulações de resultados, erros de transporte de saldos e inconsistências qualitativas que possam comprometer a fidedignidade das demonstrações contábeis.

### Matriz de Status e Impacto

- **APROVADO**: O teste não identificou divergências acima da tolerância matemática (zero para Decimal).
- **ALERTA**: Identificadas inconsistências qualitativas ou desvios estatísticos que exigem análise humana, sem necessariamente indicar erro aritmético.
- **REPROVADO**: Divergência matemática confirmada ou anomalia grave que invalida a integridade dos dados.
- **IMPACTO**: Representa o valor monetário absoluto envolvido na irregularidade.

---

## 2. Grupo 1: Integridade Estrutural

Foca na amarração matemática entre os diferentes registros da ECD.

### 1.1 Cruzamento Diário vs. Balancete

**Objetivo**: Verificar se a soma dos lançamentos analíticos (I250) corresponde à movimentação informada nos saldos mensais (I155).

- **Algoritmo**:
    1. Agrega todos os lançamentos do Diário por `COD_CTA` e `PERIODO`.
    2. **Filtro Forense**: O motor ignora automaticamente lançamentos do tipo 'E' (Encerramento), pois o balancete processado já reflete os saldos restaurados ao estado pré-encerramento.
    3. **Isolamento de Contas**: O teste é aplicado exclusivamente a contas analíticas (`IND_CTA == 'A'`).
    4. Compara a soma dos lançamentos (`VL_D` e `VL_C`) com os campos `VL_DEB` e `VL_CRED` do registro I155.
    5. **Composição da Prova**: Identifica as chaves (Conta + Período) com erro e extrai todos os registros I250 correspondentes para a aba `EVIDENCIAS_DETALHADAS`.
- **Fundamento**: Princípio da Partida Dobrada e Integridade de Transporte.

### 1.2 Validação da Hierarquia Nativa

**Objetivo**: Validar se a estrutura de consolidação sintética informada no arquivo é matematicamente consistente (Bottom-Up).

- **Algoritmo**:
    1. Iterar por cada período (mês) presente no balancete.
    2. Identificar todas as contas sintéticas (`IND_CTA = 'S'`).
    3. Para cada conta sintética:
        - Localizar todas as contas filhas diretas (`COD_CTA_SUP` == conta atual).
        - Somar o saldo final (`VL_SLD_FIN_SIG`) das filhas.
        - Comparar o somatório com o saldo informado na conta sintética pai.
    4. Reportar divergências de soma ou contas sintéticas sem filhas que possuam saldo.
- **Fundamento**: Estrutura Conceitual para Relatórios Financeiros (NBC TG Estrutura Conceitual).

---

## 3. Grupo 3: Coerência Referencial (Normas RFB)

Valida a conformidade com as regras de mapeamento da Receita Federal.

### 3.1 Consistência de Natureza

**Objetivo**: Garantir que o mapeamento para o Plano de Contas Referencial respeita a natureza da conta (Ativo/Passivo/DRE).

- **Lógica de Validação**:
    - **Nat 01 (Ativo)**: O código referencial deve obrigatoriamente iniciar com `1`.
    - **Nat 02 (Passivo)**: O código referencial deve obrigatoriamente iniciar com `2`.
    - **Nat 03 (PL)**: O código referencial deve obrigatoriamente iniciar com `2`.
    - **Nat 04 (Resultado)**: O código referencial deve obrigatoriamente iniciar com `3`.
    - **Nat 05/09**: Não devem estar mapeadas no Grupo 1 (Ativo) ou Grupo 2 (Passivo).
- **Status**: Gera **ALERTA** se detectar cruzamentos indevidos.

### 3.2 Auditoria de Contas "Órfãs"

**Objetivo**: Identificar contas analíticas com movimentação relevante que não possuem mapeamento para o plano referencial (campo I051 ausente).

- **Algoritmo**:
    1. Filtrar contas analíticas (`IND_CTA == 'A'`) com Mapping Referencial (`COD_CTA_REF`) nulo ou vazio.
    2. Aplicar filtro de relevância: Saldo Inicial, Débito, Crédito ou Saldo Final devem ser diferentes de zero.
    3. Reportar ocorrências, destacando o maior saldo absoluto encontrado no período.

---

## 4. Grupo 4: Análise Forense de Padrões

Utiliza métodos estatísticos e detecção de anomalias temporais.

### 4.1 Lei de Benford (Teste do Primeiro Dígito)

**Objetivo**: Detectar se os valores dos lançamentos foram manipulados artificialmente através da análise da frequência natural dos dígitos iniciais.

- **Algoritmo**:
    1. Extrai o primeiro dígito significativo (1-9) de todos os valores absolutos do Diário.
    2. Compara a frequência observada com a distribuição teórica: $P(d) = \log_{10}(1 + \frac{1}{d})$.
    3. **Indicador de Risco (MAD)**: Utiliza o *Mean Absolute Deviation*.
        - **APROVADO**: MAD < 0.012
        - **ALERTA**: MAD entre 0.012 e 0.020
        - **REPROVADO**: MAD > 0.020 (indica forte desvio estatístico)
    4. **Composição da Prova (Drill-Down)**: Identifica os dígitos viciados, mapeia os 10 valores exatos mais frequentes, apontando contas e históricos predominantes.

### 4.2 Detecção de Duplicidades

**Objetivo**: Identificar erros de digitação ou fraudes por duplicidade de lançamentos.

- **Algoritmo**:
    1. Busca no Diário lançamentos com a mesma **Data**, **Conta**, **Valor** e **Histórico**.
    2. **Filtro de Ruído Forense**:
        - **Filtro A (Materialidade)**: Ignora duplicidades de baixo valor (< R$ 100,00) em contas bancárias/tarifas.
        - **Filtro B (Batch Processing)**: Se > 5 lançamentos idênticos no dia, assume conformidade operacional (ex: folha).
- **Impacto**: Soma absoluta dos lançamentos classificados como "Suspeitos" após filtragem.

### 4.3 Omissão de Encerramento (DRE não zerada)

**Objetivo**: Confirmar se as contas de resultado foram devidamente zeradas ao final do exercício.

- **Algoritmo**:
    1. Soma os saldos finais de todas as contas analíticas de natureza `04` na última data do arquivo.
    2. Confronta o saldo com os lançamentos de encerramento (`IND_LCTO = 'E'`).
    3. O resultado líquido **deve ser zero**. Caso contrário, reportar omissão de encerramento.

---

## 5. Grupo 5: Indicadores Profissionais (Qualitativos)

### 5.1 Inversão de Natureza

**Objetivo**: Detectar saldos anômalos (Ativo Credor ou Passivo Devedor).
**Lógica**: Identifica Ativo < 0 ou Passivo > 0, ignorando contas redutoras (ex: depreciasão, amortisação).

### 5.2 Estouro de Caixa

**Objetivo**: Identificar momentos em que as contas de disponibilidade ficaram com saldo credor.
**Lógica**: Filtra contas de Caixa/Bancos (via Nome ou Referencial 1.01.01) e reporta qualquer saldo final negativo no mês.

### 5.3 Passivo Fictício (Estaticidade)

**Objetivo**: Identificar obrigações que permanecem no balancete sem qualquer movimentação no ano.
**Lógica**: Identifica contas de natureza `02` com saldo > R$ 1.000,00 que tiveram **zero** movimentação de débito e crédito no exercício.

### 5.4 Consistência PL vs. Resultado

**Objetivo**: Validar a amarração técnica entre lucro da DRE e transferência para o PL.
**Lógica**: Compara o Lucro Líquido da DRE com o valor total dos lançamentos 'E' que atingiram o Patrimônio Líquido.
**Tolerância**: Admite divergência de até **R$ 100,00** para arredondamentos.

---

## 6. Fluxo de Output e Evidências

Toda a metodologia converge para o arquivo `<DATA>_07_Auditoria.xlsx`. A inovação forense principal é a aba **EVIDENCIAS_DETALHADAS**, que extrai as linhas do Diário (I250) vinculadas a qualquer erro identificado, poupando o trabalho de busca manual no PVA.
