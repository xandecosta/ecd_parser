# SPED-ECD Parser Pro 🚀

## O que é este projeto?

Este programa é um "tradutor" inteligente de arquivos do **SPED Contábil (ECD)**.

Arquivos ECD são documentos complexos que as empresas enviam ao governo (Receita Federal) contendo toda a sua contabilidade. Nosso software lê esses arquivos (em formato `.txt`), entende a lógica contábil por trás deles e gera relatórios organizados em **Excel** e **Parquet**, prontos para auditoria e análise financeira.

### ✨ O que ele faz de especial?

1. **Auditoria Forense Automática**: Procura erros, fraudes e inconsistências nos lançamentos.
2. **Ponte Virtual**: Recupera informações de anos vizinhos para completar dados que faltam em arquivos antigos (como o ano de 2014).
3. **Visão da Receita Federal**: Transforma a contabilidade da empresa no formato que o governo exige (Plano Referencial).
4. **Consolidação Inteligente**: Junta vários anos em um único resumo para você ver a "saúde" da empresa ao longo do tempo.
5. **Máxima Performance (O(1))**: Núcleo vetorial refatorado para processar milhares de contas via C-engine sem asfixiar a sua memória RAM.

---

## 🚀 Como Começar (Início Rápido)

### 1. Preparar o Ambiente

Se você está no Windows, abra o terminal na pasta do projeto e use:

```bash
# 1. Criar o ambiente virtual (isolamento do projeto)
python -m venv venv

# 2. Ativar o ambiente
source venv/Scripts/activate # Windows
.\venv\Scripts\activate  # Windows alternative
source venv/bin/activate # Linux/Mac
pip install -r requirements.txt
```

### 2. Rodar o Programa

Siga estes dois passos simples:

1. **Preparar Planos do Governo**: Rode o veloz gestor de tabelas para que o robô escaneie eficientemente a pasta de metadados brutos via C-engine e construa todos os catálogos base:

    ```bash
    python intelligence/ref_plan_manager.py
    ```

2. **Processar seus Arquivos**: Coloque seus arquivos `.txt` (ECD) na pasta `data/input` e rode o motor principal:

    ```bash
    python main.py
    ```

---

## 🗺️ Onde encontro cada coisa?

Para que você não se perca, dividimos a documentação por necessidade:

| Documento | Quando abrir? |
| :--- | :--- |
| **[CONTEXT.md](./CONTEXT.md)** | "Quero saber o que cada pasta/arquivo faz" ou "Como o código funciona?" |
| **[.cursorrules.md](./.cursorrules.md)** | "Quais são as regras de ouro do projeto?" (Decimal, UTF-8, etc) |
| **[CHANGELOG.md](./CHANGELOG.md)** | "O que mudou na última versão?" |
| **[Metodologia de Auditoria](./docs/architecture/audit_methodology.md)** | "Como o teste de fraude (Benford) funciona?" |

---
**Dica para Iniciantes**: Sempre que for rodar o sistema, lembre-se de ativar o ambiente virtual (`venv`). Se o terminal mostrar `(venv)` ao lado do nome da pasta, você está pronto!
