# SPED-ECD Parser Pro 🚀

## O que é este projeto?

Este programa é um "tradutor" inteligente de arquivos do **SPED Contábil (ECD)**.

Arquivos ECD são documentos complexos que as empresas enviam ao governo (Receita Federal) contendo toda a sua contabilidade. Nosso software lê esses arquivos (em formato `.txt`), entende a lógica contábil por trás deles e gera relatórios profissionais em **CSV (Padrão PT-BR)** e **Parquet**, prontos para auditoria de larga escala e análise em PowerBI.

### ✨ O que ele faz de especial?

1. **Auditoria Forense Industrial**: 11 testes automatizados (Option D) que geram dashboards unificados e evidências individuais em CSV.
2. **Performance Industrial (Vectorized)**: Núcleo reescrito em `float64` com tecnologia Pandas/NumPy, processando anos de dados em segundos.
3. **Sem Limite de Linhas**: A migração para CSV permite ultrapassar o limite de 1 milhão de linhas do Excel na consolidação histórica.
4. **Ponte Virtual Inteligente**: Recupera informações de anos vizinhos e persiste o conhecimento em disco (`history.json`) via IO Inteligente.
5. **Visão da Receita Federal**: Transforma a contabilidade no formato Referencial exato exigido pelo fisco.

---

### 1. Preparar o Ambiente

**Opção A: Rápida (Recomendado para Windows)**
Apenas dê dois cliques no arquivo **`setup_inicial.bat`** na raiz do projeto. Ele criará o ambiente virtual e instalará todas as dependências automaticamente.

**Opção B: Manual (Terminal)**
Se preferir o terminal, abra a pasta do projeto e use:

```bash
# 1. Criar o ambiente virtual (isolamento do projeto)
python -m venv .venv

# 2. Ativar o ambiente
source .venv/Scripts/activate   # Git Bash (recomendado)
.\.venv\Scripts\activate        # PowerShell
.venv\Scripts\activate          # CMD
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
