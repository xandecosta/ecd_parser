---
description: Analisa um arquivo e propõe uma refatoração completa com foco em performance e segurança.
---

# Workflow: Refatoração de Código Ouro

Siga rigorosamente estes passos ao ser acionado para refatorar um arquivo:

1. **Leitura e Diagnóstico**:
   Descreva as 10 piores falhas de arquitetura do código atual antes de escrever qualquer nova função no arquivo.
   Foque em identificar gargalos (ex: loops O(n^2)), problemas de legibilidade (Clean Code, PEP-8) e ausência de tratamento de erros defensivo.

2. **Planejamento da Melhoria**:
   Apresente as modificações que pretende fazer.
   Priorize:
   - Melhoria de Performance e redução de uso excessivo de memória.
   - Legibilidade e Tipagem Estática (`typing`).
   - Resiliência com `Try/Except`.

3. **Restrições OBRIGATÓRIAS**:
   - NÃO quebre a compatibilidade com a versão atual da API que consome este código.
   - NÃO adicione bibliotecas externas pesadas a menos que estritamente necessário (mantenha o arquivo `requirements.txt` atual).
   - Comunique verbalmente o impacto em dependências externas de outras classes antes de editar o arquivo.

4. **Ação Rápida**:
   Aguarde a aprovação do usuário sobre os 3 defeitos descritos na etapa 1 para, em seguida, usar as ferramentas para alterar o código.
