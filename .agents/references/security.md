# Seguranca

Fronteiras:

- O servidor acessa MySQL diretamente.
- Nao registrar credenciais, connection strings, tokens, payloads sensiveis ou valores reais de `.env`.
- Nao colocar exemplos com dados pessoais reais em README, AGENTS ou testes.

MCP:

- Tool MCP e codigo executavel; mantenha contratos pequenos e claros.
- Preserve validacao de input com Zod.
- Erros devem explicar a acao sem ecoar valor sensivel.
- stdout pertence ao protocolo JSON-RPC quando usando stdio.

SQL:

- Manter queries parametrizadas.
- Nao interpolar filtros vindos do usuario sem allowlist.
- Qualquer nova query deve avaliar owner, periodo, limites e paginacao.

Operacao:

- Evite aumentar permissoes do usuario do banco; este servidor deve permanecer analitico/read-only.
