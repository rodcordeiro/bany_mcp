# Divida Tecnica

- O package e o checkout usam `bany_mcp`, enquanto o nome operacional do servidor e `bany-mcp`; manter a diferenca explicita ate haver rename coordenado.
- Tools de Feedback exigem `bk_nlp_feedback` (JSON legado). A API principal persiste `bk_tb_feedback`; no schema atual da API as tools de Feedback falham.
- Tools de Transaction exigem owner; tools de Feedback aceitam omitir e nao filtram dono.
- Sem coluna `sync_status`, listagem preenche `pending` e o filtro de status nao aplica.
- `transactions_overview.net` e fluxo do periodo, nao Saldo da Account.
- Nao ha testes automatizados no checkout atual.
- Outputs sao JSON em `content.text`; avaliar retorno mais estruturado se o host consumidor exigir.
- Falhas de analytics dependem do schema real MySQL; manter verificacoes defensivas de tabela/coluna.
- README contem exemplos de caminho local antigo; evitar propagar path absoluto em novos exemplos.
