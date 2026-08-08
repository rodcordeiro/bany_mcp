# Divida Tecnica

- O package e o checkout usam `bany_mcp`, enquanto o nome operacional do servidor e `bany-mcp`; manter a diferenca explicita ate haver rename coordenado.
- Nao ha testes automatizados no checkout atual.
- Outputs sao JSON em `content.text`; avaliar retorno mais estruturado se o host consumidor exigir.
- Falhas de analytics dependem do schema real MySQL; manter verificacoes defensivas de tabela/coluna.
- README contem exemplos de caminho local antigo; evitar propagar path absoluto em novos exemplos.
