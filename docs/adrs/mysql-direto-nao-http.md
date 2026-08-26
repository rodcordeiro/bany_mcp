# MCP lê MySQL; não chama a API HTTP

O servidor existe para o host MCP consultar o mesmo banco do Banky sem passar pelo *contrato* HTTP. Consultas parametrizadas e read-only; stdout é JSON-RPC. Chamar `banky_api` por HTTP quebraria o desenho stdio/analytics e duplicaria auth. Mudar isso é troca de superfície, não refactor de query.
