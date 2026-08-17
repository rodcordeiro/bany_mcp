---
name: banky-mcp-tools
description: Consultar e interpretar transacoes financeiras e feedbacks de NLP do ecossistema Banky pelas tools read-only do MCP Banky. Usar quando o pedido envolver resumo financeiro, linha do tempo, busca ou paginacao de transacoes, gastos no cartao, anomalias de despesas, volume ou qualidade de feedbacks e fila de treinamento.
---

# Banky MCP Tools

Consultar dados do Banky somente pelas tools `mcp__banky__*` disponibilizadas pelo host.

## Fluxo

1. Identificar se o pedido trata de transacoes ou feedbacks.
2. Escolher a menor tool que responda diretamente a pergunta; consultar [references/tools.md](references/tools.md) para contratos e limites.
3. Confirmar `ownerId` quando o usuario pedir um titular especifico. Omitir somente quando o ambiente puder usar `DEFAULT_OWNER_ID`.
4. Definir periodo e filtros explicitamente quando forem relevantes. Nao inferir identificadores, status ou valores financeiros.
5. Executar primeiro uma tool agregada; usar listagem apenas para detalhar ou paginar.
6. Explicar periodo, filtros e unidade das metricas. Distinguir dado retornado de inferencia.

## Regras

- Tratar todas as tools como analiticas e read-only; elas nao corrigem transacoes, nao alteram feedbacks e nao marcam itens como usados em treino.
- Nunca expor credenciais, configuracao do banco, connection strings ou valores de `.env`.
- Nao enviar texto sensivel em `search` sem necessidade. Nao reproduzir payloads pessoais completos na resposta.
- Respeitar ranges e enums documentados. Em anomalias, garantir `baselineDays > recentDays`.
- Nao somar resultados de paginas como se fossem o universo completo. Informar `limit` e `offset` ao apresentar amostras.
- Interpretar `accuracy`, `correctedRate` e `trainingCoverageRate` como proporcoes entre `0` e `1`; converter para percentual apenas na apresentacao.
- Tratar anomalias como sinal estatistico, nao como fraude ou erro confirmado.
- Se a tool retornar erro de tabela/coluna ausente, relatar limitacao do schema conectado sem tentar alterar banco ou servidor.
- Preservar a nomenclatura exata das tools. No codigo do servidor os nomes nao possuem o prefixo do host; nas chamadas usar `mcp__banky__<tool>`.

## Composicao

Para perguntas compostas, seguir [references/examples.md](references/examples.md). Fazer chamadas independentes apenas quando cada uma responder uma parte necessaria; evitar consultas amplas por precaucao.

## Resultado

Responder de forma compacta com:

- periodo e filtros aplicados;
- metricas ou itens relevantes;
- limitacoes da consulta;
- proxima consulta sugerida somente quando agregar valor.
