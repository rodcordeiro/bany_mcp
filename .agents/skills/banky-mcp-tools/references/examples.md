# Exemplos de uso

## Resumo financeiro

Pedido: "Quanto entrou e saiu nos ultimos 30 dias?"

1. Chamar `mcp__banky__transactions_overview` com `{ "days": 30 }` e `ownerId` quando necessario.
2. Informar `income`, `expense`, `net`, quantidade de transacoes e periodo.
3. Usar `transactions_timeline` apenas se o usuario pedir evolucao diaria.

## Investigar gasto atipico

Pedido: "Quais categorias subiram muito nesta semana?"

1. Chamar `mcp__banky__transactions_detect_anomalies` com `{ "recentDays": 7, "baselineDays": 30 }`.
2. Apresentar `recentDaily`, `baselineDaily`, `deltaAbsolute` e `deltaPercent`.
3. Explicar que o resultado aponta variacao, nao fraude confirmada.
4. Se solicitado, detalhar a categoria com `transactions_list` usando periodo e busca adequados.

## Conferir sincronizacao

Pedido: "Mostre transacoes com erro de sincronizacao."

Chamar `mcp__banky__transactions_list` com, por exemplo, `{ "status": "error", "days": 30, "limit": 50, "offset": 0 }`. Declarar que o retorno e uma pagina de ate 50 itens.

## Avaliar feedbacks do modelo

Pedido: "Como esta a qualidade e quanto ainda pode ir para treino?"

1. Chamar `mcp__banky__feedbacks_quality` para acuracia por campo.
2. Chamar `mcp__banky__feedbacks_training_queue` para elegibilidade atual.
3. Se volume e distribuicao forem relevantes, acrescentar `feedbacks_overview`.
4. Nao afirmar que itens foram enviados ou marcados para treino.

## Buscar feedbacks corrigidos

Pedido: "Liste feedbacks corrigidos que mencionam mercado."

Chamar `mcp__banky__feedbacks_list` com `{ "status": "corrected", "search": "mercado", "limit": 50, "offset": 0 }`. Resumir os itens sem reproduzir texto pessoal alem do necessario.

## Falhas e ausencia de dados

- Se faltar `ownerId`, solicitar o identificador ou orientar a configurar `DEFAULT_OWNER_ID`; nunca inventar um valor.
- Se o periodo retornar zero itens, dizer "nenhum registro encontrado com estes filtros", nao concluir que nao existem dados fora do periodo.
- Se `baselineDays <= recentDays`, corrigir os parametros antes da chamada.
