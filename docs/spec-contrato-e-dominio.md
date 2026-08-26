# Spec: *contrato* e domínio do bany-mcp (review 2026-08-18)

## Problem Statement

Quem usa as tools do MCP Banky não sabe o que está lendo. A skill chama o net do overview de “saldo”, mas Saldo no Banky é o `ammount` da Account. As tools de Feedback prometem qualidade e fila de treino, porém o servidor ainda exige a tabela JSON `bk_nlp_feedback`; a API principal grava `bk_tb_feedback` em colunas. Transações exigem owner id; Feedbacks aceitam consulta sem dono. Filtro de sync status some no silêncio se a coluna não existe e a listagem inventa `pending`. Não há teste automatizado. Agente mistura fraude, Credit payment, treino e *contrato* HTTP.

## Solution

Uma spec de registro descreve o servidor **como está**: nove tools read-only via stdio, MySQL direto, Net ≠ Saldo, Feedback legado JSON, owner obrigatório só em transações. Lacunas (apontar Feedback para o schema da API, exigir owner nas tools de Feedback, deixar de fingir sync status, testes) ficam explícitas e fora desta implementação. Glossário no CONTEXT; catálogo na skill; operação no AGENTS; knowledge no Nero.

## User Stories

1. As an analyst, I want a period overview of Income, Expense and Net for my User, so that I can see cash flow without opening the app.
2. As an analyst, I want Net explained as Income minus Expense, so that I do not confuse it with Account Saldo.
3. As an analyst, I want top expense Categories in the overview, so that I know where money went.
4. As an analyst, I want pendingSync in the overview when the connected schema has sync or integrated columns, so that I see leftover sync work.
5. As an analyst, I want pendingSync to be zero when those columns do not exist, so that I do not invent a sync queue.
6. As an analyst, I want a daily timeline of Income, Expense and Net, so that I can see the period unfold.
7. As an analyst, I want a paginated Transaction list for my User, so that I can inspect descriptions, values, Account and Category names.
8. As an analyst, I want to search the list by description, Category, Account or id, so that I can find a lançamento without dumping the period.
9. As an analyst, I want to know a list page is not the full universe, so that I do not sum pages as totals.
10. As an analyst, I want to filter the list by Sync status when that column exists, so that I can look at error/pending sync rows.
11. As an analyst, I want a clear no-op when I filter Sync status against a schema without that column, so that I do not think the API hid the errors.
12. As an analyst, I want list rows not to fake Sync status `pending` when the column is missing, so that I do not chase a ghost queue.
13. As an analyst, I want Anomaly detection by Category expense versus a longer baseline, so that I see unusual increases.
14. As an analyst, I want Anomaly described as a statistical signal, so that I do not call it fraud.
15. As an analyst, I want `baselineDays` greater than `recentDays`, so that the comparison window is valid.
16. As an analyst, I want default windows (7 vs 30) and thresholds (40% and 10) documented, so that I know what “atípico” meant.
17. As an analyst, I want card spending totals for Accounts whose Payment type name looks like credit/card, so that I can see cartão Expense.
18. As an analyst, I want card spending to exclude internal Categories when that flag exists, so that Transfer-like internals do not inflate the cartão bill.
19. As an analyst, I want card spending not called Credit payment, so that I do not mix fatura payment with cartão Expense.
20. As a reviewer, I want Feedback overview volume by Status de negócio and training coverage, so that I see the queue health.
21. As a reviewer, I want Feedback list with status, usedForTraining and text search, so that I can sample corrected items.
22. As a reviewer, I want Feedback quality Accuracy per intent, Category, Account and value, so that I see where the parser disagrees with the human.
23. As a reviewer, I want quality to compare only rows with a correction blob, so that validated-without-correction is not scored as a match.
24. As a reviewer, I want Accuracy as a 0–1 proportion, so that the host does not double-convert percentages.
25. As a reviewer, I want a Training queue of non-pending unused Feedbacks, so that I know what *could* go to treino.
26. As a reviewer, I want consulting the Training queue to leave usedForTraining untouched, so that looking is not training.
27. As a reviewer, I want Feedback tools to fail loudly when `bk_nlp_feedback` is absent, so that I do not read `bk_tb_feedback` as if it were JSON.
28. As a reviewer, I want the spec to say Feedback legado ≠ Feedback da API, so that I do not map `orrected*` keys onto `userCorrectedJson`.
29. As a user, I want Transaction tools to refuse the call without owner id (or DEFAULT_OWNER_ID), so that totals are never mixed across Users.
30. As a user, I want Feedback tools to require the same owner rule, so that quality metrics are not global by accident.
31. As a user, I want today’s Feedback tools to document that owner is optional and skipped when absent, so that I know the current leak.
32. As an agent, I want to pick an aggregate tool before a list, so that I do not page through the database to answer “how much”.
33. As an agent, I want host names `mcp__banky__*` while the server registers unprefixed names, so that I call what the host actually exposes.
34. As an agent, I want stdout reserved for JSON-RPC, so that a log line does not break the session.
35. As an agent, I want success as formatted JSON text and expected failure as `isError`, so that I can branch without parsing stack traces.
36. As an agent, I want Zod to reject out-of-range days, limits and enums, so that I do not send unbounded queries.
37. As an agent, I want never to invent an owner id, so that I ask or use the configured default.
38. As an agent, I want never to print connection strings or `.env` values, so that the chat stays free of secrets.
39. As an operator, I want the process to close the MySQL pool on SIGINT/SIGTERM, so that deploys do not leak connections.
40. As an operator, I want DEFAULT_LOOKBACK_DAYS (1–365, default 30) when the caller omits days, so that ad-hoc questions still have a window.
41. As an operator, I want parameterized SQL only, so that search text cannot become a query.
42. As an operator, I want the DB user to stay read-only in intent, so that a bad tool cannot UPDATE usedForTraining.
43. As an operator, I want package `bany_mcp`, server name `bany-mcp`, and checkout folder `bany_mcp` documented together, so that rename is a coordinated change.
44. As an operator, I want README examples without a machine-specific absolute path as the canonical sample, so that clones do not copy `D:/projetos/...`.
45. As a developer, I want `server` to own MCP schemas and `analytics` to own SQL, so that a new tool has one place for *contrato* and one for query.
46. As a developer, I want adding a tool to update the skill catalog and README, so that agents do not call a ghost name.
47. As a developer, I want defensive table/column checks, so that an older MySQL still answers Transaction tools.
48. As a developer, I want those checks not to invent domain values (fake pending, fake JSON), so that tolerance ≠ lying.
49. As a tester, I want at least one automated seam on analytics results, so that schema drift fails in CI instead of in chat.
50. As a product owner, I want migrating Feedback queries to `bk_tb_feedback` to be a later spec, so that this review does not silently rewrite SQL.
51. As a product owner, I want dropping Sync status from the *contrato* to be a later spec if the API never grows that column.
52. As a consumer of banky-api language, I want this server to keep User ≠ Account and Feedback status ≠ auto-review decision, so that MCP answers stay in Banky vocabulary.
53. As an analyst, I want zero rows in a window explained as “none with these filters”, so that I do not conclude the User has no history outside the window.
54. As an agent, I want composed questions to use independent tools only when each part is needed, so that I do not spray nine calls.
55. As a security reviewer, I want Feedback search not to echo full originalText in the agent reply, so that PII stays minimized.

## Implementation Decisions

- Este servidor é analytics read-only sobre MySQL. Não é cliente HTTP da API. Não escreve Transaction, Feedback, alias ou treino.
- Nove tools: five de Transaction (overview, timeline, list, anomalies, card spending) e four de Feedback (overview, list, quality, training queue). Capability: `tools` only.
- Nomes registrados no servidor sem prefixo; o host Banky expõe `mcp__banky__<nome>`.
- Owner id é o User. Tools de Transaction exigem owner na chamada ou `DEFAULT_OWNER_ID`. Tools de Feedback **hoje** aceitam omitir e, sem id, não filtram `owner`.
- Days omitido usa `DEFAULT_LOOKBACK_DAYS` (default 30, 1–365).
- Income / Expense / Net vêm de Transaction.value × Category.positive no período. Net não é Saldo (`ammount`).
- Listagem de Transaction é uma página (`limit` default 50, max 200; `offset` default 0). Não há totalCount garantido.
- Sync status (`pending|processing|done|error`) é *contrato* deste servidor, opcional no schema. Sem coluna: filtro por status não aplica; listagem **hoje** preenche `pending`. Documentar como débito, não como verdade da API.
- Anomaly compara médias/totais de Expense por Category; `baselineDays > recentDays` é erro explícito.
- Card spending é heurística no nome do Payment type (`credito` / `credit` / `cartao`) + Category débito; não é o fluxo Credit payment.
- Feedback *contrato* atual = tabela `bk_nlp_feedback` e blobs JSON. Ausência da tabela → erro nomeando a tabela. Não há fallback para `bk_tb_feedback`.
- Quality compara JSON paths intent/category/account/origin/destiny/value; `totalCompared` são linhas com `userCorrectedJson` não nulo. Account match mistura account/origin/destiny.
- Training queue: status ≠ pending e usedForTraining falso. A chamada não faz UPDATE.
- Sucesso: JSON em `content[type=text]`. Erro esperado: `isError: true` sem ecoar segredo. Zod issues serializadas.
- stdout = JSON-RPC. Pool fecha em SIGINT/SIGTERM.
- Checkout `bany_mcp`; metadata `bany-mcp` 0.2.0. Rename só com pedido explícito.
- Consumidor da linguagem: mesma do CONTEXT da API. Consumidor das tools: agentes no host. Mudança de *contrato* MCP deve atualizar a skill; mudança de schema Banky deve atualizar `analytics`.

## Testing Decisions

- Testar comportamento observável: JSON de cada tool (periodDays, net = income − expense, erro de tabela de Feedback, owner obrigatório em transações, `baselineDays <= recentDays` → isError, página de list não afirma total). Não testar cache de information_schema nem SQL string interna além do efeito.
- Seam desta spec: funções de analytics + o dispatcher de tools (validação Zod e `isError`). É o ponto mais alto que existe hoje: não há e2e nem host de teste. Um único seam de contrato (input → objeto de resultado / mensagem de erro) evita duplicar SQL em mock de MCP SDK.
- Prior art: nenhum `*.spec.ts` no checkout. O primeiro teste deve nascer nessa seam, não em typecheck-only.
- Não usar `pnpm run typecheck` como prova de *contrato*. Typecheck não vê `bk_nlp_feedback` vs `bk_tb_feedback`.
- Fixtures de linha devem usar vocabulário Banky (User id, Category.positive, Feedback status pending/validated/corrected) sem connection strings reais.

## Out of Scope

- Reapontar Feedback tools para `bk_tb_feedback` / predicted* / corrected*.
- Exigir owner nas tools de Feedback no código (só documentado como lacuna).
- Remover ou realinhar Sync status com a entidade Transaction da API.
- Chamar banky_api por HTTP, ligar writes, ou marcar usedForTraining.
- Renomear package/pasta `bany_mcp`.
- Ativar testes no CI neste recorte (a spec só escolhe a seam).
- Issue tracker; entrega combinada: `docs/` + Nero.

## Further Notes

Grill fechado com evidência de checkout MCP × entidades da API × serviço NLP antigo:

- `bk_nlp_feedback` + JSON é o *contrato* vivo do MCP; `bk_tb_feedback` + colunas é o *contrato* vivo da API. Os dois não se substituem sozinhos.
- Overview “saldo” na skill estava errado; Net ≠ Saldo. CONTEXT e ADR registram a distinção.
- Transaction **não** tem `sync_status` na API. O MCP tolera a coluna se existir.
- Feedback tools sem owner: fato do dispatcher, não da skill (a skill fala em confirmar owner).
- Log de fatal ainda diz `[banky_mcp]`; metadata diz `bany-mcp`.

Aberto (não assumido):

1. Migrar Feedback para o schema da API agora ou manter legado até o banco antigo morrer?
2. Owner obrigatório também em Feedback?
3. Sync status sai do *contrato* ou a API passará a persistir?
4. Heurística de cartão vira Payment type canônico?
5. Um teste de analytics entra neste recorte ou numa spec seguinte?

Seam adotada: analytics + dispatcher. Se o time quiser só smoke manual no host, a lacuna de schema continua invisível até a tool falhar em produção.
