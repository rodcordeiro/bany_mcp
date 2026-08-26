# Feedback tools ainda falam o schema legado

As tools de Feedback consultam `bk_nlp_feedback` com `predictedJson` / `userCorrectedJson` (formato do serviço NLP antigo). A API principal persiste Feedback em `bk_tb_feedback` com campos previstos/corrigidos. Enquanto não houver decisão de migrar as queries, o *contrato* MCP de Feedback é o legado; contra o schema da API as tools de Feedback falham com tabela ausente. Não “corrigir” silenciosamente para colunas novas sem spec.
