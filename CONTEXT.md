# Banky MCP

Servidor de consulta analítica do Banky: o agente lê Transactions e Feedbacks de um User, sem criar lançamento, sem revisar Feedback e sem treinar modelo.

## Language

Os termos **User**, **Account**, **Saldo**, **Category**, **Transaction**, **Transfer**, **Credit payment**, **Feedback**, **Status de negócio** e **usedForTraining** são os da API principal. Abaixo só o que este servidor precisa distinguir.

**Owner id**:
Identificador do User cujos dados a consulta deve restringir.
_Avoid_: username, token, “o usuário logado no host”

**Net**:
Entrada menos saída dos Transactions no período, pela Category (`positive`). Não é o Saldo da Account.
_Avoid_: Saldo, ammount, balance

**Income**:
Soma dos Transactions cuja Category é crédito no período.
_Avoid_: Saldo, depósito bancário

**Expense**:
Soma dos Transactions cuja Category é débito no período.
_Avoid_: Fatura, credit payment

**Sync status**:
Estado opcional de sincronização de um Transaction neste servidor (`pending`, `processing`, `done`, `error`). Não faz parte do *contrato* HTTP atual da API.
_Avoid_: Status de negócio do Feedback, “erro de lançamento”

**Anomaly**:
Sinal estatístico de aumento de Expense por Category frente a uma baseline. Não é fraude nem erro confirmado.
_Avoid_: Fraude, outlier confirmado, alerta de cartão

**Card spending**:
Expense em Accounts cujo Payment type o servidor trata como cartão (nome contendo crédito/cartão). Não é o fluxo Credit payment.
_Avoid_: Credit payment, fatura paga, Transfer

**Feedback legado**:
Feedback no formato JSON previsto/corrigido (`predictedJson` / `userCorrectedJson`), que este servidor ainda consulta. Distinto do Feedback em campos separados da API principal.
_Avoid_: Feedback da API, predicted fields, orrected*

**Training queue**:
Feedbacks elegíveis para treino (status de negócio diferente de pending e ainda não usados). Consultar a fila não treina e não marca usedForTraining.
_Avoid_: Treino executado, POST /nlp/training

**Accuracy**:
Proporção entre 0 e 1 de matches entre previsto e corrigido no conjunto comparado. Não é percentual até a apresentação.
_Avoid_: Qualidade do autoavaliador, score de shadow
