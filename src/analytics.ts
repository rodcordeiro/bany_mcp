import type { Pool, RowDataPacket } from 'mysql2/promise';

export type SyncStatus = 'pending' | 'processing' | 'done' | 'error';

export type OverviewParams = {
  ownerId: string;
  days: number;
};

export type TimelineParams = {
  ownerId: string;
  days: number;
};

export type ListTransactionsParams = {
  ownerId: string;
  days: number;
  status: 'all' | SyncStatus;
  search?: string;
  limit: number;
  offset: number;
};

export type AnomalyParams = {
  ownerId: string;
  recentDays: number;
  baselineDays: number;
  minIncreasePercent: number;
  minAbsoluteDelta: number;
  limit: number;
};

export type CreditCardSpendingParams = {
  ownerId: string;
  days: number;
  limit: number;
};

export type FeedbackStatus = 'pending' | 'validated' | 'corrected';

export type FeedbackOverviewParams = {
  ownerId?: string;
  days: number;
};

export type ListFeedbackParams = {
  ownerId?: string;
  days: number;
  status: 'all' | FeedbackStatus;
  usedForTraining?: boolean;
  search?: string;
  limit: number;
  offset: number;
};

export type FeedbackQualityParams = {
  ownerId?: string;
  days: number;
};

export type FeedbackTrainingQueueParams = {
  ownerId?: string;
  days: number;
  limit: number;
};

type TotalsRow = RowDataPacket & {
  income: number | null;
  expense: number | null;
  transactions: number | null;
};

type PendingRow = RowDataPacket & {
  pending_sync: number | null;
};

type TopCategoryRow = RowDataPacket & {
  category_id: string | null;
  category_name: string | null;
  total_expense: number | null;
  transactions: number | null;
};

type TimelineRow = RowDataPacket & {
  date: string;
  income: number | null;
  expense: number | null;
  net: number | null;
  transactions: number | null;
};

type ListRow = RowDataPacket & {
  id: string;
  description: string | null;
  value: number | null;
  date: string | null;
  created_at: string | null;
  sync_status: SyncStatus | null;
  sync_error: string | null;
  batch_id: string | null;
  account_name: string | null;
  category_name: string | null;
};

type AnomalyRow = RowDataPacket & {
  category_id: string | null;
  category_name: string | null;
  recent_total: number | null;
  baseline_total: number | null;
};

type CreditCardTotalsRow = RowDataPacket & {
  total_expense: number | null;
  transactions: number | null;
};

type CreditCardAccountRow = RowDataPacket & {
  account_id: string | null;
  account_name: string | null;
  payment_name: string | null;
  total_expense: number | null;
  transactions: number | null;
};

type FeedbackTotalsRow = RowDataPacket & {
  total: number | null;
  pending: number | null;
  validated: number | null;
  corrected: number | null;
  used_for_training: number | null;
};

type FeedbackDailyRow = RowDataPacket & {
  date: string;
  total: number | null;
  corrected: number | null;
};

type FeedbackListRow = RowDataPacket & {
  id: string;
  originalText: string | null;
  predictedJson: unknown;
  userCorrectedJson: unknown;
  status: FeedbackStatus | null;
  usedForTraining: number | boolean | null;
  created_at: string | null;
  updated_at: string | null;
};

type FeedbackQualityRow = RowDataPacket & {
  total_compared: number | null;
  intent_matches: number | null;
  category_matches: number | null;
  account_matches: number | null;
  value_matches: number | null;
};

type FeedbackQueueStatusRow = RowDataPacket & {
  status: FeedbackStatus | null;
  total: number | null;
};

const tableCache = new Map<string, boolean>();

const columnCache = new Map<string, boolean>();

function toNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function escapeSqlString(value: string) {
  return value.replace(/'/g, "''");
}

function normalizeSqlText(expression: string) {
  const replacements: Array<[string, string]> = [
    ['á', 'a'],
    ['à', 'a'],
    ['ã', 'a'],
    ['â', 'a'],
    ['ä', 'a'],
    ['é', 'e'],
    ['è', 'e'],
    ['ê', 'e'],
    ['ë', 'e'],
    ['í', 'i'],
    ['ì', 'i'],
    ['î', 'i'],
    ['ï', 'i'],
    ['ó', 'o'],
    ['ò', 'o'],
    ['õ', 'o'],
    ['ô', 'o'],
    ['ö', 'o'],
    ['ú', 'u'],
    ['ù', 'u'],
    ['û', 'u'],
    ['ü', 'u'],
    ['ç', 'c'],
    ['?', ''],
  ];

  let normalized = `lower(trim(coalesce(${expression}, '')))`;
  for (const [from, to] of replacements) {
    normalized = `replace(${normalized}, '${escapeSqlString(from)}', '${escapeSqlString(to)}')`;
  }

  return normalized;
}

function jsonText(pathSource: string, jsonPath: string) {
  return `json_unquote(json_extract(${pathSource}, '${escapeSqlString(jsonPath)}'))`;
}

async function hasColumn(pool: Pool, tableName: string, columnName: string) {
  const cacheKey = `${tableName}:${columnName}`;
  const cached = columnCache.get(cacheKey);
  if (cached !== undefined) return cached;

  const [rows] = await pool.query<RowDataPacket[]>(
    `
      select count(*) as total
      from information_schema.columns
      where table_schema = database()
        and table_name = ?
        and column_name = ?
    `,
    [tableName, columnName]
  );

  const exists = toNumber(rows[0]?.total) > 0;
  columnCache.set(cacheKey, exists);
  return exists;
}

async function hasTable(pool: Pool, tableName: string) {
  const cached = tableCache.get(tableName);
  if (cached !== undefined) return cached;

  const [rows] = await pool.query<RowDataPacket[]>(
    `
      select count(*) as total
      from information_schema.tables
      where table_schema = database()
        and table_name = ?
    `,
    [tableName]
  );

  const exists = toNumber(rows[0]?.total) > 0;
  tableCache.set(tableName, exists);
  return exists;
}

async function hasTransactionColumn(pool: Pool, columnName: string) {
  return hasColumn(pool, 'bk_tb_transactions', columnName);
}

async function hasCategoryColumn(pool: Pool, columnName: string) {
  return hasColumn(pool, 'bk_tb_categories', columnName);
}

async function hasFeedbackColumn(pool: Pool, columnName: string) {
  return hasColumn(pool, 'bk_nlp_feedback', columnName);
}

export async function getOverview(pool: Pool, params: OverviewParams) {
  const [hasSyncStatus, hasIntegrated] = await Promise.all([
    hasTransactionColumn(pool, 'sync_status'),
    hasTransactionColumn(pool, 'integrated'),
  ]);

  const pendingExpression = hasSyncStatus
    ? "coalesce(sum(case when sync_status in ('pending', 'processing', 'error') then 1 else 0 end), 0)"
    : hasIntegrated
      ? 'coalesce(sum(case when coalesce(integrated, 0) = 0 then 1 else 0 end), 0)'
      : '0';

  const [totalsRows] = await pool.query<TotalsRow[]>(
    `
      select
        coalesce(sum(case when c.positive = 1 then t.value else 0 end), 0) as income,
        coalesce(sum(case when c.positive = 0 then t.value else 0 end), 0) as expense,
        coalesce(count(*), 0) as transactions
      from bk_tb_transactions t
      inner join bk_tb_categories c on c.id = t.category
      where t.owner = ?
        and date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
    `,
    [params.ownerId, params.days]
  );

  const [pendingRows] = await pool.query<PendingRow[]>(
    `
      select
        ${pendingExpression} as pending_sync
      from bk_tb_transactions
      where owner = ?
    `,
    [params.ownerId]
  );

  const [topCategoryRows] = await pool.query<TopCategoryRow[]>(
    `
      select
        c.id as category_id,
        c.name as category_name,
        coalesce(sum(t.value), 0) as total_expense,
        count(*) as transactions
      from bk_tb_transactions t
      inner join bk_tb_categories c on c.id = t.category
      where t.owner = ?
        and c.positive = 0
        and date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
      group by c.id, c.name
      order by total_expense desc
      limit 5
    `,
    [params.ownerId, params.days]
  );

  const totals = totalsRows[0];
  const pending = pendingRows[0];
  const income = toNumber(totals?.income);
  const expense = toNumber(totals?.expense);

  return {
    periodDays: params.days,
    income,
    expense,
    net: income - expense,
    transactions: toNumber(totals?.transactions),
    pendingSync: toNumber(pending?.pending_sync),
    topExpenseCategories: topCategoryRows.map((row) => ({
      categoryId: row.category_id ?? 'unknown',
      categoryName: row.category_name ?? 'Sem categoria',
      totalExpense: toNumber(row.total_expense),
      transactions: toNumber(row.transactions),
    })),
  };
}

export async function getTimeline(pool: Pool, params: TimelineParams) {
  const [rows] = await pool.query<TimelineRow[]>(
    `
      select
        date(coalesce(t.date, t.created_at, current_timestamp)) as date,
        coalesce(sum(case when c.positive = 1 then t.value else 0 end), 0) as income,
        coalesce(sum(case when c.positive = 0 then t.value else 0 end), 0) as expense,
        coalesce(sum(case when c.positive = 1 then t.value else -1 * t.value end), 0) as net,
        count(*) as transactions
      from bk_tb_transactions t
      inner join bk_tb_categories c on c.id = t.category
      where t.owner = ?
        and date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
      group by date(coalesce(t.date, t.created_at, current_timestamp))
      order by date asc
    `,
    [params.ownerId, params.days]
  );

  return {
    periodDays: params.days,
    points: rows.map((row) => ({
      date: row.date,
      income: toNumber(row.income),
      expense: toNumber(row.expense),
      net: toNumber(row.net),
      transactions: toNumber(row.transactions),
    })),
  };
}

export async function listTransactions(pool: Pool, params: ListTransactionsParams) {
  const [hasSyncStatus, hasSyncError] = await Promise.all([
    hasTransactionColumn(pool, 'sync_status'),
    hasTransactionColumn(pool, 'sync_error'),
  ]);

  const conditions: string[] = [
    't.owner = ?',
    "date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)",
  ];
  const values: unknown[] = [params.ownerId, params.days];

  if (params.status !== 'all' && hasSyncStatus) {
    conditions.push('t.sync_status = ?');
    values.push(params.status);
  }

  if (params.search && params.search.trim().length > 0) {
    conditions.push(
      "(lower(coalesce(t.description, '')) like ? or lower(coalesce(c.name, '')) like ? or lower(coalesce(a.name, '')) like ? or lower(coalesce(t.id, '')) like ?)"
    );
    const pattern = `%${params.search.trim().toLowerCase()}%`;
    values.push(pattern, pattern, pattern, pattern);
  }

  values.push(params.limit, params.offset);

  const query = `
    select
      t.id,
      t.description,
      t.value,
      coalesce(t.date, t.created_at, current_timestamp) as date,
      t.created_at,
      ${hasSyncStatus ? 't.sync_status' : "'pending'"} as sync_status,
      ${hasSyncError ? 't.sync_error' : 'null'} as sync_error,
      t.batch_id,
      a.name as account_name,
      c.name as category_name
    from bk_tb_transactions t
    left join bk_tb_accounts a on a.id = t.account
    left join bk_tb_categories c on c.id = t.category
    where ${conditions.join(' and ')}
    order by date desc, t.created_at desc
    limit ? offset ?
  `;

  const [rows] = await pool.query<ListRow[]>(query, values);

  return {
    periodDays: params.days,
    status: params.status,
    search: params.search ?? null,
    limit: params.limit,
    offset: params.offset,
    transactions: rows.map((row) => ({
      id: row.id,
      description: row.description ?? '',
      value: toNumber(row.value),
      date: row.date,
      createdAt: row.created_at,
      syncStatus: row.sync_status ?? 'pending',
      syncError: row.sync_error,
      batchId: row.batch_id,
      accountName: row.account_name ?? 'Conta desconhecida',
      categoryName: row.category_name ?? 'Categoria desconhecida',
    })),
  };
}

export async function getCreditCardSpending(pool: Pool, params: CreditCardSpendingParams) {
  const hasCategoryInternal = await hasCategoryColumn(pool, 'internal');
  const internalFilter = hasCategoryInternal ? 'and coalesce(c.internal, 0) = 0' : '';
  const cardPaymentFilter =
    "(lower(coalesce(p.name, '')) like '%credito%' or lower(coalesce(p.name, '')) like '%credit%' or lower(coalesce(p.name, '')) like '%cartao%')";

  const [totalsRows] = await pool.query<CreditCardTotalsRow[]>(
    `
      select
        coalesce(sum(t.value), 0) as total_expense,
        count(*) as transactions
      from bk_tb_transactions t
      inner join bk_tb_accounts a on a.id = t.account
      inner join bk_tb_payments p on p.id = a.paymentType
      inner join bk_tb_categories c on c.id = t.category
      where t.owner = ?
        and c.positive = 0
        ${internalFilter}
        and ${cardPaymentFilter}
        and date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
    `,
    [params.ownerId, params.days]
  );

  const [accountsRows] = await pool.query<CreditCardAccountRow[]>(
    `
      select
        a.id as account_id,
        a.name as account_name,
        p.name as payment_name,
        coalesce(sum(t.value), 0) as total_expense,
        count(*) as transactions
      from bk_tb_transactions t
      inner join bk_tb_accounts a on a.id = t.account
      inner join bk_tb_payments p on p.id = a.paymentType
      inner join bk_tb_categories c on c.id = t.category
      where t.owner = ?
        and c.positive = 0
        ${internalFilter}
        and ${cardPaymentFilter}
        and date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
      group by a.id, a.name, p.name
      order by total_expense desc
      limit ?
    `,
    [params.ownerId, params.days, params.limit]
  );

  const totals = totalsRows[0];

  return {
    periodDays: params.days,
    totalExpense: toNumber(totals?.total_expense),
    transactions: toNumber(totals?.transactions),
    accounts: accountsRows.map((row) => ({
      accountId: row.account_id ?? 'unknown',
      accountName: row.account_name ?? 'Conta desconhecida',
      paymentName: row.payment_name ?? 'Forma de pagamento desconhecida',
      totalExpense: toNumber(row.total_expense),
      transactions: toNumber(row.transactions),
    })),
  };
}

export async function detectExpenseAnomalies(pool: Pool, params: AnomalyParams) {
  const [rows] = await pool.query<AnomalyRow[]>(
    `
      select
        c.id as category_id,
        c.name as category_name,
        coalesce(sum(case
          when date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
          then t.value
          else 0
        end), 0) as recent_total,
        coalesce(sum(case
          when date(coalesce(t.date, t.created_at, current_timestamp)) < date_sub(curdate(), interval ? day)
            and date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
          then t.value
          else 0
        end), 0) as baseline_total
      from bk_tb_transactions t
      inner join bk_tb_categories c on c.id = t.category
      where t.owner = ?
        and c.positive = 0
        and date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)
      group by c.id, c.name
    `,
    [
      params.recentDays,
      params.recentDays,
      params.baselineDays,
      params.ownerId,
      params.baselineDays,
    ]
  );

  const historicalDays = Math.max(1, params.baselineDays - params.recentDays);
  const anomalies = rows
    .map((row) => {
      const recentDaily = toNumber(row.recent_total) / Math.max(1, params.recentDays);
      const baselineDaily = toNumber(row.baseline_total) / historicalDays;
      const deltaAbsolute = recentDaily - baselineDaily;
      const deltaPercent = baselineDaily > 0 ? (deltaAbsolute / baselineDaily) * 100 : 0;

      return {
        categoryId: row.category_id ?? 'unknown',
        categoryName: row.category_name ?? 'Sem categoria',
        recentDaily,
        baselineDaily,
        deltaAbsolute,
        deltaPercent,
      };
    })
    .filter(
      (item) =>
        item.baselineDaily > 0 &&
        item.deltaPercent >= params.minIncreasePercent &&
        item.deltaAbsolute >= params.minAbsoluteDelta
    )
    .sort((a, b) => b.deltaPercent - a.deltaPercent)
    .slice(0, params.limit);

  return {
    period: {
      recentDays: params.recentDays,
      baselineDays: params.baselineDays,
    },
    thresholds: {
      minIncreasePercent: params.minIncreasePercent,
      minAbsoluteDelta: params.minAbsoluteDelta,
    },
    anomalies,
  };
}

function buildFeedbackFilters(
  hasOwner: boolean,
  params: {
    ownerId?: string;
    days: number;
    status?: 'all' | FeedbackStatus;
    usedForTraining?: boolean;
    search?: string;
  }
) {
  const conditions: string[] = [
    'date(coalesce(f.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)',
  ];
  const values: unknown[] = [params.days];

  if (hasOwner && params.ownerId) {
    conditions.push('f.owner = ?');
    values.push(params.ownerId);
  }

  if (params.status && params.status !== 'all') {
    conditions.push('f.status = ?');
    values.push(params.status);
  }

  if (typeof params.usedForTraining === 'boolean') {
    conditions.push('coalesce(f.usedForTraining, 0) = ?');
    values.push(params.usedForTraining ? 1 : 0);
  }

  if (params.search && params.search.trim().length > 0) {
    conditions.push("lower(coalesce(f.originalText, '')) like ?");
    values.push(`%${params.search.trim().toLowerCase()}%`);
  }

  return { conditions, values };
}

export async function getFeedbackOverview(pool: Pool, params: FeedbackOverviewParams) {
  const feedbackTableExists = await hasTable(pool, 'bk_nlp_feedback');
  if (!feedbackTableExists) {
    throw new Error('Feedback table bk_nlp_feedback not found in the connected database.');
  }

  const hasOwner = await hasFeedbackColumn(pool, 'owner');
  const { conditions, values } = buildFeedbackFilters(hasOwner, {
    ownerId: params.ownerId,
    days: params.days,
  });

  const [totalsRows] = await pool.query<FeedbackTotalsRow[]>(
    `
      select
        count(*) as total,
        coalesce(sum(case when f.status = 'pending' then 1 else 0 end), 0) as pending,
        coalesce(sum(case when f.status = 'validated' then 1 else 0 end), 0) as validated,
        coalesce(sum(case when f.status = 'corrected' then 1 else 0 end), 0) as corrected,
        coalesce(sum(case when coalesce(f.usedForTraining, 0) = 1 then 1 else 0 end), 0) as used_for_training
      from bk_nlp_feedback f
      where ${conditions.join(' and ')}
    `,
    values
  );

  const [dailyRows] = await pool.query<FeedbackDailyRow[]>(
    `
      select
        date(coalesce(f.created_at, current_timestamp)) as date,
        count(*) as total,
        coalesce(sum(case when f.status = 'corrected' then 1 else 0 end), 0) as corrected
      from bk_nlp_feedback f
      where ${conditions.join(' and ')}
      group by date(coalesce(f.created_at, current_timestamp))
      order by date asc
    `,
    values
  );

  const totals = totalsRows[0];
  const total = toNumber(totals?.total);
  const corrected = toNumber(totals?.corrected);
  const usedForTraining = toNumber(totals?.used_for_training);

  return {
    periodDays: params.days,
    total,
    pending: toNumber(totals?.pending),
    validated: toNumber(totals?.validated),
    corrected,
    usedForTraining,
    correctedRate: total > 0 ? corrected / total : 0,
    trainingCoverageRate: total > 0 ? usedForTraining / total : 0,
    daily: dailyRows.map((row) => ({
      date: row.date,
      total: toNumber(row.total),
      corrected: toNumber(row.corrected),
    })),
  };
}

export async function listFeedbacks(pool: Pool, params: ListFeedbackParams) {
  const feedbackTableExists = await hasTable(pool, 'bk_nlp_feedback');
  if (!feedbackTableExists) {
    throw new Error('Feedback table bk_nlp_feedback not found in the connected database.');
  }

  const hasOwner = await hasFeedbackColumn(pool, 'owner');
  const { conditions, values } = buildFeedbackFilters(hasOwner, params);
  const queryValues = [...values, params.limit, params.offset];

  const [rows] = await pool.query<FeedbackListRow[]>(
    `
      select
        f.id,
        f.originalText,
        f.predictedJson,
        f.userCorrectedJson,
        f.status,
        f.usedForTraining,
        f.created_at,
        f.updated_at
      from bk_nlp_feedback f
      where ${conditions.join(' and ')}
      order by f.created_at desc
      limit ? offset ?
    `,
    queryValues
  );

  return {
    periodDays: params.days,
    status: params.status,
    usedForTraining: params.usedForTraining ?? null,
    search: params.search ?? null,
    limit: params.limit,
    offset: params.offset,
    feedbacks: rows.map((row) => ({
      id: row.id,
      originalText: row.originalText ?? '',
      predictedJson: row.predictedJson ?? null,
      userCorrectedJson: row.userCorrectedJson ?? null,
      status: row.status ?? 'pending',
      usedForTraining: Boolean(row.usedForTraining),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    })),
  };
}

export async function getFeedbackQuality(pool: Pool, params: FeedbackQualityParams) {
  const feedbackTableExists = await hasTable(pool, 'bk_nlp_feedback');
  if (!feedbackTableExists) {
    throw new Error('Feedback table bk_nlp_feedback not found in the connected database.');
  }

  const hasOwner = await hasFeedbackColumn(pool, 'owner');
  const { conditions, values } = buildFeedbackFilters(hasOwner, {
    ownerId: params.ownerId,
    days: params.days,
  });
  conditions.push('f.userCorrectedJson is not null');

  const predictedIntent = normalizeSqlText(jsonText('f.predictedJson', '$.intent'));
  const correctedIntent = normalizeSqlText(jsonText('f.userCorrectedJson', '$.intent'));
  const predictedCategory = normalizeSqlText(jsonText('f.predictedJson', '$.category'));
  const correctedCategory = normalizeSqlText(jsonText('f.userCorrectedJson', '$.category'));
  const predictedAccount = normalizeSqlText(jsonText('f.predictedJson', '$.account'));
  const correctedAccount = normalizeSqlText(jsonText('f.userCorrectedJson', '$.account'));
  const predictedOrigin = normalizeSqlText(jsonText('f.predictedJson', '$.origin'));
  const correctedOrigin = normalizeSqlText(jsonText('f.userCorrectedJson', '$.origin'));
  const predictedDestiny = normalizeSqlText(jsonText('f.predictedJson', '$.destiny'));
  const correctedDestiny = normalizeSqlText(jsonText('f.userCorrectedJson', '$.destiny'));
  const predictedValue = `coalesce(${jsonText('f.predictedJson', '$.value')}, '')`;
  const correctedValue = `coalesce(${jsonText('f.userCorrectedJson', '$.value')}, '')`;
  const accountMatch = `
    case
      when ${predictedIntent} = 'transfer' and ${correctedIntent} = 'transfer'
      then case when ${predictedOrigin} = ${correctedOrigin} and ${predictedDestiny} = ${correctedDestiny} then 1 else 0 end
      when ${predictedIntent} = 'transfer' or ${correctedIntent} = 'transfer'
      then 0
      when ${predictedAccount} = ${correctedAccount}
      then 1 else 0
    end
  `;

  const [rows] = await pool.query<FeedbackQualityRow[]>(
    `
      select
        count(*) as total_compared,
        coalesce(sum(case
          when ${predictedIntent} = ${correctedIntent}
          then 1 else 0 end), 0) as intent_matches,
        coalesce(sum(case
          when ${predictedCategory} = ${correctedCategory}
          then 1 else 0 end), 0) as category_matches,
        coalesce(sum(${accountMatch}), 0) as account_matches,
        coalesce(sum(case
          when ${predictedValue} = ${correctedValue}
          then 1 else 0 end), 0) as value_matches
      from bk_nlp_feedback f
      where ${conditions.join(' and ')}
    `,
    values
  );

  const quality = rows[0];
  const totalCompared = toNumber(quality?.total_compared);
  const intentMatches = toNumber(quality?.intent_matches);
  const categoryMatches = toNumber(quality?.category_matches);
  const accountMatches = toNumber(quality?.account_matches);
  const valueMatches = toNumber(quality?.value_matches);

  const safeRate = (matches: number) => (totalCompared > 0 ? matches / totalCompared : 0);

  return {
    periodDays: params.days,
    totalCompared,
    fields: {
      intent: { matches: intentMatches, accuracy: safeRate(intentMatches) },
      category: { matches: categoryMatches, accuracy: safeRate(categoryMatches) },
      account: { matches: accountMatches, accuracy: safeRate(accountMatches) },
      value: { matches: valueMatches, accuracy: safeRate(valueMatches) },
    },
  };
}

export async function getFeedbackTrainingQueue(pool: Pool, params: FeedbackTrainingQueueParams) {
  const feedbackTableExists = await hasTable(pool, 'bk_nlp_feedback');
  if (!feedbackTableExists) {
    throw new Error('Feedback table bk_nlp_feedback not found in the connected database.');
  }

  const hasOwner = await hasFeedbackColumn(pool, 'owner');
  const { conditions, values } = buildFeedbackFilters(hasOwner, {
    ownerId: params.ownerId,
    days: params.days,
  });
  conditions.push("f.status <> 'pending'");
  conditions.push('coalesce(f.usedForTraining, 0) = 0');

  const [countRows] = await pool.query<RowDataPacket[]>(
    `
      select count(*) as total
      from bk_nlp_feedback f
      where ${conditions.join(' and ')}
    `,
    values
  );

  const [statusRows] = await pool.query<FeedbackQueueStatusRow[]>(
    `
      select
        f.status,
        count(*) as total
      from bk_nlp_feedback f
      where ${conditions.join(' and ')}
      group by f.status
    `,
    values
  );

  const [sampleRows] = await pool.query<FeedbackListRow[]>(
    `
      select
        f.id,
        f.originalText,
        f.predictedJson,
        f.userCorrectedJson,
        f.status,
        f.usedForTraining,
        f.created_at,
        f.updated_at
      from bk_nlp_feedback f
      where ${conditions.join(' and ')}
      order by f.created_at desc
      limit ?
    `,
    [...values, params.limit]
  );

  return {
    periodDays: params.days,
    pendingTrainingTotal: toNumber(countRows[0]?.total),
    byStatus: statusRows.map((row) => ({
      status: row.status ?? 'pending',
      total: toNumber(row.total),
    })),
    samples: sampleRows.map((row) => ({
      id: row.id,
      originalText: row.originalText ?? '',
      predictedJson: row.predictedJson ?? null,
      userCorrectedJson: row.userCorrectedJson ?? null,
      status: row.status ?? 'pending',
      createdAt: row.created_at,
    })),
  };
}
