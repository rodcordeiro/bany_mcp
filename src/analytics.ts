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

function toNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export async function getOverview(pool: Pool, params: OverviewParams) {
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
        coalesce(sum(case when sync_status in ('pending', 'processing', 'error') then 1 else 0 end), 0) as pending_sync
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
  const conditions: string[] = [
    't.owner = ?',
    "date(coalesce(t.date, t.created_at, current_timestamp)) >= date_sub(curdate(), interval ? day)",
  ];
  const values: unknown[] = [params.ownerId, params.days];

  if (params.status !== 'all') {
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
      t.sync_status,
      t.sync_error,
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
