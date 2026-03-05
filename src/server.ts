import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import type { Pool } from 'mysql2/promise';
import { z } from 'zod';

import type { AppConfig } from './config.js';
import {
  detectExpenseAnomalies,
  getFeedbackOverview,
  getFeedbackQuality,
  getFeedbackTrainingQueue,
  getCreditCardSpending,
  getOverview,
  getTimeline,
  listFeedbacks,
  listTransactions,
  type FeedbackStatus,
  type SyncStatus,
} from './analytics.js';

const statusEnum = z.enum(['all', 'pending', 'processing', 'done', 'error']);
const feedbackStatusEnum = z.enum(['all', 'pending', 'validated', 'corrected']);

const overviewInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
});

const timelineInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
});

const listInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
  status: statusEnum.optional(),
  search: z.string().optional(),
  limit: z.number().int().min(1).max(200).optional(),
  offset: z.number().int().min(0).optional(),
});

const anomaliesInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  recentDays: z.number().int().min(1).max(60).optional(),
  baselineDays: z.number().int().min(7).max(365).optional(),
  minIncreasePercent: z.number().min(1).max(1000).optional(),
  minAbsoluteDelta: z.number().min(0).max(1_000_000).optional(),
  limit: z.number().int().min(1).max(50).optional(),
});

const creditCardSpendingInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
  limit: z.number().int().min(1).max(100).optional(),
});

const feedbackOverviewInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
});

const feedbackListInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
  status: feedbackStatusEnum.optional(),
  usedForTraining: z.boolean().optional(),
  search: z.string().optional(),
  limit: z.number().int().min(1).max(200).optional(),
  offset: z.number().int().min(0).optional(),
});

const feedbackQualityInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
});

const feedbackTrainingQueueInputSchema = z.object({
  ownerId: z.string().min(1).optional(),
  days: z.number().int().min(1).max(365).optional(),
  limit: z.number().int().min(1).max(100).optional(),
});

function parseArgs(args: unknown): Record<string, unknown> {
  if (args && typeof args === 'object' && !Array.isArray(args)) {
    return args as Record<string, unknown>;
  }
  return {};
}

function resolveOwnerId(ownerId: string | undefined, config: AppConfig): string {
  const resolved = ownerId ?? config.DEFAULT_OWNER_ID;
  if (!resolved) {
    throw new Error(
      'ownerId is required. Provide ownerId in the tool call or define DEFAULT_OWNER_ID.'
    );
  }
  return resolved;
}

function asTextResult(data: unknown) {
  return {
    content: [
      {
        type: 'text' as const,
        text: JSON.stringify(data, null, 2),
      },
    ],
  };
}

function asErrorResult(message: string) {
  return {
    isError: true,
    content: [
      {
        type: 'text' as const,
        text: message,
      },
    ],
  };
}

export async function startMcpServer(pool: Pool, config: AppConfig) {
  const server = new Server(
    {
      name: config.MCP_SERVER_NAME,
      version: config.MCP_SERVER_VERSION,
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => {
    return {
      tools: [
        {
          name: 'transactions_overview',
          description: 'Returns summary metrics for income, expense, net, sync status and top categories.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'transactions_timeline',
          description: 'Returns daily timeline points for income, expense and net.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'transactions_list',
          description: 'Returns transaction list with pagination and optional filters.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
              status: { type: 'string', enum: ['all', 'pending', 'processing', 'done', 'error'] },
              search: { type: 'string' },
              limit: { type: 'number', minimum: 1, maximum: 200 },
              offset: { type: 'number', minimum: 0 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'transactions_detect_anomalies',
          description: 'Detects expense anomalies comparing recent period against baseline period.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              recentDays: { type: 'number', minimum: 1, maximum: 60 },
              baselineDays: { type: 'number', minimum: 7, maximum: 365 },
              minIncreasePercent: { type: 'number', minimum: 1, maximum: 1000 },
              minAbsoluteDelta: { type: 'number', minimum: 0, maximum: 1000000 },
              limit: { type: 'number', minimum: 1, maximum: 50 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'transactions_credit_card_spending',
          description: 'Returns total expense on credit card accounts in the selected period.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
              limit: { type: 'number', minimum: 1, maximum: 100 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'feedbacks_overview',
          description:
            'Returns feedback volume, status distribution, training usage and daily trend for the selected period.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'feedbacks_list',
          description: 'Returns paginated feedback list with filters by status, training usage and search text.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
              status: { type: 'string', enum: ['all', 'pending', 'validated', 'corrected'] },
              usedForTraining: { type: 'boolean' },
              search: { type: 'string' },
              limit: { type: 'number', minimum: 1, maximum: 200 },
              offset: { type: 'number', minimum: 0 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'feedbacks_quality',
          description:
            'Compares predicted and corrected feedback fields to estimate model quality by intent, category, account and value.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
            },
            additionalProperties: false,
          },
        },
        {
          name: 'feedbacks_training_queue',
          description:
            'Returns feedbacks eligible for training (non-pending and not usedForTraining), including status summary and samples.',
          inputSchema: {
            type: 'object',
            properties: {
              ownerId: { type: 'string' },
              days: { type: 'number', minimum: 1, maximum: 365 },
              limit: { type: 'number', minimum: 1, maximum: 100 },
            },
            additionalProperties: false,
          },
        },
      ],
    };
  });

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    try {
      const name = request.params.name;
      const args = parseArgs(request.params.arguments);

      if (name === 'transactions_overview') {
        const input = overviewInputSchema.parse(args);
        const ownerId = resolveOwnerId(input.ownerId, config);
        const days = input.days ?? config.DEFAULT_LOOKBACK_DAYS;
        const data = await getOverview(pool, { ownerId, days });
        return asTextResult(data);
      }

      if (name === 'transactions_timeline') {
        const input = timelineInputSchema.parse(args);
        const ownerId = resolveOwnerId(input.ownerId, config);
        const days = input.days ?? config.DEFAULT_LOOKBACK_DAYS;
        const data = await getTimeline(pool, { ownerId, days });
        return asTextResult(data);
      }

      if (name === 'transactions_list') {
        const input = listInputSchema.parse(args);
        const ownerId = resolveOwnerId(input.ownerId, config);
        const data = await listTransactions(pool, {
          ownerId,
          days: input.days ?? config.DEFAULT_LOOKBACK_DAYS,
          status: (input.status ?? 'all') as 'all' | SyncStatus,
          search: input.search,
          limit: input.limit ?? 50,
          offset: input.offset ?? 0,
        });
        return asTextResult(data);
      }

      if (name === 'transactions_detect_anomalies') {
        const input = anomaliesInputSchema.parse(args);
        const ownerId = resolveOwnerId(input.ownerId, config);
        const recentDays = input.recentDays ?? 7;
        const baselineDays = input.baselineDays ?? 30;

        if (baselineDays <= recentDays) {
          return asErrorResult('baselineDays must be greater than recentDays.');
        }

        const data = await detectExpenseAnomalies(pool, {
          ownerId,
          recentDays,
          baselineDays,
          minIncreasePercent: input.minIncreasePercent ?? 40,
          minAbsoluteDelta: input.minAbsoluteDelta ?? 10,
          limit: input.limit ?? 10,
        });
        return asTextResult(data);
      }

      if (name === 'transactions_credit_card_spending') {
        const input = creditCardSpendingInputSchema.parse(args);
        const ownerId = resolveOwnerId(input.ownerId, config);
        const data = await getCreditCardSpending(pool, {
          ownerId,
          days: input.days ?? config.DEFAULT_LOOKBACK_DAYS,
          limit: input.limit ?? 10,
        });
        return asTextResult(data);
      }

      if (name === 'feedbacks_overview') {
        const input = feedbackOverviewInputSchema.parse(args);
        const data = await getFeedbackOverview(pool, {
          ownerId: input.ownerId ?? config.DEFAULT_OWNER_ID,
          days: input.days ?? config.DEFAULT_LOOKBACK_DAYS,
        });
        return asTextResult(data);
      }

      if (name === 'feedbacks_list') {
        const input = feedbackListInputSchema.parse(args);
        const data = await listFeedbacks(pool, {
          ownerId: input.ownerId ?? config.DEFAULT_OWNER_ID,
          days: input.days ?? config.DEFAULT_LOOKBACK_DAYS,
          status: (input.status ?? 'all') as 'all' | FeedbackStatus,
          usedForTraining: input.usedForTraining,
          search: input.search,
          limit: input.limit ?? 50,
          offset: input.offset ?? 0,
        });
        return asTextResult(data);
      }

      if (name === 'feedbacks_quality') {
        const input = feedbackQualityInputSchema.parse(args);
        const data = await getFeedbackQuality(pool, {
          ownerId: input.ownerId ?? config.DEFAULT_OWNER_ID,
          days: input.days ?? config.DEFAULT_LOOKBACK_DAYS,
        });
        return asTextResult(data);
      }

      if (name === 'feedbacks_training_queue') {
        const input = feedbackTrainingQueueInputSchema.parse(args);
        const data = await getFeedbackTrainingQueue(pool, {
          ownerId: input.ownerId ?? config.DEFAULT_OWNER_ID,
          days: input.days ?? config.DEFAULT_LOOKBACK_DAYS,
          limit: input.limit ?? 20,
        });
        return asTextResult(data);
      }

      return asErrorResult(`Unknown tool: ${name}`);
    } catch (error) {
      if (error instanceof z.ZodError) {
        return asErrorResult(`Invalid input: ${JSON.stringify(error.issues, null, 2)}`);
      }

      return asErrorResult(error instanceof Error ? error.message : 'Unexpected error');
    }
  });

  const transport = new StdioServerTransport();
  await server.connect(transport);
}
