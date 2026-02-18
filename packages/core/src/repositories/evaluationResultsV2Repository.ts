import {
  isAfter,
  isBefore,
  isToday,
  parseJSON,
  startOfDay,
  subDays,
} from 'date-fns'
import {
  and,
  asc,
  count,
  desc,
  eq,
  getTableColumns,
  gt,
  gte,
  inArray,
  isNotNull,
  isNull,
  lte,
  sql,
} from 'drizzle-orm'
import {
  EvaluationResultV2,
  EvaluationType,
  ISSUE_GENERATION_MAX_RESULTS,
  ISSUE_GENERATION_RECENCY_DAYS,
  ISSUE_GENERATION_RECENCY_RATIO,
  LogSources,
  Span,
} from '../constants'
import { EvaluationResultsV2Search } from '../helpers'
import { NotFoundError } from '../lib/errors'
import { calculateOffset } from '../lib/pagination/index'
import { Result } from '../lib/Result'
import { commits } from '../schema/models/commits'
import { datasetRows } from '../schema/models/datasetRows'
import { datasets } from '../schema/models/datasets'
import { evaluationResultsV2 } from '../schema/models/evaluationResultsV2'
import { issueEvaluationResults } from '../schema/models/issueEvaluationResults'
import { Commit } from '../schema/models/types/Commit'
import { Issue } from '../schema/models/types/Issue'
import { Workspace } from '../schema/models/types/Workspace'
import {
  EvaluationResultV2WithDetails,
  EvaluationV2Stats,
  ResultWithEvaluationV2,
} from '../schema/types'
import { CommitsRepository } from './commitsRepository'
import { EvaluationsV2Repository } from './evaluationsV2Repository'
import { findLastActiveAssignedIssue } from '../queries/issueEvaluationResults/findLastActiveAssignedIssue'
import Repository from './repositoryV2'
import { SpansRepository } from './spansRepository'
import { spans } from '../schema/models/spans'
import { Database, database } from '../client'
import { clickhouseClient } from '../client/clickhouse'
import { isClickHouseEvaluationResultsReadEnabled } from '../services/workspaceFeatures/isClickHouseEvaluationResultsReadEnabled'
import { captureException } from '../utils/datadogCapture'
import { findEvaluationResultByUuid } from '../queries/clickhouse/evaluationResultsV2/findByUuid'
import { listEvaluationResultsByEvaluation } from '../queries/clickhouse/evaluationResultsV2/listByEvaluation'
import { countEvaluationResultsByEvaluation } from '../queries/clickhouse/evaluationResultsV2/countByEvaluation'
import { listEvaluationResultsByTraceIds } from '../queries/clickhouse/evaluationResultsV2/listByTraceIds'
import { listEvaluationResultsBySpanAndEvaluations } from '../queries/clickhouse/evaluationResultsV2/listBySpanAndEvaluations'
import { findEvaluationResultBySpanAndEvaluation } from '../queries/clickhouse/evaluationResultsV2/findBySpanAndEvaluation'
import {
  EvaluationResultV2Row,
  TABLE_NAME as CH_EVALUATION_RESULTS_TABLE,
} from '../schema/models/clickhouse/evaluationResults'

const tt = getTableColumns(evaluationResultsV2)

export class EvaluationResultsV2Repository extends Repository<EvaluationResultV2> {
  private clickHouseOverride: boolean | undefined

  constructor(
    workspaceId: number,
    db: Database = database,
    options?: { useClickHouse?: boolean },
  ) {
    super(workspaceId, db)
    this.clickHouseOverride = options?.useClickHouse
  }

  private async shouldUseClickHouse(): Promise<boolean> {
    if (this.clickHouseOverride !== undefined) return this.clickHouseOverride

    try {
      this.clickHouseOverride = await isClickHouseEvaluationResultsReadEnabled(
        this.workspaceId,
        this.db,
      )
    } catch (error) {
      captureException(error as Error)
      this.clickHouseOverride = false
    }

    return this.clickHouseOverride
  }

  private async mapCommitUuidsToIds(commitUuids: string[]) {
    if (!commitUuids.length) return new Map<string, number>()

    const commitsData = await this.db
      .select({ id: commits.id, uuid: commits.uuid })
      .from(commits)
      .where(inArray(commits.uuid, commitUuids))

    return new Map(commitsData.map((c) => [c.uuid, c.id]))
  }

  private mapClickHouseRowToEvaluationResult(
    row: EvaluationResultV2Row,
    commitId?: number,
  ) {
    let metadata: Record<string, unknown> | null = null
    if (row.metadata) {
      try {
        metadata = JSON.parse(row.metadata)
      } catch {
        metadata = null
      }
    }

    let error: Record<string, unknown> | null = null
    if (row.error) {
      try {
        error = JSON.parse(row.error)
      } catch {
        error = null
      }
    }

    return {
      id: row.id,
      workspaceId: row.workspace_id,
      commitId,
      evaluationUuid: row.evaluation_uuid,
      experimentId: row.experiment_id,
      datasetId: row.dataset_id,
      evaluatedRowId: row.evaluated_row_id,
      evaluatedSpanId: row.evaluated_span_id,
      evaluatedTraceId: row.evaluated_trace_id,
      type: row.type,
      metric: row.metric,
      model: row.model,
      provider: row.provider,
      score: row.score,
      normalizedScore: row.normalized_score,
      hasPassed: row.has_passed === null ? null : row.has_passed === 1,
      metadata,
      error,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      commitUuid: row.commit_uuid,
      documentUuid: row.document_uuid,
      tokens: row.tokens,
      cost: row.cost,
      uuid: (row as EvaluationResultV2Row & { uuid?: string }).uuid,
    } as unknown as EvaluationResultV2 & { commitUuid?: string }
  }

  get scopeFilter() {
    return eq(evaluationResultsV2.workspaceId, this.workspaceId)
  }

  get scope() {
    return this.db
      .select(tt)
      .from(evaluationResultsV2)
      .where(this.scopeFilter)
      .orderBy(
        desc(evaluationResultsV2.createdAt),
        desc(evaluationResultsV2.id),
      )
      .$dynamic()
  }

  async findByUuid(uuid: string) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const chResult = await findEvaluationResultByUuid(
        { workspaceId: this.workspaceId, uuid },
        this.db,
      )

      if (!chResult) {
        return Result.error(
          new NotFoundError(
            `Record with uuid ${uuid} not found in evaluation_results`,
          ),
        )
      }

      return Result.ok(chResult as unknown as EvaluationResultV2)
    }

    const result = await this.scope
      .where(and(this.scopeFilter, eq(evaluationResultsV2.uuid, uuid)))
      .limit(1)
      .then((r) => r[0])

    if (!result) {
      return Result.error(
        new NotFoundError(
          `Record with uuid ${uuid} not found in ${this.scope._.tableName}`,
        ),
      )
    }

    return Result.ok<EvaluationResultV2>(result as EvaluationResultV2)
  }

  async listBySpanAndEvaluations({
    spans,
    evaluationUuids,
    commitHistoryUuids,
  }: {
    spans: { id: string; traceId: string }[]
    evaluationUuids: string[]
    commitHistoryUuids: string[]
  }): Promise<EvaluationResultV2[]> {
    if (!spans.length || !evaluationUuids.length) return []

    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const chResults = await listEvaluationResultsBySpanAndEvaluations(
        {
          workspaceId: this.workspaceId,
          spans,
          evaluationUuids,
          commitUuids: commitHistoryUuids,
        },
        this.db,
      )

      return chResults as unknown as EvaluationResultV2[]
    }

    const spanIds = spans.map((s) => s.id)
    const traceIds = spans.map((s) => s.traceId)

    const commitHistoryIds = await this.db
      .select({ id: commits.id })
      .from(commits)
      .where(inArray(commits.uuid, commitHistoryUuids))
      .then((rows) => rows.map((r) => r.id))

    const results = await this.db
      .select(tt)
      .from(evaluationResultsV2)
      .where(
        and(
          this.scopeFilter,
          inArray(evaluationResultsV2.evaluatedSpanId, spanIds),
          inArray(evaluationResultsV2.evaluatedTraceId, traceIds),
          inArray(evaluationResultsV2.evaluationUuid, evaluationUuids),
          inArray(evaluationResultsV2.commitId, commitHistoryIds),
        ),
      )

    return results as EvaluationResultV2[]
  }

  async findByEvaluatedSpanAndEvaluation({
    evaluatedSpanId,
    evaluatedTraceId,
    evaluationUuid,
  }: {
    evaluatedSpanId: string
    evaluatedTraceId: string
    evaluationUuid: string
  }) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const chResult = await findEvaluationResultBySpanAndEvaluation(
        {
          workspaceId: this.workspaceId,
          evaluatedSpanId,
          evaluatedTraceId,
          evaluationUuid,
        },
        this.db,
      )

      return chResult as unknown as EvaluationResultV2 | undefined
    }

    return (await this.scope
      .where(
        and(
          this.scopeFilter,
          eq(evaluationResultsV2.evaluatedSpanId, evaluatedSpanId),
          eq(evaluationResultsV2.evaluatedTraceId, evaluatedTraceId),
          eq(evaluationResultsV2.evaluationUuid, evaluationUuid),
        ),
      )
      .limit(1)
      .then((r) => r[0])) as EvaluationResultV2 | undefined
  }

  private listByEvaluationFilter({
    evaluationUuid,
    params: { filters },
  }: {
    evaluationUuid: string
    params: EvaluationResultsV2Search
  }) {
    const filter = [
      this.scopeFilter,
      isNull(commits.deletedAt),
      eq(evaluationResultsV2.evaluationUuid, evaluationUuid),
    ]

    if (filters?.commitUuids !== undefined) {
      if (filters.commitUuids.length > 0) {
        filter.push(
          inArray(
            evaluationResultsV2.commitId,
            this.db
              .select({ id: commits.id })
              .from(commits)
              .where(inArray(commits.uuid, filters.commitUuids)),
          ),
        )
      } else filter.push(eq(sql`TRUE`, sql`FALSE`))
    }

    if (filters?.experimentIds !== undefined) {
      if (filters.experimentIds.length > 0) {
        filter.push(
          inArray(evaluationResultsV2.experimentId, filters.experimentIds),
        )
      } else filter.push(isNull(evaluationResultsV2.experimentId))
    }

    if (filters?.errored !== undefined) {
      if (filters.errored) filter.push(isNotNull(evaluationResultsV2.error))
      else filter.push(isNull(evaluationResultsV2.error))
    }

    if (filters?.createdAt?.from) {
      filter.push(gte(evaluationResultsV2.createdAt, filters.createdAt.from))
    }

    if (filters?.createdAt?.to) {
      filter.push(lte(evaluationResultsV2.createdAt, filters.createdAt.to))
    }

    return and(...filter)
  }

  async listByEvaluation({
    evaluationUuid,
    params,
  }: {
    evaluationUuid: string
    params: EvaluationResultsV2Search
  }) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const chResults = await listEvaluationResultsByEvaluation(
        {
          workspaceId: this.workspaceId,
          evaluationUuid,
          commitUuids: params.filters?.commitUuids,
          experimentIds: params.filters?.experimentIds,
          errored: params.filters?.errored,
          createdAtFrom: params.filters?.createdAt?.from,
          createdAtTo: params.filters?.createdAt?.to,
          limit: params.pagination.pageSize,
          offset: calculateOffset(
            params.pagination.page,
            params.pagination.pageSize,
          ),
          orderBy: params.orders?.recency === 'asc' ? 'asc' : 'desc',
        },
        this.db,
      )

      return Result.ok(chResults as unknown as EvaluationResultV2WithDetails[])
    }

    const filter = this.listByEvaluationFilter({ evaluationUuid, params })

    let query = this.db
      .select({
        ...tt,
        commit: commits,
        dataset: datasets,
        evaluatedRow: datasetRows,
      })
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .leftJoin(datasets, eq(datasets.id, evaluationResultsV2.datasetId))
      .leftJoin(
        datasetRows,
        eq(datasetRows.id, evaluationResultsV2.evaluatedRowId),
      )
      .where(filter)
      .$dynamic()

    if (params.orders?.recency === 'asc') {
      query = query.orderBy(
        asc(evaluationResultsV2.createdAt),
        asc(evaluationResultsV2.id),
      )
    }

    if (params.orders?.recency === 'desc') {
      query = query.orderBy(
        desc(evaluationResultsV2.createdAt),
        desc(evaluationResultsV2.id),
      )
    }

    query = query
      .limit(params.pagination.pageSize)
      .offset(
        calculateOffset(params.pagination.page, params.pagination.pageSize),
      )

    return Result.ok<EvaluationResultV2WithDetails[]>(
      (await query) as EvaluationResultV2WithDetails[],
    )
  }

  async countListByEvaluation({
    evaluationUuid,
    params,
  }: {
    evaluationUuid: string
    params: EvaluationResultsV2Search
  }) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const count = await countEvaluationResultsByEvaluation(
        {
          workspaceId: this.workspaceId,
          evaluationUuid,
          commitUuids: params.filters?.commitUuids,
          experimentIds: params.filters?.experimentIds,
          errored: params.filters?.errored,
          createdAtFrom: params.filters?.createdAt?.from,
          createdAtTo: params.filters?.createdAt?.to,
        },
        this.db,
      )

      return Result.ok(count)
    }

    const filter = this.listByEvaluationFilter({ evaluationUuid, params })

    const count = await this.db
      .select({
        count: sql`count(*)`.mapWith(Number).as('count'),
      })
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .where(filter)
      .then((r) => r[0]?.count ?? 0)

    return Result.ok<number>(count)
  }

  async findListByEvaluationPosition({
    evaluationUuid,
    params,
  }: {
    evaluationUuid: string
    params: EvaluationResultsV2Search
  }) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const conditions: string[] = [
        'workspace_id = {workspaceId: UInt64}',
        'evaluation_uuid = {evaluationUuid: UUID}',
      ]

      const queryParams: Record<string, unknown> = {
        workspaceId: this.workspaceId,
        evaluationUuid,
      }

      const filters = params.filters

      if (filters?.commitUuids !== undefined) {
        if (filters.commitUuids.length > 0) {
          conditions.push('commit_uuid IN ({commitUuids: Array(UUID)})')
          queryParams.commitUuids = filters.commitUuids
        } else {
          return Result.ok(undefined)
        }
      }

      if (filters?.experimentIds !== undefined) {
        if (filters.experimentIds.length > 0) {
          conditions.push(
            'experiment_id IN ({experimentIds: Array(Nullable(UInt64))})',
          )
          queryParams.experimentIds = filters.experimentIds
        } else {
          conditions.push('experiment_id IS NULL')
        }
      }

      if (filters?.errored !== undefined) {
        conditions.push(filters.errored ? 'has_error = 1' : 'has_error = 0')
      }

      if (filters?.createdAt?.from) {
        conditions.push('created_at >= {createdAtFrom: DateTime64(3)}')
        queryParams.createdAtFrom = filters.createdAt.from.toISOString()
      }

      if (filters?.createdAt?.to) {
        conditions.push('created_at <= {createdAtTo: DateTime64(3)}')
        queryParams.createdAtTo = filters.createdAt.to.toISOString()
      }

      const resultUuid = params.pagination.resultUuid
      if (!resultUuid) return Result.ok(undefined)

      const targetResult = await clickhouseClient().query({
        query: `
          SELECT id, created_at
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE workspace_id = {workspaceId: UInt64}
            AND uuid = {resultUuid: UUID}
          ORDER BY updated_at DESC
          LIMIT 1
        `,
        format: 'JSONEachRow',
        query_params: {
          workspaceId: this.workspaceId,
          resultUuid,
        },
      })

      const targetRows = await targetResult.json<{
        id: number
        created_at: string
      }>()
      const target = targetRows[0]

      if (!target) return Result.ok(undefined)

      queryParams.cursorCreatedAt = target.created_at
      queryParams.cursorId = target.id

      const recency = params.orders?.recency === 'asc' ? 'asc' : 'desc'
      conditions.push(
        recency === 'asc'
          ? '(created_at, id) <= ({cursorCreatedAt: DateTime64(3)}, {cursorId: UInt64})'
          : '(created_at, id) >= ({cursorCreatedAt: DateTime64(3)}, {cursorId: UInt64})',
      )

      const countQuery = await clickhouseClient().query({
        query: `
          SELECT count() as count
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE ${conditions.join(' AND ')}
        `,
        format: 'JSONEachRow',
        query_params: queryParams,
      })

      const countRows = await countQuery.json<{ count: number }>()
      const position = countRows[0]?.count
      if (!position) return Result.ok(undefined)

      const page = Math.ceil(position / params.pagination.pageSize)

      return Result.ok<number>(page)
    }

    const result = await this.db
      .select({
        id: evaluationResultsV2.id,
        createdAt: evaluationResultsV2.createdAt,
      })
      .from(evaluationResultsV2)
      .where(
        and(
          this.scopeFilter,
          eq(evaluationResultsV2.uuid, params.pagination.resultUuid!),
        ),
      )
      .then((r) => r[0])
    if (!result) return Result.ok(undefined)

    const filter = [this.listByEvaluationFilter({ evaluationUuid, params })]

    if (params.orders?.recency === 'asc') {
      filter.push(
        sql`(${evaluationResultsV2.createdAt}, ${evaluationResultsV2.id}) <= (${new Date(result.createdAt).toISOString()}, ${result.id})`,
      )
    }

    if (params.orders?.recency === 'desc') {
      filter.push(
        sql`(${evaluationResultsV2.createdAt}, ${evaluationResultsV2.id}) >= (${new Date(result.createdAt).toISOString()}, ${result.id})`,
      )
    }

    const position = await this.db
      .select({
        count: sql`count(*)`.mapWith(Number).as('count'),
      })
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .where(and(...filter))
      .then((r) => r[0]?.count)
    if (position === undefined) return Result.ok(undefined)

    const page = Math.ceil(position / params.pagination.pageSize)

    return Result.ok<number>(page)
  }

  async statsByEvaluation({
    projectId,
    commitUuid,
    documentUuid,
    evaluationUuid,
    params,
  }: {
    projectId?: number
    commitUuid: string
    documentUuid: string
    evaluationUuid: string
    params: EvaluationResultsV2Search
  }) {
    const useClickHouse = await this.shouldUseClickHouse()

    const evaluationsRepository = new EvaluationsV2Repository(
      this.workspaceId,
      this.db,
    )
    const evaluation = await evaluationsRepository
      .getAtCommitByDocument({
        projectId: projectId,
        commitUuid: commitUuid,
        documentUuid: documentUuid,
        evaluationUuid: evaluationUuid,
      })
      .then((r) => r.unwrap())

    if (useClickHouse) {
      const conditions: string[] = [
        'workspace_id = {workspaceId: UInt64}',
        'evaluation_uuid = {evaluationUuid: UUID}',
        'has_error = 0',
      ]

      const queryParams: Record<string, unknown> = {
        workspaceId: this.workspaceId,
        evaluationUuid,
      }

      const filters = params.filters

      if (filters?.commitUuids !== undefined) {
        if (filters.commitUuids.length > 0) {
          conditions.push('commit_uuid IN ({commitUuids: Array(UUID)})')
          queryParams.commitUuids = filters.commitUuids
        } else {
          return Result.nil()
        }
      }

      if (filters?.experimentIds !== undefined) {
        if (filters.experimentIds.length > 0) {
          conditions.push(
            'experiment_id IN ({experimentIds: Array(Nullable(UInt64))})',
          )
          queryParams.experimentIds = filters.experimentIds
        } else {
          conditions.push('experiment_id IS NULL')
        }
      }

      if (filters?.errored !== undefined) {
        conditions.push(filters.errored ? 'has_error = 1' : 'has_error = 0')
      }

      if (filters?.createdAt?.from) {
        conditions.push('created_at >= {createdAtFrom: DateTime64(3)}')
        queryParams.createdAtFrom = filters.createdAt.from.toISOString()
      }

      if (filters?.createdAt?.to) {
        conditions.push('created_at <= {createdAtTo: DateTime64(3)}')
        queryParams.createdAtTo = filters.createdAt.to.toISOString()
      }

      const now = new Date()

      const totalQuery = await clickhouseClient().query({
        query: `
          SELECT
            count() as total_results,
            avg(score) as average_score,
            ${evaluation.type === EvaluationType.Llm ? 'sum(tokens)' : '0'} as total_tokens,
            ${evaluation.type === EvaluationType.Llm ? 'sum(cost)' : '0'} as total_cost
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE ${conditions.join(' AND ')}
        `,
        format: 'JSONEachRow',
        query_params: queryParams,
      })

      const totalRows = await totalQuery.json<{
        total_results: number
        average_score: number | null
        total_tokens: number | null
        total_cost: number | null
      }>()
      const totalStats = totalRows[0]

      if (!totalStats || totalStats.total_results === 0) return Result.nil()

      const dailyQuery = await clickhouseClient().query({
        query: `
          SELECT
            toStartOfDay(created_at) as date,
            count() as total_results,
            avg(score) as average_score,
            ${evaluation.type === EvaluationType.Llm ? 'sum(tokens)' : '0'} as total_tokens,
            ${evaluation.type === EvaluationType.Llm ? 'sum(cost)' : '0'} as total_cost
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE ${conditions.join(' AND ')}
          GROUP BY date
          ORDER BY date ASC
        `,
        format: 'JSONEachRow',
        query_params: queryParams,
      })

      const dailyRows = await dailyQuery.json<{
        date: string
        total_results: number
        average_score: number
        total_tokens: number | null
        total_cost: number | null
      }>()

      const dailyStats = dailyRows.map((row) => ({
        date: new Date(row.date),
        totalResults: row.total_results,
        averageScore: row.average_score,
        totalTokens: row.total_tokens ?? 0,
        totalCost: row.total_cost ?? 0,
      }))

      let runningResults = 0
      let runningScore = 0
      for (let i = 0; i < dailyStats.length; i++) {
        runningResults += dailyStats[i]!.totalResults
        runningScore +=
          dailyStats[i]!.averageScore * dailyStats[i]!.totalResults
        dailyStats[i]!.averageScore = runningScore / runningResults
      }

      if (
        runningResults > 0 &&
        (!dailyStats.at(-1)?.date || !isToday(dailyStats.at(-1)!.date)) &&
        (!params.filters?.createdAt?.from ||
          isBefore(params.filters.createdAt.from, now)) &&
        (!params.filters?.createdAt?.to ||
          isAfter(params.filters.createdAt.to, now))
      ) {
        dailyStats.push({
          date: startOfDay(now),
          totalResults: 0,
          averageScore: runningScore / runningResults,
          totalTokens: 0,
          totalCost: 0,
        })
      }

      const versionQuery = await clickhouseClient().query({
        query: `
          SELECT
            commit_uuid,
            count() as total_results,
            avg(score) as average_score,
            ${evaluation.type === EvaluationType.Llm ? 'sum(tokens)' : '0'} as total_tokens,
            ${evaluation.type === EvaluationType.Llm ? 'sum(cost)' : '0'} as total_cost
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE ${conditions.join(' AND ')}
          GROUP BY commit_uuid
          ORDER BY total_results ASC
        `,
        format: 'JSONEachRow',
        query_params: queryParams,
      })

      const versionRows = await versionQuery.json<{
        commit_uuid: string
        total_results: number
        average_score: number | null
        total_tokens: number | null
        total_cost: number | null
      }>()

      const commitUuids = [...new Set(versionRows.map((r) => r.commit_uuid))]
      const commitsData = commitUuids.length
        ? await this.db
            .select()
            .from(commits)
            .where(inArray(commits.uuid, commitUuids))
        : []

      const commitMap = new Map(commitsData.map((c) => [c.uuid, c]))

      const versionStats: EvaluationV2Stats['versionOverview'] = []
      for (const row of versionRows) {
        const version = commitMap.get(row.commit_uuid)
        if (!version) continue

        versionStats.push({
          version,
          totalResults: row.total_results,
          averageScore: row.average_score ?? 0,
          totalTokens: row.total_tokens ?? 0,
          totalCost: row.total_cost ?? 0,
        })
      }

      return Result.ok<EvaluationV2Stats>({
        totalResults: totalStats.total_results,
        averageScore: totalStats.average_score ?? 0,
        totalTokens: totalStats.total_tokens ?? 0,
        totalCost: totalStats.total_cost ?? 0,
        dailyOverview: dailyStats,
        versionOverview: versionStats,
      })
    }

    const now = new Date()

    const stats = {
      totalResults: sql`count(*)`.mapWith(Number).as('total_results'),
      averageScore: sql`avg(${evaluationResultsV2.score})`
        .mapWith(Number)
        .as('average_score'),
      totalTokens:
        evaluation.type === EvaluationType.Llm
          ? sql`sum((${evaluationResultsV2.metadata}->>'tokens')::bigint)`
              .mapWith(Number)
              .as('total_tokens')
          : sql`0`.mapWith(Number),
      totalCost:
        evaluation.type === EvaluationType.Llm
          ? sql`sum((${evaluationResultsV2.metadata}->>'cost')::bigint)`
              .mapWith(Number)
              .as('total_cost')
          : sql`0`.mapWith(Number),
    }

    const filter = and(
      this.listByEvaluationFilter({ evaluationUuid, params }),
      isNull(evaluationResultsV2.error),
    )

    const totalStats = await this.db
      .select(stats)
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .where(filter)
      .then((r) => r[0])

    if (!totalStats || totalStats.totalResults === 0) return Result.nil()

    const [dailyStats, versionStats] = await Promise.all([
      (async () => {
        const dailyStats = await this.db
          .select({
            date: sql`DATE_TRUNC('day', ${evaluationResultsV2.createdAt})`
              .mapWith(parseJSON)
              .as('date'),
            ...stats,
          })
          .from(evaluationResultsV2)
          .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
          .where(filter)
          .groupBy(sql`DATE_TRUNC('day', ${evaluationResultsV2.createdAt})`)
          .orderBy(
            asc(sql`DATE_TRUNC('day', ${evaluationResultsV2.createdAt})`),
          )

        // Note: average score is being computed as a running average
        let runningResults = 0
        let runningScore = 0
        for (let i = 0; i < dailyStats.length; i++) {
          runningResults += dailyStats[i]!.totalResults
          runningScore +=
            dailyStats[i]!.averageScore * dailyStats[i]!.totalResults
          dailyStats[i]!.averageScore = runningScore / runningResults
        }

        // Note: extending the running average to today when applies
        if (
          (!dailyStats.at(-1)?.date || !isToday(dailyStats.at(-1)!.date)) &&
          (!params.filters?.createdAt?.from ||
            isBefore(params.filters.createdAt.from, now)) &&
          (!params.filters?.createdAt?.to ||
            isAfter(params.filters.createdAt.to, now))
        ) {
          dailyStats.push({
            date: startOfDay(now),
            totalResults: 0,
            averageScore: runningScore / runningResults,
            totalTokens: 0,
            totalCost: 0,
          })
        }

        return dailyStats
      })(),
      (async () => {
        const versionStats = await this.db
          .select({
            version: commits,
            ...stats,
          })
          .from(evaluationResultsV2)
          .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
          .where(filter)
          .groupBy(commits.id)
          .orderBy(asc(stats.totalResults))

        return versionStats
      })(),
    ])

    return Result.ok<EvaluationV2Stats>({
      ...totalStats,
      dailyOverview: dailyStats,
      versionOverview: versionStats,
    })
  }

  async listBySpans(spans: Span[]) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const valid = spans.filter((s) => Boolean(s.id && s.traceId))
      if (!valid.length) return Result.ok([])

      const tuples = valid
        .map((_, i) => `({spanId_${i}: String}, {traceId_${i}: String})`)
        .join(', ')

      const queryParams: Record<string, unknown> = {
        workspaceId: this.workspaceId,
      }

      for (let i = 0; i < valid.length; i++) {
        queryParams[`spanId_${i}`] = valid[i]!.id
        queryParams[`traceId_${i}`] = valid[i]!.traceId
      }

      const query = await clickhouseClient().query({
        query: `
          SELECT *
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE workspace_id = {workspaceId: UInt64}
            AND (evaluated_span_id, evaluated_trace_id) IN (${tuples})
          ORDER BY created_at DESC, id DESC
        `,
        format: 'JSONEachRow',
        query_params: queryParams,
      })

      const rows = await query.json<EvaluationResultV2Row>()
      const commitMap = await this.mapCommitUuidsToIds([
        ...new Set(rows.map((r) => r.commit_uuid)),
      ])

      return Result.ok(
        rows.map((row) =>
          this.mapClickHouseRowToEvaluationResult(
            row,
            commitMap.get(row.commit_uuid),
          ),
        ) as unknown as (EvaluationResultV2 & { commitUuid: string })[],
      )
    }

    const results = await this.db
      .select({
        ...tt,
        commitUuid: commits.uuid,
      })
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .where(
        and(
          this.scopeFilter,
          isNull(commits.deletedAt),
          inArray(
            evaluationResultsV2.evaluatedSpanId,
            spans.map((s) => s.id),
          ),
          inArray(
            evaluationResultsV2.evaluatedTraceId,
            spans.map((s) => s.traceId),
          ),
        ),
      )
      .orderBy(
        desc(evaluationResultsV2.createdAt),
        desc(evaluationResultsV2.id),
      )

    return Result.ok(results)
  }

  async listByTraceIds(traceIds: string[]) {
    traceIds = [...new Set(traceIds)].filter(Boolean)
    if (!traceIds.length) {
      return Result.ok([])
    }

    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const chResults = await listEvaluationResultsByTraceIds(
        { workspaceId: this.workspaceId, traceIds },
        this.db,
      )

      return Result.ok(
        chResults.map((r) => ({
          ...r,
          commitUuid: r.commit_uuid,
        })) as unknown as (EvaluationResultV2 & { commitUuid: string })[],
      )
    }

    const results = await this.db
      .select(tt)
      .from(evaluationResultsV2)
      .where(
        and(
          this.scopeFilter,
          inArray(evaluationResultsV2.evaluatedTraceId, traceIds),
        ),
      )
      .orderBy(
        desc(evaluationResultsV2.createdAt),
        desc(evaluationResultsV2.id),
      )

    if (results.length === 0) {
      return Result.ok([])
    }

    const commitIds = [...new Set(results.map((r) => r.commitId))]
    const commitsData = await this.db
      .select({ id: commits.id, uuid: commits.uuid })
      .from(commits)
      .where(and(inArray(commits.id, commitIds), isNull(commits.deletedAt)))

    const commitMap = new Map(commitsData.map((c) => [c.id, c.uuid]))

    const resultsWithCommit = results
      .filter((r) => commitMap.has(r.commitId))
      .map((r) => ({ ...r, commitUuid: commitMap.get(r.commitId)! }))

    return Result.ok(resultsWithCommit)
  }

  // Be careful using this with merged issues, as there will be multiple evaluation results for the same issue
  async listByIssueIds(issueIds: number[], commitHistoryIds: number[]) {
    const uniqueIssueIds = [...new Set(issueIds)].filter(Boolean)
    if (!uniqueIssueIds.length) {
      return []
    }

    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const commitUuids = commitHistoryIds.length
        ? await this.db
            .select({ uuid: commits.uuid })
            .from(commits)
            .where(inArray(commits.id, commitHistoryIds))
            .then((rows) => rows.map((r) => r.uuid))
        : []

      if (!commitUuids.length) {
        return []
      }

      const query = await clickhouseClient().query({
        query: `
          SELECT *
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE workspace_id = {workspaceId: UInt64}
            AND commit_uuid IN ({commitUuids: Array(UUID)})
            AND hasAny(issue_ids, {issueIds: Array(UInt64)})
          ORDER BY created_at DESC, id DESC
        `,
        format: 'JSONEachRow',
        query_params: {
          workspaceId: this.workspaceId,
          commitUuids,
          issueIds: uniqueIssueIds,
        },
      })

      const rows = await query.json<EvaluationResultV2Row>()
      const commitMap = await this.mapCommitUuidsToIds([
        ...new Set(rows.map((r) => r.commit_uuid)),
      ])

      return rows.map((row) => {
        const joinedIssueId = row.issue_ids.find((id) =>
          uniqueIssueIds.includes(Number(id)),
        )

        return {
          ...(this.mapClickHouseRowToEvaluationResult(
            row,
            commitMap.get(row.commit_uuid),
          ) as EvaluationResultV2),
          joinedIssueId: joinedIssueId ?? uniqueIssueIds[0]!,
        }
      }) as (EvaluationResultV2 & { joinedIssueId: number })[]
    }

    const results = await this.db
      .select({
        ...tt,
        commitUuid: commits.uuid,
        joinedIssueId: issueEvaluationResults.issueId,
      })
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .innerJoin(
        issueEvaluationResults,
        eq(issueEvaluationResults.evaluationResultId, evaluationResultsV2.id),
      )
      .where(
        and(
          this.scopeFilter,
          isNull(commits.deletedAt),
          inArray(issueEvaluationResults.issueId, uniqueIssueIds),
          inArray(evaluationResultsV2.commitId, commitHistoryIds),
        ),
      )
      .orderBy(
        desc(evaluationResultsV2.createdAt),
        desc(evaluationResultsV2.id),
      )

    return results as (EvaluationResultV2 & { joinedIssueId: number })[]
  }

  async listBySpanAndDocumentLogUuid({
    projectId,
    documentUuid,
    spanId,
    documentLogUuid,
  }: {
    projectId?: number
    documentUuid: string
    spanId: string
    documentLogUuid: string
  }) {
    const spansRepository = new SpansRepository(this.workspaceId, this.db)
    const traceIds =
      await spansRepository.listTraceIdsByLogUuid(documentLogUuid)

    if (traceIds.length === 0) {
      return Result.ok<ResultWithEvaluationV2[]>([])
    }

    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const query = await clickhouseClient().query({
        query: `
          SELECT *
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE workspace_id = {workspaceId: UInt64}
            AND evaluated_span_id = {spanId: String}
            AND evaluated_trace_id IN ({traceIds: Array(String)})
          ORDER BY created_at DESC, id DESC
        `,
        format: 'JSONEachRow',
        query_params: {
          workspaceId: this.workspaceId,
          spanId,
          traceIds,
        },
      })

      const rows = await query.json<EvaluationResultV2Row>()
      const commitUuids = [...new Set(rows.map((r) => r.commit_uuid))]
      const commitMap = await this.mapCommitUuidsToIds(commitUuids)

      const results = rows.map((row) =>
        this.mapClickHouseRowToEvaluationResult(
          row,
          commitMap.get(row.commit_uuid),
        ),
      ) as (Omit<EvaluationResultV2, 'score'> & { commitUuid: string })[]

      const evaluationsByCommit = await this.getEvaluationsByCommit({
        projectId: projectId!,
        documentUuid,
        results,
      })

      const resultsWithEvaluations: ResultWithEvaluationV2[] = []
      for (const result of results) {
        const evaluation = evaluationsByCommit[result.commitUuid]!.find(
          (e) => e.uuid === result.evaluationUuid,
        )
        if (!evaluation) continue

        const activeAssignment = await findLastActiveAssignedIssue(
          {
            workspaceId: this.workspaceId,
            resultId: result.id,
          },
          this.db,
        )

        const resultWithIssue = {
          ...result,
          issueId: activeAssignment?.issueId ?? null,
        } as EvaluationResultV2

        resultsWithEvaluations.push({
          result: resultWithIssue,
          evaluation,
        } as ResultWithEvaluationV2)
      }

      return Result.ok<ResultWithEvaluationV2[]>(resultsWithEvaluations)
    }

    const results = await this.db
      .select({
        ...tt,
        commitUuid: commits.uuid,
      })
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .where(
        and(
          this.scopeFilter,
          isNull(commits.deletedAt),
          eq(evaluationResultsV2.evaluatedSpanId, spanId),
          inArray(evaluationResultsV2.evaluatedTraceId, traceIds),
        ),
      )
      .orderBy(
        desc(evaluationResultsV2.createdAt),
        desc(evaluationResultsV2.id),
      )

    const evaluationsByCommit = await this.getEvaluationsByCommit({
      projectId: projectId!,
      documentUuid,
      results: results as (Omit<EvaluationResultV2, 'score'> & {
        commitUuid: string
      })[],
    })

    const resultsWithEvaluations: ResultWithEvaluationV2[] = []
    for (const result of results) {
      const evaluation = evaluationsByCommit[result.commitUuid]!.find(
        (e) => e.uuid === result.evaluationUuid,
      )
      if (!evaluation) continue

      const activeAssignment = await findLastActiveAssignedIssue(
        {
          workspaceId: this.workspaceId,
          resultId: (result as EvaluationResultV2).id,
        },
        this.db,
      )

      const resultWithIssue = {
        ...result,
        issueId: activeAssignment?.issueId ?? null,
      } as EvaluationResultV2

      resultsWithEvaluations.push({
        result: resultWithIssue,
        evaluation,
      } as ResultWithEvaluationV2)
    }

    return Result.ok<ResultWithEvaluationV2[]>(resultsWithEvaluations)
  }

  async countSinceDate(since: Date) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const query = await clickhouseClient().query({
        query: `
          SELECT count() as count
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE workspace_id = {workspaceId: UInt64}
            AND has_error = 0
            AND created_at >= {since: DateTime64(3)}
        `,
        format: 'JSONEachRow',
        query_params: {
          workspaceId: this.workspaceId,
          since: since.toISOString(),
        },
      })

      const rows = await query.json<{ count: number }>()
      return Result.ok<number>(rows[0]?.count ?? 0)
    }

    const result = await this.db
      .select({ count: count() })
      .from(evaluationResultsV2)
      .where(
        and(
          this.scopeFilter,
          isNull(evaluationResultsV2.error),
          gte(evaluationResultsV2.createdAt, since),
        ),
      )
      .then((r) => r[0]!)

    return Result.ok<number>(result.count)
  }

  async selectForIssueGeneration({ issueId }: { issueId: number }) {
    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const mergedCommits = await this.db
        .select({ id: commits.id, uuid: commits.uuid })
        .from(commits)
        .where(isNotNull(commits.mergedAt))

      const mergedCommitUuids = mergedCommits.map((c) => c.uuid)
      if (!mergedCommitUuids.length) {
        return Result.ok<EvaluationResultV2[]>([])
      }

      const newerLimit = Math.ceil(
        ISSUE_GENERATION_MAX_RESULTS * ISSUE_GENERATION_RECENCY_RATIO,
      )

      const queryParams = {
        workspaceId: this.workspaceId,
        issueId,
        mergedCommitUuids,
        recentDate: subDays(
          new Date(),
          ISSUE_GENERATION_RECENCY_DAYS,
        ).toISOString(),
        newerLimit,
        olderLimit: ISSUE_GENERATION_MAX_RESULTS - newerLimit,
      }

      const newerQuery = await clickhouseClient().query({
        query: `
          SELECT *
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE workspace_id = {workspaceId: UInt64}
            AND has_error = 0
            AND experiment_id IS NULL
            AND has_passed != 1
            AND has(issue_ids, {issueId: UInt64})
            AND commit_uuid IN ({mergedCommitUuids: Array(UUID)})
            AND created_at >= {recentDate: DateTime64(3)}
          ORDER BY created_at DESC, id DESC, normalized_score ASC
          LIMIT {newerLimit: UInt64}
        `,
        format: 'JSONEachRow',
        query_params: queryParams,
      })

      const olderQuery = await clickhouseClient().query({
        query: `
          SELECT *
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE workspace_id = {workspaceId: UInt64}
            AND has_error = 0
            AND experiment_id IS NULL
            AND has_passed != 1
            AND has(issue_ids, {issueId: UInt64})
            AND commit_uuid IN ({mergedCommitUuids: Array(UUID)})
          ORDER BY created_at ASC, id ASC, normalized_score ASC
          LIMIT {olderLimit: UInt64}
        `,
        format: 'JSONEachRow',
        query_params: queryParams,
      })

      const newerRows = await newerQuery.json<EvaluationResultV2Row>()
      const olderRows = await olderQuery.json<EvaluationResultV2Row>()
      const commitMap = new Map(mergedCommits.map((c) => [c.uuid, c.id]))

      let results = newerRows.map((row) =>
        this.mapClickHouseRowToEvaluationResult(
          row,
          commitMap.get(row.commit_uuid),
        ),
      )

      for (const row of olderRows) {
        if (results.find((r) => r.id === row.id)) continue

        results.push(
          this.mapClickHouseRowToEvaluationResult(
            row,
            commitMap.get(row.commit_uuid),
          ),
        )
      }

      results = results.slice(0, ISSUE_GENERATION_MAX_RESULTS)
      return Result.ok<EvaluationResultV2[]>(results as EvaluationResultV2[])
    }

    const conditions = [
      this.scopeFilter,
      eq(issueEvaluationResults.issueId, issueId),
      isNotNull(commits.mergedAt),
      isNull(evaluationResultsV2.error),
      isNull(evaluationResultsV2.experimentId),
      sql`${evaluationResultsV2.hasPassed} IS NOT TRUE`,
    ]

    const newerLimit = Math.ceil(
      ISSUE_GENERATION_MAX_RESULTS * ISSUE_GENERATION_RECENCY_RATIO,
    )
    const newer = await this.db
      .select(tt)
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .innerJoin(
        issueEvaluationResults,
        eq(issueEvaluationResults.evaluationResultId, evaluationResultsV2.id),
      )
      .where(
        and(
          ...conditions,
          gte(
            evaluationResultsV2.createdAt,
            subDays(new Date(), ISSUE_GENERATION_RECENCY_DAYS),
          ),
        ),
      )
      .orderBy(
        desc(evaluationResultsV2.createdAt),
        desc(evaluationResultsV2.id),
        asc(evaluationResultsV2.normalizedScore),
      )
      .limit(newerLimit)

    const olderLimit = ISSUE_GENERATION_MAX_RESULTS - newerLimit
    const older = await this.db
      .select(tt)
      .from(evaluationResultsV2)
      .innerJoin(
        issueEvaluationResults,
        eq(issueEvaluationResults.evaluationResultId, evaluationResultsV2.id),
      )
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .where(and(...conditions))
      .orderBy(
        asc(evaluationResultsV2.createdAt),
        asc(evaluationResultsV2.id),
        asc(evaluationResultsV2.normalizedScore),
      )
      .limit(olderLimit)

    let results = [...newer]
    for (const result of older) {
      if (newer.find((r) => r.id === result.id)) continue
      results.push(result)
    }
    results = results.slice(0, ISSUE_GENERATION_MAX_RESULTS)

    return Result.ok<EvaluationResultV2[]>(results as EvaluationResultV2[])
  }

  async fetchPaginatedHITLResultsByIssue({
    workspace,
    commit,
    issue,
    page,
    pageSize,
    afterDate,
    orderDirection = 'asc',
  }: {
    workspace: Workspace
    commit: Commit
    issue: Issue
    page: number
    pageSize: number
    orderDirection?: 'asc' | 'desc'
    afterDate?: string
  }) {
    const commitsRepo = new CommitsRepository(workspace.id, this.db)
    const commitHistory = await commitsRepo.getCommitsHistory({ commit })
    const commitIds = commitHistory.map((c) => c.id)
    const commitUuids = commitHistory.map((c) => c.uuid)
    const limit = pageSize + 1
    const offset = calculateOffset(page, pageSize)

    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const conditions = [
        'workspace_id = {workspaceId: UInt64}',
        'type = {evaluationType: String}',
        'evaluated_span_id IS NOT NULL',
        'evaluated_trace_id IS NOT NULL',
        'has(issue_ids, {issueId: UInt64})',
      ]

      const queryParams: Record<string, unknown> = {
        workspaceId: workspace.id,
        evaluationType: EvaluationType.Human,
        issueId: issue.id,
      }

      if (commitUuids.length > 0) {
        conditions.push('commit_uuid IN ({commitUuids: Array(UUID)})')
        queryParams.commitUuids = commitUuids
      }

      if (afterDate) {
        conditions.push('created_at > {afterDate: DateTime64(3)}')
        queryParams.afterDate = new Date(afterDate).toISOString()
      }

      const orderDirectionSql = orderDirection === 'asc' ? 'ASC' : 'DESC'
      const fetchLimit = limit * 2

      const query = await clickhouseClient().query({
        query: `
          SELECT id, evaluated_span_id, evaluated_trace_id, created_at, commit_uuid
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE ${conditions.join(' AND ')}
          ORDER BY created_at ${orderDirectionSql}, id ${orderDirectionSql}
          LIMIT {fetchLimit: UInt64} OFFSET {offset: UInt64}
        `,
        format: 'JSONEachRow',
        query_params: {
          ...queryParams,
          fetchLimit,
          offset,
        },
      })

      const allEvalResults = await query.json<{
        id: number
        evaluated_span_id: string | null
        evaluated_trace_id: string | null
        created_at: string
        commit_uuid: string
      }>()

      const seenSpans = new Set<string>()
      const deduplicatedResults = allEvalResults.filter((result) => {
        const spanKey = `${result.evaluated_span_id}:${result.evaluated_trace_id}`
        if (seenSpans.has(spanKey)) {
          return false
        }
        seenSpans.add(spanKey)
        return true
      })

      const commitMap = new Map(commitHistory.map((c) => [c.uuid, c.id]))
      const mapped = deduplicatedResults
        .slice(0, pageSize + 1)
        .map((row) => ({
          id: row.id,
          evaluatedSpanId: row.evaluated_span_id,
          evaluatedTraceId: row.evaluated_trace_id,
          createdAt: new Date(row.created_at),
          commitId: commitMap.get(row.commit_uuid),
          type: EvaluationType.Human,
        }))
        .filter((r) => r.commitId !== undefined)

      const paginatedResults = mapped.slice(0, pageSize)
      const hasNextPage = mapped.length > pageSize
      const results = hasNextPage ? paginatedResults : mapped

      return {
        results: results as EvaluationResultV2[],
        hasNextPage,
      }
    }

    const whereConditions = [
      eq(issueEvaluationResults.workspaceId, workspace.id),
      eq(issueEvaluationResults.issueId, issue.id),
      eq(evaluationResultsV2.type, EvaluationType.Human),
      isNotNull(evaluationResultsV2.evaluatedSpanId),
      isNotNull(evaluationResultsV2.evaluatedTraceId),
      isNull(commits.deletedAt),
      inArray(evaluationResultsV2.commitId, commitIds),
    ]

    if (afterDate) {
      whereConditions.push(
        gt(evaluationResultsV2.createdAt, new Date(afterDate)),
      )
    }

    const orderDirectionFn = orderDirection === 'asc' ? asc : desc

    // Fetch more results to account for duplicates during deduplication. We
    // multiply by 2 to have a buffer.
    const fetchLimit = limit * 2
    const allEvalResults = await this.db
      .select({
        id: evaluationResultsV2.id,
        evaluatedSpanId: evaluationResultsV2.evaluatedSpanId,
        evaluatedTraceId: evaluationResultsV2.evaluatedTraceId,
        createdAt: evaluationResultsV2.createdAt,
      })
      .from(issueEvaluationResults)
      .innerJoin(
        evaluationResultsV2,
        eq(issueEvaluationResults.evaluationResultId, evaluationResultsV2.id),
      )
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .where(and(...whereConditions))
      .orderBy(
        orderDirectionFn(evaluationResultsV2.createdAt),
        orderDirectionFn(evaluationResultsV2.id),
      )
      .limit(fetchLimit)
      .offset(offset)

    // Deduplicate by span (keep first occurrence which is the latest due to ordering)
    const seenSpans = new Set<string>()
    const deduplicatedResults = allEvalResults.filter((result) => {
      const spanKey = `${result.evaluatedSpanId}:${result.evaluatedTraceId}`
      if (seenSpans.has(spanKey)) {
        return false
      }
      seenSpans.add(spanKey)
      return true
    })

    const paginatedResults = deduplicatedResults.slice(0, pageSize)
    const hasNextPage = deduplicatedResults.length > pageSize
    const results = hasNextPage ? paginatedResults : deduplicatedResults
    return { results, hasNextPage }
  }

  async fetchPaginatedHITLResultsByDocument({
    workspace,
    commit,
    documentUuid,
    excludeIssueId,
    page,
    pageSize,
    afterDate,
    orderDirection = 'asc',
  }: {
    workspace: Workspace
    commit: Commit
    documentUuid: string
    excludeIssueId: number
    page: number
    pageSize: number
    orderDirection?: 'asc' | 'desc'
    afterDate?: string
  }) {
    const commitsRepo = new CommitsRepository(workspace.id, this.db)
    const commitHistory = await commitsRepo.getCommitsHistory({ commit })
    const commitIds = commitHistory.map((c) => c.id)
    const commitUuids = commitHistory.map((c) => c.uuid)
    const limit = pageSize + 1
    const offset = calculateOffset(page, pageSize)

    const evaluationsRepo = new EvaluationsV2Repository(workspace.id, this.db)
    const evaluations = await evaluationsRepo
      .listAtCommitByDocument({
        commitUuid: commit.uuid,
        documentUuid,
      })
      .then((r) => r.unwrap())

    const evaluationUuids = evaluations.map((e) => e.uuid)

    if (evaluationUuids.length === 0) {
      return { results: [], hasNextPage: false }
    }

    const useClickHouse = await this.shouldUseClickHouse()

    if (useClickHouse) {
      const conditions = [
        'workspace_id = {workspaceId: UInt64}',
        'type = {evaluationType: String}',
        'evaluated_span_id IS NOT NULL',
        'evaluated_trace_id IS NOT NULL',
        'evaluation_uuid IN ({evaluationUuids: Array(UUID)})',
        'NOT has(issue_ids, {excludeIssueId: UInt64})',
      ]

      const queryParams: Record<string, unknown> = {
        workspaceId: workspace.id,
        evaluationType: EvaluationType.Human,
        evaluationUuids,
        excludeIssueId,
      }

      if (commitUuids.length > 0) {
        conditions.push('commit_uuid IN ({commitUuids: Array(UUID)})')
        queryParams.commitUuids = commitUuids
      }

      if (afterDate) {
        conditions.push('created_at > {afterDate: DateTime64(3)}')
        queryParams.afterDate = new Date(afterDate).toISOString()
      }

      const orderDirectionSql = orderDirection === 'asc' ? 'ASC' : 'DESC'
      const fetchLimit = limit * 2

      const query = await clickhouseClient().query({
        query: `
          SELECT id, evaluated_span_id, evaluated_trace_id, created_at, commit_uuid, evaluation_uuid
          FROM ${CH_EVALUATION_RESULTS_TABLE}
          WHERE ${conditions.join(' AND ')}
          ORDER BY created_at ${orderDirectionSql}, id ${orderDirectionSql}
          LIMIT {fetchLimit: UInt64} OFFSET {offset: UInt64}
        `,
        format: 'JSONEachRow',
        query_params: {
          ...queryParams,
          fetchLimit,
          offset,
        },
      })

      const allEvalResults = await query.json<{
        id: number
        evaluated_span_id: string | null
        evaluated_trace_id: string | null
        created_at: string
        commit_uuid: string
        evaluation_uuid: string
      }>()

      const seenSpans = new Set<string>()
      const deduplicatedResults = allEvalResults.filter((result) => {
        const spanKey = `${result.evaluated_span_id}:${result.evaluated_trace_id}`
        if (seenSpans.has(spanKey)) {
          return false
        }
        seenSpans.add(spanKey)
        return true
      })

      const commitMap = new Map(commitHistory.map((c) => [c.uuid, c.id]))
      const mapped = deduplicatedResults
        .slice(0, pageSize + 1)
        .map((row) => ({
          id: row.id,
          evaluatedSpanId: row.evaluated_span_id,
          evaluatedTraceId: row.evaluated_trace_id,
          createdAt: new Date(row.created_at),
          commitId: commitMap.get(row.commit_uuid),
          evaluationUuid: row.evaluation_uuid,
          type: EvaluationType.Human,
        }))
        .filter((r) => r.commitId !== undefined)

      const paginatedResults = mapped.slice(0, pageSize)
      const hasNextPage = mapped.length > pageSize
      const results = hasNextPage ? paginatedResults : mapped
      return { results: results as EvaluationResultV2[], hasNextPage }
    }

    const whereConditions = [
      eq(evaluationResultsV2.workspaceId, workspace.id),
      eq(evaluationResultsV2.type, EvaluationType.Human),
      isNotNull(evaluationResultsV2.evaluatedSpanId),
      isNotNull(evaluationResultsV2.evaluatedTraceId),
      isNull(commits.deletedAt),
      inArray(evaluationResultsV2.commitId, commitIds),
      isNull(issueEvaluationResults.id),
      inArray(evaluationResultsV2.evaluationUuid, evaluationUuids),
    ]

    if (afterDate) {
      whereConditions.push(
        gt(evaluationResultsV2.createdAt, new Date(afterDate)),
      )
    }

    const orderDirectionFn = orderDirection === 'asc' ? asc : desc

    // Fetch more results to account for duplicates during deduplication. We
    // multiply by 2 to have a buffer.
    const fetchLimit = limit * 2
    const allEvalResults = await this.db
      .select(tt)
      .from(evaluationResultsV2)
      .innerJoin(commits, eq(commits.id, evaluationResultsV2.commitId))
      .leftJoin(
        issueEvaluationResults,
        and(
          eq(issueEvaluationResults.evaluationResultId, evaluationResultsV2.id),
          eq(issueEvaluationResults.workspaceId, workspace.id),
          eq(issueEvaluationResults.issueId, excludeIssueId),
        ),
      )
      .where(and(...whereConditions))
      .orderBy(
        orderDirectionFn(evaluationResultsV2.createdAt),
        orderDirectionFn(evaluationResultsV2.id),
      )
      .limit(fetchLimit)
      .offset(offset)

    // Deduplicate by span (keep first occurrence which is the latest due to ordering)
    const seenSpans = new Set<string>()
    const deduplicatedResults = allEvalResults.filter((result) => {
      const spanKey = `${result.evaluatedSpanId}:${result.evaluatedTraceId}`
      if (seenSpans.has(spanKey)) {
        return false
      }
      seenSpans.add(spanKey)
      return true
    })

    const paginatedResults = deduplicatedResults.slice(0, pageSize)
    const hasNextPage = deduplicatedResults.length > pageSize
    const results = hasNextPage ? paginatedResults : deduplicatedResults
    return { results: results as EvaluationResultV2[], hasNextPage }
  }

  private async getEvaluationsByCommit({
    projectId,
    documentUuid,
    results,
  }: {
    documentUuid: string
    results: Array<
      Omit<EvaluationResultV2, 'score'> & {
        commitUuid: string
      }
    >
    projectId?: number
  }) {
    const evaluationsRepository = new EvaluationsV2Repository(
      this.workspaceId,
      this.db,
    )
    const commitUuids = [...new Set(results.map((r) => r.commitUuid))]
    return Object.fromEntries(
      await Promise.all(
        commitUuids.map(
          async (commitUuid) =>
            [
              commitUuid,
              await evaluationsRepository
                .listAtCommitByDocument({
                  projectId,
                  commitUuid,
                  documentUuid,
                })
                .then((r) => r.unwrap()),
            ] as const,
        ),
      ),
    )
  }

  async isFromOptimization(
    result: Pick<EvaluationResultV2, 'evaluatedTraceId' | 'evaluatedSpanId'>,
  ) {
    if (!result.evaluatedSpanId || !result.evaluatedTraceId) return false

    const span = await this.db
      .select({ source: spans.source })
      .from(spans)
      .where(
        and(
          eq(spans.traceId, result.evaluatedTraceId),
          eq(spans.id, result.evaluatedSpanId),
          eq(spans.workspaceId, this.workspaceId),
        ),
      )
      .limit(1)
      .then((r) => r[0])

    return span?.source === LogSources.Optimization
  }
}
