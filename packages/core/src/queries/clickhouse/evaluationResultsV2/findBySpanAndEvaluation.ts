import { clickhouseClient } from '../../../client/clickhouse'
import {
  TABLE_NAME,
  EvaluationResultV2Row,
} from '../../../schema/models/clickhouse/evaluationResults'
import { scopedQuery } from '../../scope'

export const findEvaluationResultBySpanAndEvaluation = scopedQuery(
  async function findEvaluationResultBySpanAndEvaluation(
    {
      workspaceId,
      evaluatedSpanId,
      evaluatedTraceId,
      evaluationUuid,
    }: {
      workspaceId: number
      evaluatedSpanId: string
      evaluatedTraceId: string
      evaluationUuid: string
    },
    _db,
  ): Promise<EvaluationResultV2Row | null> {
    const result = await clickhouseClient().query({
      query: `
        SELECT *
        FROM ${TABLE_NAME}
        WHERE workspace_id = {workspaceId: UInt64}
          AND evaluated_span_id = {evaluatedSpanId: String}
          AND evaluated_trace_id = {evaluatedTraceId: String}
          AND evaluation_uuid = {evaluationUuid: UUID}
        ORDER BY updated_at DESC
        LIMIT 1
      `,
      format: 'JSONEachRow',
      query_params: {
        workspaceId,
        evaluatedSpanId,
        evaluatedTraceId,
        evaluationUuid,
      },
    })

    const rows = await result.json<EvaluationResultV2Row>()
    return rows[0] ?? null
  },
)
