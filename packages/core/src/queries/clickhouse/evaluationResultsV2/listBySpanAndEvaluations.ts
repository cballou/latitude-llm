import { clickhouseClient } from '../../../client/clickhouse'
import {
  TABLE_NAME,
  EvaluationResultV2Row,
} from '../../../schema/models/clickhouse/evaluationResults'
import { scopedQuery } from '../../scope'

interface ListBySpanAndEvaluationsParams {
  workspaceId: number
  spans: { id: string; traceId: string }[]
  evaluationUuids: string[]
  commitUuids: string[]
}

export const listEvaluationResultsBySpanAndEvaluations = scopedQuery(
  async function listEvaluationResultsBySpanAndEvaluations(
    params: ListBySpanAndEvaluationsParams,
    _db,
  ): Promise<EvaluationResultV2Row[]> {
    const { workspaceId, spans, evaluationUuids, commitUuids } = params

    if (!spans.length || !evaluationUuids.length) return []

    const spanIds = spans.map((s) => s.id)
    const traceIds = spans.map((s) => s.traceId)

    const result = await clickhouseClient().query({
      query: `
        SELECT *
        FROM ${TABLE_NAME}
        WHERE workspace_id = {workspaceId: UInt64}
          AND evaluated_span_id IN ({spanIds: Array(String)})
          AND evaluated_trace_id IN ({traceIds: Array(String)})
          AND evaluation_uuid IN ({evaluationUuids: Array(UUID)})
          AND commit_uuid IN ({commitUuids: Array(UUID)})
        ORDER BY created_at DESC, id DESC
      `,
      format: 'JSONEachRow',
      query_params: {
        workspaceId,
        spanIds,
        traceIds,
        evaluationUuids,
        commitUuids,
      },
    })

    return result.json<EvaluationResultV2Row>()
  },
)
