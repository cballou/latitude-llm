import { clickhouseClient } from '../../../client/clickhouse'
import {
  TABLE_NAME,
  EvaluationResultV2Row,
} from '../../../schema/models/clickhouse/evaluationResults'
import { scopedQuery } from '../../scope'

export const listEvaluationResultsByTraceIds = scopedQuery(
  async function listEvaluationResultsByTraceIds(
    { workspaceId, traceIds }: { workspaceId: number; traceIds: string[] },
    _db,
  ): Promise<EvaluationResultV2Row[]> {
    if (!traceIds.length) return []

    const result = await clickhouseClient().query({
      query: `
        SELECT *
        FROM ${TABLE_NAME}
        WHERE workspace_id = {workspaceId: UInt64}
          AND evaluated_trace_id IN ({traceIds: Array(String)})
        ORDER BY created_at DESC, id DESC
      `,
      format: 'JSONEachRow',
      query_params: { workspaceId, traceIds },
    })

    return result.json<EvaluationResultV2Row>()
  },
)
