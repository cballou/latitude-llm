import { clickhouseClient } from '../../../client/clickhouse'
import {
  TABLE_NAME,
  EvaluationResultV2Row,
} from '../../../schema/models/clickhouse/evaluationResults'
import { scopedQuery } from '../../scope'

export const findEvaluationResultByUuid = scopedQuery(
  async function findEvaluationResultV2RowByUuid(
    { workspaceId, uuid }: { workspaceId: number; uuid: string },
    _db,
  ): Promise<EvaluationResultV2Row | null> {
    const result = await clickhouseClient().query({
      query: `
        SELECT *
        FROM ${TABLE_NAME}
        WHERE workspace_id = {workspaceId: UInt64}
          AND uuid = {uuid: UUID}
        ORDER BY updated_at DESC
        LIMIT 1
      `,
      format: 'JSONEachRow',
      query_params: { workspaceId, uuid },
    })

    const rows = await result.json<EvaluationResultV2Row>()
    return rows[0] ?? null
  },
)
