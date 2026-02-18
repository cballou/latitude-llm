import { clickhouseClient } from '../../../client/clickhouse'
import { TABLE_NAME } from '../../../schema/models/clickhouse/evaluationResults'
import { scopedQuery } from '../../scope'

interface HasResultsFromOtherCommitsParams {
  workspaceId: number
  issueId: number
  excludeCommitUuid: string
}

export const hasEvaluationResultsFromOtherCommits = scopedQuery(
  async function hasEvaluationResultsFromOtherCommits(
    params: HasResultsFromOtherCommitsParams,
    _db,
  ) {
    const { workspaceId, issueId, excludeCommitUuid } = params

    const query = `
      SELECT 1 as exists
      FROM ${TABLE_NAME}
      WHERE workspace_id = {workspaceId: UInt64}
        AND commit_uuid != {excludeCommitUuid: UUID}
        AND evaluated_trace_id IS NOT NULL
        AND evaluated_span_id IS NOT NULL
        AND has(issue_ids, {issueId: UInt64})
      LIMIT 1
    `

    const result = await clickhouseClient().query({
      query,
      format: 'JSONEachRow',
      query_params: { workspaceId, issueId, excludeCommitUuid },
    })

    const rows = await result.json<{ exists: number }>()
    return rows.length > 0
  },
)
