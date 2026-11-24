/** 字段停用工作流 API：停用前预览、解析试跑、撤回复算、批次决策与请求链复查。 */
import { fetchJson, request } from './client'

const BASE = '/api/field-deactivation'

export type LineEvidence = {
  line_number: number
  raw_line: string
  rule_version: string
  parse_status: string
  parsed_fields: Record<string, string>
  missing_fields: string[]
}

export type TrialHistoryItem = {
  at: string
  action: string
  rule_version?: string
  result?: string
  released_new_entitlement?: boolean
}

export type FieldTrial = {
  trial_id: string
  source_id: string
  field_name: string
  rule_version: string
  raw_fingerprint: string
  raw_lines: string[]
  started_at: string
  status: string
  evidence: LineEvidence[]
  restored_evidence: LineEvidence[] | null
  restored_at: string | null
  confirmed_at: string | null
  history: TrialHistoryItem[]
}

export type SourceState = {
  source_id: string
  source_name: string
  eligibility: string
  dependency_fields: string[]
  dependency_notes: string[]
  rule_versions: string[]
  active_trial_id: string | null
  entitlement: string
  confirmed_new: boolean
}

export type ImpactSource = SourceState & {
  hit: boolean
  event_count: number
  sample_line_count: number
  missing_fields: string[]
  old_rule_version: string | null
  new_rule_version: string | null
  note: string
  raw_lines: string[]
}

export type DeactivationPreview = {
  field_name: string
  total_event_count: number
  hit_sources: string[]
  generated_at: string
  sources: ImpactSource[]
}

export type BatchDecisionItem = {
  source_id: string
  source_name: string
  decision: string
  decision_raw?: string
  reason?: string
  effective_rule: string | null
  output_contains_field: boolean | null
  dependency_blocked?: boolean
  trial_parse_ok?: boolean
  old_rule_version?: string
  new_rule_version?: string
  schema_change?: {
    added_fields: string[]
    removed_fields: string[]
    is_compatible: boolean
    affected_event_count: number
  } | null
  evidence?: LineEvidence[]
}

export type BatchDecision = {
  field_name: string
  mode: string
  decided_at: string
  continuable_sources: string[]
  decisions: BatchDecisionItem[]
}

export type ChainPerSource = {
  source_id: string
  source_name: string
  rule_version: string
  parse_ok: boolean
  field_present: boolean
  evidence: LineEvidence[]
}

export type ChainEvidenceResult = {
  ok: boolean
  deliverable?: boolean
  message: string
  convention?: string
  field_name?: string
  per_source?: ChainPerSource[]
}

export type ActionResponse = {
  ok: boolean
  message: string
  trial?: FieldTrial
  state?: SourceState
}

async function post<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await request(`${BASE}${path}`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error('字段停用接口请求失败')
  return (await response.json()) as T
}

export async function fetchSources(): Promise<{ ok: boolean; sources: SourceState[] }> {
  return fetchJson(`${BASE}/sources`)
}
export async function fetchPreview(fieldName: string): Promise<DeactivationPreview> {
  return post('/preview', { field_name: fieldName })
}

export async function saveDependency(
  sourceId: string,
  fieldName: string,
  depends: boolean,
  note = '',
): Promise<ActionResponse> {
  return post(`/sources/${sourceId}/dependency`, { field_name: fieldName, depends, note })
}

export async function startTrial(sourceId: string, fieldName: string): Promise<ActionResponse> {
  return post(`/sources/${sourceId}/trials`, { field_name: fieldName })
}

export async function fetchTrials(sourceId?: string): Promise<{ ok: boolean; items: FieldTrial[] }> {
  const query = sourceId ? `?source_id=${encodeURIComponent(sourceId)}` : ''
  return fetchJson(`${BASE}/trials${query}`)
}

export async function confirmTrial(trialId: string): Promise<ActionResponse> {
  return post('/trials/confirm', { trial_id: trialId })
}

export async function withdrawTrial(trialId: string): Promise<ActionResponse> {
  return post('/trials/withdraw', { trial_id: trialId })
}

export async function recomputeTrial(
  trialId: string,
  ruleVersion: string,
): Promise<ActionResponse & { evidence: LineEvidence[] }> {
  return post('/trials/recompute', { trial_id: trialId, rule_version: ruleVersion })
}

export async function readTrialLine(
  trialId: string,
  lineNumber: number,
): Promise<{ ok: boolean; message?: string; evidence?: LineEvidence }> {
  return fetchJson(`${BASE}/trials/${trialId}/lines/${lineNumber}`)
}

export async function decideBatch(
  fieldName: string,
  mode: 'independent' | 'group',
): Promise<BatchDecision> {
  return post('/batch-decisions', { field_name: fieldName, mode })
}

export async function buildChainEvidence(
  fieldName: string,
  sourceIds: string[],
  convention: 'new' | 'old',
): Promise<ChainEvidenceResult> {
  return post('/chain-evidence', {
    field_name: fieldName,
    source_ids: sourceIds,
    convention,
  })
}
