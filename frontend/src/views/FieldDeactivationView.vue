<template>
  <section class="page" data-module="field_deactivation">
    <header class="page-head">
      <div>
        <h2>字段停用影响与解析试跑</h2>
        <p class="page-desc">
          停用前按来源用历史原行校验解析依赖，生成可展开影响表；解析验收领取真实原行与规则版本重新解析，
          支持撤回复算、批次切换决策与请求链证据复查。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="refresh-trials" @click="reloadTrials">
          刷新试跑记录
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">全库命中事件总数</span>
        <strong class="stat-value">{{ preview?.total_event_count ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">命中来源</span>
        <strong class="stat-value">{{ preview?.hit_sources.join('、') || '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可试跑 / 阻断 / 待核</span>
        <strong class="stat-value">{{ eligibilitySummary }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">试跑记录</span>
        <strong class="stat-value">{{ trials.length }}</strong>
      </article>
    </div>

    <form class="analysis-form" @submit.prevent="loadPreview">
      <label>
        停用字段
        <input v-model="fieldName" data-testid="deactivate-field" placeholder="如 trace_id" />
      </label>
      <button class="btn primary" type="submit" data-testid="run-preview" :disabled="loading">
        {{ loading ? '预览生成中…' : '生成停用前预览' }}
      </button>
    </form>

    <p v-if="message" class="page-foot" :class="{ 'error-text': !lastOk }">{{ message }}</p>

    <!-- 字段停用影响表（可展开的停用前预览） -->
    <h3 class="section-title">字段停用影响表（停用前预览）</h3>
    <table class="data-table" data-testid="impact-table">
      <thead>
        <tr>
          <th>来源</th>
          <th>是否命中</th>
          <th>命中事件数</th>
          <th>停用后缺失字段</th>
          <th>旧规则版本</th>
          <th>新规则版本</th>
          <th>试跑资格</th>
          <th>试跑资格状态</th>
          <th>操作</th>
          <th>展开预览</th>
        </tr>
      </thead>
      <tbody>
        <template v-for="source in preview?.sources ?? []" :key="source.source_id">
          <tr :data-testid="`impact-row-${source.source_id}`" :data-eligibility="source.eligibility">
            <td>{{ source.source_id }} · {{ source.source_name }}</td>
            <td>{{ source.hit ? '命中' : '未命中' }}</td>
            <td>{{ source.event_count }}</td>
            <td>{{ source.missing_fields.join('、') || '—' }}</td>
            <td>{{ source.old_rule_version || '—' }}</td>
            <td>{{ source.new_rule_version || '—' }}</td>
            <td>{{ source.eligibility }}</td>
            <td>{{ source.entitlement }}</td>
            <td class="row-actions">
              <button
                v-if="source.rule_versions.length"
                class="link"
                type="button"
                :data-testid="`trial-start-${source.source_id}`"
                @click="onStartTrial(source.source_id)"
              >
                新版试跑
              </button>
              <button
                v-if="source.rule_versions.length"
                class="link"
                type="button"
                :data-testid="`dep-${source.source_id}`"
                @click="onToggleDependency(source)"
              >
                {{ source.dependency_fields.includes(fieldName) ? '解除依赖记录' : '补录依赖' }}
              </button>
              <span v-else class="muted-text">单独待核</span>
            </td>
            <td>
              <button
                class="link"
                type="button"
                :data-testid="`expand-${source.source_id}`"
                @click="toggleExpand(source.source_id)"
              >
                {{ expanded[source.source_id] ? '收起' : '展开原行' }}
              </button>
            </td>
          </tr>
          <tr v-if="expanded[source.source_id]" :data-testid="`detail-${source.source_id}`">
            <td colspan="10" class="impact-detail">
              <p class="muted-text">{{ source.note }}</p>
              <ul v-if="source.dependency_notes.length" class="note-list">
                <li v-for="(note, idx) in source.dependency_notes" :key="idx">{{ note }}</li>
              </ul>
              <table class="data-table inner-table">
                <thead>
                  <tr><th>行号</th><th>真实原始日志行</th></tr>
                </thead>
                <tbody>
                  <tr v-for="(line, idx) in source.raw_lines" :key="idx">
                    <td>{{ idx + 1 }}</td>
                    <td class="raw-line">{{ line }}</td>
                  </tr>
                </tbody>
              </table>
            </td>
          </tr>
        </template>
        <tr v-if="preview && !preview.sources.length">
          <td colspan="10" class="empty-state">暂无预览，请先生成停用前预览</td>
        </tr>
      </tbody>
    </table>

    <!-- 批次决策 -->
    <h3 class="section-title">本批切换决策（detect_schema_change → apply_parse_rules）</h3>
    <div class="analysis-form">
      <button class="btn" type="button" data-testid="decide-independent" @click="onDecide('independent')">
        先试 B（能切先切）
      </button>
      <button class="btn" type="button" data-testid="decide-group" @click="onDecide('group')">
        整组等待
      </button>
    </div>
    <table v-if="batchDecision" class="data-table" data-testid="batch-table">
      <thead>
        <tr>
          <th>来源</th><th>决策</th><th>生效规则</th><th>输出是否含停用字段</th>
          <th>Schema 删除字段</th><th>实际解析</th><th>原因</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in batchDecision.decisions" :key="item.source_id" :data-testid="`batch-${item.source_id}`">
          <td>{{ item.source_id }} · {{ item.source_name }}</td>
          <td>{{ item.decision }}</td>
          <td>{{ item.effective_rule || '—' }}</td>
          <td>{{ item.output_contains_field === null ? '—' : item.output_contains_field ? '含' : '不含' }}</td>
          <td>{{ item.schema_change?.removed_fields.join('、') || '—' }}</td>
          <td>{{ item.trial_parse_ok === undefined ? '—' : item.trial_parse_ok ? '全部通过' : '存在失败行' }}</td>
          <td>{{ item.reason || '—' }}</td>
        </tr>
      </tbody>
    </table>

    <!-- 试跑记录与证据 -->
    <h3 class="section-title">解析试跑记录与试跑证据</h3>
    <table class="data-table" data-testid="trial-table">
      <thead>
        <tr>
          <th>试跑编号</th><th>来源</th><th>规则版本</th><th>状态</th>
          <th>原行指纹</th><th>实际解析字段（首行）</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <template v-for="trial in trials" :key="trial.trial_id">
          <tr :data-testid="`trial-row-${trial.trial_id}`" :data-status="trial.status">
            <td>{{ trial.trial_id }}</td>
            <td>{{ trial.source_id }}</td>
            <td>{{ trial.rule_version }}</td>
            <td>{{ trial.status }}</td>
            <td class="raw-line">{{ trial.raw_fingerprint }}</td>
            <td class="raw-line">{{ formatFields(trial.evidence[0]?.parsed_fields) }}</td>
            <td class="row-actions">
              <button
                v-if="trial.status === '试跑通过'"
                class="link" type="button"
                :data-testid="`confirm-${trial.trial_id}`"
                @click="onConfirm(trial.trial_id)"
              >领用新版</button>
              <button
                v-if="trial.status !== '已撤回'"
                class="link" type="button"
                :data-testid="`withdraw-${trial.trial_id}`"
                @click="onWithdraw(trial.trial_id)"
              >撤回复算</button>
              <button
                class="link" type="button"
                :data-testid="`recompute-old-${trial.trial_id}`"
                @click="onRecompute(trial, 'old')"
              >旧版复算</button>
              <button
                class="link" type="button"
                :data-testid="`recompute-new-${trial.trial_id}`"
                @click="onRecompute(trial, 'new')"
              >新版复算</button>
              <button
                class="link" type="button"
                :data-testid="`read-${trial.trial_id}`"
                @click="onReadLine(trial)"
              >实际读取</button>
              <button
                class="link" type="button"
                :data-testid="`trial-expand-${trial.trial_id}`"
                @click="toggleTrialExpand(trial.trial_id)"
              >{{ trialExpanded[trial.trial_id] ? '收起证据' : '查看证据' }}</button>
            </td>
          </tr>
          <tr v-if="trialExpanded[trial.trial_id]">
            <td colspan="7" class="impact-detail">
              <div class="evidence-grid">
                <div v-for="ev in trial.evidence" :key="ev.line_number" class="evidence-card">
                  <p><strong>第 {{ ev.line_number }} 行 · {{ ev.rule_version }} · {{ ev.parse_status }}</strong></p>
                  <p class="raw-line">原行：{{ ev.raw_line }}</p>
                  <p>实际解析字段：{{ formatFields(ev.parsed_fields) }}</p>
                  <p v-if="ev.missing_fields.length" class="error-text">缺失字段：{{ ev.missing_fields.join('、') }}</p>
                </div>
              </div>
              <div v-if="trial.restored_evidence" class="restored-box" data-testid="restored-evidence">
                <h4>撤回后旧规则重新解析（恢复旧字段解释）· {{ trial.restored_at }}</h4>
                <div v-for="ev in trial.restored_evidence" :key="ev.line_number" class="evidence-card">
                  <p><strong>第 {{ ev.line_number }} 行 · {{ ev.rule_version }} · {{ ev.parse_status }}</strong></p>
                  <p class="raw-line">原行：{{ ev.raw_line }}</p>
                  <p>恢复字段：{{ formatFields(ev.parsed_fields) }}</p>
                </div>
              </div>
            </td>
          </tr>
        </template>
        <tr v-if="!trials.length">
          <td colspan="7" class="empty-state">暂无试跑记录，从影响表发起新版试跑</td>
        </tr>
      </tbody>
    </table>

    <!-- 请求链复查 -->
    <h3 class="section-title">值班复查：请求链证据（同一字段约定才可交付）</h3>
    <div class="analysis-form">
      <button class="btn" type="button" data-testid="chain-new-ab" @click="onChain(['SRC-A', 'SRC-B'], 'new')">
        A+B 新版约定复查
      </button>
      <button class="btn" type="button" data-testid="chain-old-ab" @click="onChain(['SRC-A', 'SRC-B'], 'old')">
        A+B 旧版约定复查
      </button>
      <button class="btn" type="button" data-testid="chain-new-bc" @click="onChain(['SRC-B', 'SRC-C'], 'new')">
        B+C 新版约定复查
      </button>
    </div>
    <div v-if="chainResult" class="chain-box" :data-testid="'chain-result'" :data-deliverable="String(chainResult.deliverable)">
      <p :class="chainResult.ok ? 'ok-text' : 'error-text'">
        {{ chainResult.ok ? '可交付' : '不可交付' }}：{{ chainResult.message }}
      </p>
      <table v-if="chainResult.per_source?.length" class="data-table inner-table">
        <thead><tr><th>来源</th><th>规则版本</th><th>复算通过</th><th>含停用字段</th></tr></thead>
        <tbody>
          <tr v-for="item in chainResult.per_source" :key="item.source_id">
            <td>{{ item.source_id }} · {{ item.source_name }}</td>
            <td>{{ item.rule_version }}</td>
            <td>{{ item.parse_ok ? '是' : '否' }}</td>
            <td>{{ item.field_present ? '是' : '否' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  buildChainEvidence,
  confirmTrial,
  decideBatch,
  fetchPreview,
  fetchSources,
  fetchTrials,
  readTrialLine,
  recomputeTrial,
  saveDependency,
  startTrial,
  withdrawTrial,
} from '@/api/field-deactivation'
import type {
  BatchDecision,
  ChainEvidenceResult,
  FieldTrial,
  ImpactSource,
  SourceState,
} from '@/api/field-deactivation'

const fieldName = ref('trace_id')
const loading = ref(false)
const message = ref('')
const lastOk = ref(true)
const preview = ref<Awaited<ReturnType<typeof fetchPreview>> | null>(null)
const sources = ref<SourceState[]>([])
const trials = ref<FieldTrial[]>([])
const batchDecision = ref<BatchDecision | null>(null)
const chainResult = ref<ChainEvidenceResult | null>(null)
const expanded = ref<Record<string, boolean>>({})
const trialExpanded = ref<Record<string, boolean>>({})

const eligibilitySummary = computed(() => {
  if (!preview.value) return '—'
  const ready = preview.value.sources.filter((item) => item.eligibility === '可试跑').length
  const blocked = preview.value.sources.filter((item) => item.eligibility === '依赖阻断').length
  const pending = preview.value.sources.filter((item) => item.eligibility === '待核').length
  return `${ready} / ${blocked} / ${pending}`
})

function notify(ok: boolean, text: string) {
  lastOk.value = ok
  message.value = text
}

function formatFields(fields?: Record<string, string>): string {
  if (!fields) return '—'
  return Object.entries(fields).map(([key, value]) => `${key}=${value}`).join('；')
}

function toggleExpand(sourceId: string) {
  expanded.value[sourceId] = !expanded.value[sourceId]
}

function toggleTrialExpand(trialId: string) {
  trialExpanded.value[trialId] = !trialExpanded.value[trialId]
}

async function loadPreview() {
  if (!fieldName.value.trim()) {
    notify(false, '请先填写停用字段名')
    return
  }
  loading.value = true
  try {
    preview.value = await fetchPreview(fieldName.value.trim())
    await reloadSources()
    notify(true, `停用前预览已生成：全库命中 ${preview.value.total_event_count} 个事件`)
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '预览生成失败')
  } finally {
    loading.value = false
  }
}

async function reloadSources() {
  const payload = await fetchSources()
  sources.value = payload.sources
  if (preview.value) {
    const stateMap = new Map(payload.sources.map((item) => [item.source_id, item]))
    preview.value.sources = preview.value.sources.map((item): ImpactSource => ({
      ...item,
      ...(stateMap.get(item.source_id) ?? {}),
    }))
  }
}

async function reloadTrials() {
  const payload = await fetchTrials()
  trials.value = payload.items
}

async function onToggleDependency(source: ImpactSource) {
  const depends = !source.dependency_fields.includes(fieldName.value)
  try {
    const result = await saveDependency(source.source_id, fieldName.value, depends, '页面复核补录')
    notify(result.ok, result.message)
    await reloadSources()
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '依赖记录失败')
  }
}

async function onStartTrial(sourceId: string) {
  try {
    const result = await startTrial(sourceId, fieldName.value)
    notify(result.ok, result.message)
    await Promise.all([reloadSources(), reloadTrials()])
    if (result.trial) trialExpanded.value[result.trial.trial_id] = true
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '试跑发起失败')
  }
}

async function onConfirm(trialId: string) {
  try {
    const result = await confirmTrial(trialId)
    notify(result.ok, result.message)
    await Promise.all([reloadSources(), reloadTrials()])
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '领用失败')
  }
}

async function onWithdraw(trialId: string) {
  try {
    const result = await withdrawTrial(trialId)
    notify(result.ok, result.message)
    trialExpanded.value[trialId] = true
    await Promise.all([reloadSources(), reloadTrials()])
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '撤回失败')
  }
}

async function onRecompute(trial: FieldTrial, version: 'old' | 'new') {
  const ruleVersion = version === 'old'
    ? trial.rule_version.replace('V2', 'V1')
    : trial.rule_version.replace('V1', 'V2')
  try {
    const result = await recomputeTrial(trial.trial_id, ruleVersion)
    notify(result.ok, `${result.message}，共 ${result.evidence.length} 行复算证据`)
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '复算失败')
  }
}

async function onReadLine(trial: FieldTrial) {
  try {
    const result = await readTrialLine(trial.trial_id, 1)
    if (!result.ok || !result.evidence) {
      notify(false, result.message ?? '读取失败')
      return
    }
    notify(true, `实际读取第 1 行（${result.evidence.rule_version}）：${formatFields(result.evidence.parsed_fields)}`)
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '读取失败')
  }
}

async function onDecide(mode: 'independent' | 'group') {
  try {
    batchDecision.value = await decideBatch(fieldName.value, mode)
    const names = batchDecision.value.continuable_sources.join('、') || '无'
    notify(true, `本批可继续切换新版的来源：${names}`)
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '批次决策失败')
  }
}

async function onChain(sourceIds: string[], convention: 'new' | 'old') {
  try {
    chainResult.value = await buildChainEvidence(fieldName.value, sourceIds, convention)
    notify(chainResult.value.ok, chainResult.value.message)
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '请求链复查失败')
  }
}

onMounted(async () => {
  await loadPreview()
  await reloadTrials()
})
</script>

<style scoped>
.section-title { margin: 20px 0 8px; font-size: 15px; }
.muted-text { color: var(--muted); font-size: 12px; }
.raw-line { font-family: monospace; font-size: 12px; word-break: break-all; }
.impact-detail { background: #fafbfc; }
.inner-table { margin-top: 8px; }
.note-list { margin: 4px 0; padding-left: 18px; font-size: 12px; color: var(--muted); }
.evidence-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 10px; }
.evidence-card { border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; background: #fff; }
.evidence-card p { margin: 4px 0; font-size: 12px; }
.restored-box { margin-top: 12px; border: 1px dashed var(--brand); border-radius: 8px; padding: 10px; }
.ok-text { color: #047857; }
.chain-box { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
</style>
