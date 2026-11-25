<template>
  <section class="delivery-panel" data-testid="delivery-panel">
    <header class="delivery-head">
      <div>
        <h3>交付视图 · 消费方快照交接明细</h3>
        <p>按消费方对比两次可用快照，列出新增与撤回文件；页凭据绑定消费方和交付编号，确认位点用于断线续取。</p>
      </div>
      <button class="btn" type="button" data-testid="delivery-overview-refresh" @click="loadOverview">
        刷新交付概览
      </button>
    </header>

    <form class="delivery-form" @submit.prevent="refreshFromSnapshot">
      <label>
        <span>所属湖表</span>
        <input v-model="form.lake_table" data-testid="delivery-table" placeholder="例如 ods_user_behavior" />
      </label>
      <label>
        <span>消费方 / 订阅方</span>
        <input v-model="form.consumer_id" data-testid="delivery-consumer" placeholder="例如 consumer-a" />
      </label>
      <label>
        <span>订阅分区（逗号分隔，空为全部分区）</span>
        <input v-model="form.partitions" data-testid="delivery-partitions" placeholder="例如 dt=20261124" />
      </label>
      <label>
        <span>原绑定快照 / 确认位置快照</span>
        <input v-model="form.base_snapshot" data-testid="delivery-base-snapshot" placeholder="S1" />
      </label>
      <label>
        <span>目标可用快照</span>
        <input v-model="form.target_snapshot" data-testid="delivery-target-snapshot" placeholder="S2，留空取最新可用" />
      </label>
      <label class="credential-field">
        <span>重连附带的页凭据</span>
        <input v-model="form.page_token" data-testid="delivery-page-token" placeholder="下游已写入的 page_token / credential" />
      </label>
      <div class="delivery-actions">
        <button class="btn primary" type="submit" data-testid="delivery-refresh">刷新并生成变更列表</button>
        <button class="btn" type="button" data-testid="delivery-reconnect" @click="reconnect">重连 / 继续领取</button>
        <button class="btn ghost" type="button" data-testid="delivery-seed-scenario" @click="seedScenario">
          构造 A/B→A/C 冲突场景
        </button>
        <button class="btn ghost" type="button" data-testid="delivery-replay" :disabled="!delivery?.delivery_id" @click="showReplay">
          按交付编号回放
        </button>
      </div>
    </form>

    <p v-if="statusMessage" class="delivery-ok" data-testid="delivery-message">{{ statusMessage }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="delivery-error">{{ errorMessage }}</p>

    <div v-if="overview" class="delivery-overview" data-testid="delivery-overview">
      <h4>交付概览</h4>
      <div class="overview-columns">
        <article>
          <h5>当前留存（{{ overview.retained_files?.length ?? 0 }}）</h5>
          <ul>
            <li v-for="file in overview.retained_files ?? []" :key="`keep-${file.consumer_id}-${file.file_id}`"
                data-testid="overview-retained-file">
              <code>{{ file.file_id }}</code>
              <span>{{ file.consumer_id }}</span>
              <em v-if="file.partition_key">{{ file.partition_key }}</em>
            </li>
          </ul>
        </article>
        <article>
          <h5>已经撤回（{{ overview.withdrawn_files?.length ?? 0 }}）</h5>
          <ul>
            <li v-for="file in overview.withdrawn_files ?? []" :key="`gone-${file.consumer_id}-${file.file_id}`"
                data-testid="overview-withdrawn-file">
              <code>{{ file.file_id }}</code>
              <span>{{ file.consumer_id }}</span>
              <em>确认位置：{{ file.ack_position ?? '未确认' }}</em>
              <em v-if="file.recycled">已回收</em>
            </li>
          </ul>
        </article>
      </div>
    </div>

    <div v-if="delivery" class="delivery-detail" data-testid="delivery-detail" :data-conflict="String(Boolean(delivery.conflict))">
      <div class="detail-head">
        <h4>交付 {{ delivery.delivery_id }}</h4>
        <span class="badge" :data-status="delivery.delivery_status ?? delivery.status">
          {{ delivery.delivery_status ?? delivery.status }}
        </span>
      </div>
      <p class="snapshot-line">
        <span>消费方：<b>{{ delivery.consumer_id }}</b></span>
        <span>{{ delivery.base_snapshot || '空清单' }} → {{ delivery.target_snapshot }}</span>
        <span>确认位点：{{ delivery.checkpoint?.checkpoint_position ?? 0 }}</span>
      </p>
      <p v-if="delivery.conflict" class="conflict-banner" data-testid="delivery-conflict-banner">
        检测到旧页冲突：未确认的新增文件已在新快照撤回。旧页可只读追溯，当前确认请以新交付中的撤回明细为准。
      </p>

      <table class="delivery-table">
        <thead>
          <tr>
            <th>文件</th>
            <th>变更</th>
            <th>分区</th>
            <th>页凭据</th>
            <th>状态 / 确认位置</th>
            <th>确认接口</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="change in delivery.changes ?? []" :key="change.change_id"
              :data-testid="`delivery-change-${change.file_id}`"
              :data-change-type="change.change_type"
              :data-acknowledged="String(Boolean(change.acknowledged))">
            <td><code>{{ change.file_id }}</code></td>
            <td>
              <span :class="['change-tag', change.change_type]">
                {{ change.change_type === 'added' ? '新增' : '撤回' }}
              </span>
            </td>
            <td>{{ change.partition_key ?? '—' }}</td>
            <td>
              <button class="link token-button" type="button"
                      :data-testid="`copy-token-${change.file_id}`"
                      @click="useCredential(change.page_token)">
                {{ shortToken(change.page_token) }}
              </button>
            </td>
            <td>
              <span>{{ change.acknowledged ? '已确认' : (change.status === 'superseded' ? '已被新快照取代' : '待确认') }}</span>
              <em v-if="change.ack_position != null"> @{{ change.ack_position }}</em>
            </td>
            <td>
              <button class="link" type="button"
                      :data-testid="`ack-${change.file_id}`"
                      :disabled="Boolean(change.acknowledged)"
                      @click="acknowledgeChange(change)">
                {{ change.acknowledged ? '已确认' : '确认该页' }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="replay" class="delivery-replay" data-testid="delivery-replay">
      <h4>时间旅行回放：{{ replay.delivery_id }}</h4>
      <p>
        <span>实际确认后留存：<b data-testid="replay-file-ids">{{ (replay.file_ids ?? []).join(', ') || '（空）' }}</b></span>
        <span>普通按版查询：{{ (replay.snapshot_file_ids ?? []).join(', ') || '（空）' }}</span>
        <span :data-testid="`replay-match-${String(Boolean(replay.match_snapshot))}`">
          结果对齐：{{ replay.match_snapshot ? '一致' : '尚未完全确认/不一致' }}
        </span>
      </p>
      <ul>
        <li v-for="marker in replay.withdrawals ?? []" :key="`replay-withdraw-${marker.file_id}`"
            data-testid="replay-withdrawal">
          撤回 <code>{{ marker.file_id }}</code>，确认位置 @{{ marker.ack_position }}
          <span v-if="marker.recycled">（文件已回收，撤回记录仍可回放）</span>
        </li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  acknowledgeDelivery,
  getDeliveryJson,
  loadDeliveryOverview,
  loadDeliveryReplay,
  postDeliveryJson,
  reconnectDelivery,
  refreshDelivery,
} from '@/api/delivery-service'

const CREDENTIAL_STORAGE_KEY = 'lake-delivery-page-credential'

const form = reactive({
  lake_table: '',
  consumer_id: '',
  partitions: '',
  base_snapshot: '',
  target_snapshot: '',
  page_token: '',
})
const delivery = ref<Record<string, any> | null>(null)
const overview = ref<Record<string, any> | null>(null)
const replay = ref<Record<string, any> | null>(null)
const statusMessage = ref('')
const errorMessage = ref('')

function cleanPayload(): Record<string, unknown> {
  return Object.fromEntries(Object.entries(form).filter(([, value]) => String(value ?? '').trim() !== ''))
}

function rememberCredential(token: string) {
  form.page_token = token
  window.localStorage.setItem(CREDENTIAL_STORAGE_KEY, token)
}

function useCredential(token: string) {
  rememberCredential(token)
  statusMessage.value = '页凭据已写入本地续取位置，重连时会附带。'
}

function shortToken(token?: string): string {
  if (!token) return '—'
  return token.length > 14 ? `${token.slice(0, 8)}…${token.slice(-4)}` : token
}

function applyDelivery(result: Record<string, any>) {
  delivery.value = result.current_delivery ?? result.delivery ?? result
}

async function seedScenario() {
  errorMessage.value = ''
  statusMessage.value = ''
  try {
    const result = await postDeliveryJson('/api/delivery-view/scenario', {})
    form.lake_table = result.lake_table
    form.consumer_id = result.consumer_id
    form.partitions = 'p1'
    form.base_snapshot = result.s1_snapshot
    form.target_snapshot = result.s2_snapshot
    form.page_token = result.page_token_b ?? ''
    if (result.page_token_b) window.localStorage.setItem(CREDENTIAL_STORAGE_KEY, result.page_token_b)
    applyDelivery(result)
    overview.value = await loadDeliveryOverview(
      new URLSearchParams({ consumer_id: result.consumer_id, lake_table: result.lake_table }),
    )
    statusMessage.value = '场景已构造：A 已确认，B 页未确认；S2 已撤回 B 并新增 C。可直接重连或逐页确认。'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '场景构造失败'
  }
}

async function refreshOverviewQuietly() {
  const query = new URLSearchParams()
  if (form.consumer_id) query.set('consumer_id', form.consumer_id)
  if (form.lake_table) query.set('lake_table', form.lake_table)
  overview.value = await loadDeliveryOverview(query)
}

async function refreshFromSnapshot() {
  errorMessage.value = ''
  statusMessage.value = ''
  try {
    const result = await refreshDelivery(cleanPayload())
    if (result.success === false) throw new Error(result.message || result.error || '交付刷新失败')
    applyDelivery(result)
    statusMessage.value = result.conflict
      ? '已生成 S2 交付，并标出旧未确认页对应的撤回冲突。'
      : '已按可用快照生成消费方变更列表。'
    await refreshOverviewQuietly()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付刷新失败'
  }
}

async function reconnect() {
  errorMessage.value = ''
  statusMessage.value = ''
  try {
    const payload = cleanPayload()
    const stored = window.localStorage.getItem(CREDENTIAL_STORAGE_KEY)
    if (!payload.page_token && stored) payload.page_token = stored
    const result = await reconnectDelivery(payload)
    if (result.success === false) throw new Error(result.message || result.error || '重连续取失败')
    applyDelivery(result)
    if (result.conflict) {
      statusMessage.value = '重连成功：旧页已读取到冲突状态，请改确认新交付中的撤回与新增。'
    } else if (result.resumed_page) {
      statusMessage.value = '已凭页凭据恢复断线前正在读取的页。'
    } else {
      statusMessage.value = '重连成功，已按确认位置返回待读取变更。'
    }
    await refreshOverviewQuietly()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重连续取失败'
  }
}

async function acknowledgeChange(change: Record<string, any>) {
  errorMessage.value = ''
  statusMessage.value = ''
  try {
    const result = await acknowledgeDelivery({
      consumer_id: form.consumer_id || delivery.value?.consumer_id,
      lake_table: form.lake_table || delivery.value?.lake_table,
      delivery_id: delivery.value?.delivery_id,
      change_id: change.change_id,
      page_id: change.page_id,
      file_id: change.file_id,
      page_token: change.page_token || form.page_token,
    })
    if (result.success === false) throw new Error(result.message || result.error || '确认失败')
    statusMessage.value = `已确认 ${change.file_id}，当前位点 ${result.checkpoint_position}`
    if (delivery.value?.delivery_id) {
      const detail = await getDeliveryJson(`/api/deliveries/${delivery.value.delivery_id}?consumer_id=${encodeURIComponent(form.consumer_id || delivery.value.consumer_id)}`)
      delivery.value = detail
    }
    await refreshOverviewQuietly()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '确认失败'
  }
}

async function loadOverview() {
  errorMessage.value = ''
  try {
    await refreshOverviewQuietly()
    if (!overview.value) throw new Error('请先填写消费方或所属湖表')
    statusMessage.value = '交付概览已刷新。'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '概览读取失败'
  }
}

async function showReplay() {
  errorMessage.value = ''
  try {
    if (!delivery.value?.delivery_id) return
    replay.value = await loadDeliveryReplay(
      String(delivery.value.delivery_id),
      form.consumer_id || delivery.value.consumer_id,
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付回放失败'
  }
}

onMounted(() => {
  const token = window.localStorage.getItem(CREDENTIAL_STORAGE_KEY)
  if (token) form.page_token = token
  void refreshOverviewQuietly().catch(() => {
    // 初始概览失败不阻塞登记表单和场景构造。
  })
})
</script>

<style scoped>
.delivery-panel {
  margin: 18px 0;
  padding: 18px;
  border: 1px solid var(--border-color, #d9e0ea);
  border-radius: 14px;
  background: linear-gradient(180deg, #f8fbff 0%, #ffffff 42%);
}

.delivery-head,
.detail-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
}

.delivery-head h3,
.delivery-detail h4,
.delivery-overview h4,
.delivery-replay h4 {
  margin: 0 0 6px;
}

.delivery-head p,
.snapshot-line {
  margin: 0;
  color: #5d6b82;
  font-size: 13px;
}

.delivery-form {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin: 16px 0;
}

.delivery-form label {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 13px;
  color: #344054;
}

.delivery-form input {
  padding: 9px 10px;
  border: 1px solid #cfd8e6;
  border-radius: 8px;
}

.credential-field {
  grid-column: span 2;
}

.delivery-actions {
  grid-column: 1 / -1;
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.delivery-ok {
  color: #067647;
  font-size: 13px;
}

.overview-columns {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 14px;
}

.overview-columns article {
  padding: 12px;
  border-radius: 10px;
  background: #fff;
  border: 1px solid #e4e9f2;
}

.overview-columns ul {
  margin: 8px 0 0;
  padding-left: 0;
  list-style: none;
}

.overview-columns li {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  padding: 6px 0;
  border-bottom: 1px dashed #e6ebf3;
  font-size: 13px;
}

.overview-columns em {
  color: #b54708;
  font-style: normal;
}

.delivery-detail,
.delivery-overview,
.delivery-replay {
  margin-top: 16px;
}

.badge {
  padding: 3px 10px;
  border-radius: 999px;
  background: #eef4ff;
  color: #2456d6;
  font-size: 12px;
}

.badge[data-status='superseded'] {
  background: #fff1f0;
  color: #b42318;
}

.conflict-banner {
  padding: 10px 12px;
  border-radius: 8px;
  background: #fff1f0;
  color: #912018;
  border: 1px solid #fda29b;
}

.delivery-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.delivery-table th,
.delivery-table td {
  padding: 9px 8px;
  border-bottom: 1px solid #e7ecf4;
  text-align: left;
}

.change-tag {
  padding: 2px 8px;
  border-radius: 999px;
}

.change-tag.added {
  background: #ecfdf3;
  color: #027a48;
}

.change-tag.withdrawn {
  background: #fffaeb;
  color: #b54708;
}

.token-button {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
}

.delivery-replay p,
.snapshot-line {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
}

@media (max-width: 980px) {
  .delivery-form {
    grid-template-columns: 1fr;
  }

  .credential-field {
    grid-column: auto;
  }

  .overview-columns {
    grid-template-columns: 1fr;
  }
}
</style>
