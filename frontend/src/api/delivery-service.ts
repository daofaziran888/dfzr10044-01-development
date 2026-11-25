/** 交付视图接口：快照交接变更、页凭据确认、重连续取和交付回放。 */
import { request } from './client'

export type DeliveryPayload = Record<string, unknown>
export type DeliveryResult = Record<string, any>

async function readJson<T>(response: Response): Promise<T> {
  const data = (await response.json().catch(() => ({}))) as T
  if (!response.ok) {
    const detail = (data as { message?: string; error?: string } | null)?.message
      || (data as { error?: string } | null)?.error
      || `接口返回 ${response.status}`
    throw new Error(detail)
  }
  return data
}

export async function getDeliveryJson<T = DeliveryResult>(path: string): Promise<T> {
  const response = await request(path)
  return readJson<T>(response)
}

export async function postDeliveryJson<T = DeliveryResult>(
  path: string,
  payload: DeliveryPayload = {},
): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return readJson<T>(response)
}

export function refreshDelivery(payload: DeliveryPayload): Promise<DeliveryResult> {
  return postDeliveryJson('/api/publish_view/refresh', payload)
}

export function acknowledgeDelivery(payload: DeliveryPayload): Promise<DeliveryResult> {
  return postDeliveryJson('/api/publish_view/ack', payload)
}

export function reconnectDelivery(payload: DeliveryPayload): Promise<DeliveryResult> {
  return postDeliveryJson('/api/publish_view/reconnect', payload)
}

export function loadDeliveryOverview(params: URLSearchParams | string): Promise<DeliveryResult> {
  const query = typeof params === 'string' ? params : params.toString()
  return getDeliveryJson(`/api/publish_view/overview?${query}`)
}

export function loadDeliveryReplay(deliveryId: string, consumerId?: string): Promise<DeliveryResult> {
  const query = consumerId ? `?consumer_id=${encodeURIComponent(consumerId)}` : ''
  return getDeliveryJson(`/api/deliveries/${encodeURIComponent(deliveryId)}/replay${query}`)
}
