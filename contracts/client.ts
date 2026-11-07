// Generated from query-api/contracts/client.ts. Run query-api/scripts/sync-query-contract.py.
import registry from '../../contracts/query-v1.json'

export const QUERY_ENDPOINT = '/query/execute'
export interface UnifiedQuery {
  version: 'v1'
  signal: string
  operation: string
  time_range: { from: string; to: string }
  filters: unknown[]
  search: string | null
  options: Record<string, unknown>
}

/** Preserve legacy call signatures at the UI boundary, not on the wire. */
export function adaptQuery(path: string, body: Record<string, unknown>): UnifiedQuery | undefined {
  const fullPath = path.startsWith('/api/v1/') ? path : `/api/v1${path}`
  const route = registry.routes.find(r => r.path === fullPath)
  if (!route || !body.time_range) return undefined
  const { time_range, filters = [], search = null, ...options } = body
  let signal = route.signal
  if (signal === 'dynamic') {
    const key = route.operation === 'scatter' ? 'source' : 'signal'
    signal = String(options[key])
    delete options[key]
  }
  return { version: 'v1', signal, operation: route.operation,
    time_range: time_range as UnifiedQuery['time_range'], filters: filters as unknown[],
    search: search as string | null, options }
}

export function metricsQuery(query: string, start: number, end: number, step?: number): UnifiedQuery {
  const options: Record<string, unknown> = { query }
  if (step !== undefined) options.step = String(step)
  return { version: 'v1', signal: 'metrics', operation: step === undefined ? 'instant' : 'range',
    time_range: { from: new Date(start * 1000).toISOString(), to: new Date(end * 1000).toISOString() },
    filters: [], search: null, options }
}

export function decodeQuery<T>(envelope: unknown, request: UnifiedQuery): T {
  if (!envelope || typeof envelope !== 'object') throw new Error('Invalid query response')
  const result = envelope as Record<string, unknown>
  if (result.version !== request.version || result.signal !== request.signal
    || result.operation !== request.operation || !Object.hasOwn(result, 'data')) {
    throw new Error('Query response contract mismatch. Check your API version.')
  }
  return result.data as T
}
