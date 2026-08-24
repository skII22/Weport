import { randomUUID } from 'crypto'
import { getProviderCatalogEntry } from './providerCatalog'
import type { ProviderAdapter, ProviderProfile, ProviderStreamInput, ProviderStreamResult } from './providerTypes'

const DEFAULT_HEADERS = { 'Content-Type': 'application/json' }

function normalizeBaseUrl(value: string): string {
  return String(value || '').trim().replace(/\/+$/, '')
}

function endpoint(baseUrl: string, path: string): string {
  const base = normalizeBaseUrl(baseUrl)
  if (!base) throw new Error('未配置服务地址')
  return `${base}${path.startsWith('/') ? path : `/${path}`}`
}

function providerError(payload: unknown): string {
  if (payload && typeof payload === 'object') {
    const record = payload as Record<string, unknown>
    const nested = record.error
    if (typeof nested === 'string' && nested.trim()) return nested.trim()
    if (nested && typeof nested === 'object') {
      const error = nested as Record<string, unknown>
      for (const value of [error.message, error.detail, error.error]) {
        if (typeof value === 'string' && value.trim()) return value.trim()
      }
    }
    for (const value of [record.message, record.detail]) {
      if (typeof value === 'string' && value.trim()) return value.trim()
    }
  }
  return String(payload || '').trim().slice(0, 1200)
}

async function readError(response: Response): Promise<never> {
  const raw = await response.text().catch(() => '')
  let detail = raw
  try { detail = providerError(JSON.parse(raw)) || raw } catch { /* plain text */ }
  const error = new Error(detail || `HTTP ${response.status}`)
  const typedError = error as Error & { status?: number; responseMeta?: ProviderStreamResult['responseMeta'] }
  typedError.status = response.status
  typedError.responseMeta = {
    status: response.status,
    contentType: response.headers.get('content-type') || '',
    bytes: new TextEncoder().encode(raw).byteLength,
    events: 0,
    parseMode: raw.trim() ? 'unknown' : 'empty',
  }
  throw error
}

async function requestJson(url: string, init: RequestInit): Promise<any> {
  const response = await fetch(url, init)
  if (!response.ok) return readError(response)
  return response.json()
}

function authHeaders(profile: ProviderProfile, kind: ProviderProfile['protocol']): Record<string, string> {
  const custom = profile.headers || {}
  if (kind === 'anthropic') {
    return { ...DEFAULT_HEADERS, 'x-api-key': profile.apiKey, 'anthropic-version': '2023-06-01', ...custom }
  }
  if (kind === 'google') {
    return { ...DEFAULT_HEADERS, 'x-goog-api-key': profile.apiKey, ...custom }
  }
  return { ...DEFAULT_HEADERS, ...(profile.apiKey ? { Authorization: `Bearer ${profile.apiKey}` } : {}), ...custom }
}

type ResponseMeta = NonNullable<ProviderStreamResult['responseMeta']>

function responseMeta(response: Response): ResponseMeta {
  return {
    status: response.status,
    contentType: response.headers.get('content-type') || '',
    bytes: 0,
    events: 0,
    parseMode: 'unknown',
  }
}

function responseParseError(message: string, meta: ResponseMeta): Error {
  const error = new Error(message) as Error & { status?: number; responseMeta?: ResponseMeta }
  error.status = meta.status
  error.responseMeta = { ...meta }
  return error
}

async function* sseEvents(response: Response, meta: ResponseMeta): AsyncGenerator<{ event: string; data: any }> {
  meta.parseMode = 'sse'
  if (!response.body) return
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let eventName = ''
  let dataLines: string[] = []
  const flush = async function* (): AsyncGenerator<{ event: string; data: any }> {
    if (dataLines.length === 0) return
    const raw = dataLines.join('\n').trim()
    dataLines = []
    const event = eventName
    eventName = ''
    if (!raw || raw === '[DONE]') return
    try {
      const data = JSON.parse(raw)
      meta.events += 1
      yield { event, data }
    } catch { /* ignore malformed provider fragments when other events remain valid */ }
  }
  while (true) {
    const { value, done } = await reader.read()
    meta.bytes += value?.byteLength || 0
    buffer += decoder.decode(value, { stream: !done })
    const lines = buffer.split(/\r?\n/)
    buffer = lines.pop() || ''
    for (const line of lines) {
      if (line === '') {
        for await (const item of flush()) yield item
      } else if (line.startsWith('event:')) {
        eventName = line.slice(6).trim()
      } else if (line.startsWith('data:')) {
        dataLines.push(line.slice(5).trim())
      }
    }
    if (done) break
  }
  if (buffer.startsWith('data:')) dataLines.push(buffer.slice(5).trim())
  for await (const item of flush()) yield item
  if (meta.bytes > 0 && meta.events === 0) {
    throw responseParseError('AI 服务返回了无法解析的 SSE 响应', meta)
  }
}

function parseBufferedEvents(raw: string, meta: ResponseMeta): Array<{ event: string; data: any }> {
  const text = raw.trim()
  if (!text) {
    meta.parseMode = 'empty'
    return []
  }

  try {
    meta.parseMode = 'json'
    const parsed = JSON.parse(text)
    const payloads = Array.isArray(parsed) ? parsed : [parsed]
    meta.events = payloads.length
    return payloads.map((data) => ({ event: '', data }))
  } catch { /* try line-oriented formats below */ }

  const events: Array<{ event: string; data: any }> = []
  if (/^(?:event|data):/m.test(text)) {
    meta.parseMode = 'sse'
    let event = ''
    let dataLines: string[] = []
    const flush = () => {
      const data = dataLines.join('\n').trim()
      dataLines = []
      if (!data || data === '[DONE]') { event = ''; return }
      try { events.push({ event, data: JSON.parse(data) }) } catch { /* checked after parsing */ }
      event = ''
    }
    for (const line of raw.split(/\r?\n/)) {
      if (!line.trim()) flush()
      else if (line.startsWith('event:')) event = line.slice(6).trim()
      else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim())
    }
    flush()
  } else {
    meta.parseMode = 'ndjson'
    for (const line of raw.split(/\r?\n/).map((item) => item.trim()).filter(Boolean)) {
      try { events.push({ event: '', data: JSON.parse(line) }) } catch { /* checked after parsing */ }
    }
  }
  meta.events = events.length
  if (events.length === 0) throw responseParseError('AI 服务返回了无法识别的响应格式', meta)
  return events
}

async function* providerEvents(response: Response, meta: ResponseMeta): AsyncGenerator<{ event: string; data: any }> {
  if (/text\/event-stream/i.test(meta.contentType)) {
    for await (const item of sseEvents(response, meta)) yield item
    return
  }
  const bytes = new Uint8Array(await response.arrayBuffer())
  meta.bytes = bytes.byteLength
  const raw = new TextDecoder().decode(bytes)
  for (const item of parseBufferedEvents(raw, meta)) yield item
}

function emptyResult(): ProviderStreamResult {
  return { content: '', reasoning: '', toolCalls: [], usage: undefined }
}

function contentText(value: unknown): string {
  if (typeof value === 'string') return value
  if (!Array.isArray(value)) return ''
  return value.map((part) => {
    if (!part || typeof part !== 'object') return ''
    const record = part as Record<string, unknown>
    return typeof record.text === 'string' ? record.text : typeof record.content === 'string' ? record.content : ''
  }).join('')
}

function usageFromOpenAI(usage: any) {
  if (!usage) return undefined
  return {
    promptTokens: Number(usage.prompt_tokens ?? usage.input_tokens) || 0,
    completionTokens: Number(usage.completion_tokens ?? usage.output_tokens) || 0,
    reasoningTokens: Number(usage.completion_tokens_details?.reasoning_tokens ?? usage.output_tokens_details?.reasoning_tokens) || 0,
    totalTokens: Number(usage.total_tokens) || 0,
    promptCacheHitTokens: Number(usage.prompt_tokens_details?.cached_tokens ?? usage.input_tokens_details?.cached_tokens) || 0,
  }
}

function toolDefinitions(input: ProviderStreamInput) {
  return input.tools.map((tool) => ({
    type: 'function' as const,
    function: {
      name: tool.function.name,
      description: tool.function.description,
      parameters: tool.function.parameters,
    },
  }))
}

function openAIInput(messages: Array<Record<string, unknown>>): { instructions: string; input: Array<Record<string, unknown>> } {
  const system = messages.filter((message) => message.role === 'system').map((message) => String(message.content || '')).filter(Boolean).join('\n\n')
  const input: Array<Record<string, unknown>> = []
  for (const message of messages) {
    const role = String(message.role || '')
    if (role === 'system') continue
    if (role === 'tool') {
      input.push({ type: 'function_call_output', call_id: String(message.tool_call_id || ''), output: String(message.content || '') })
      continue
    }
    if (role === 'assistant' && Array.isArray(message.tool_calls)) {
      if (message.content) input.push({ role: 'assistant', content: String(message.content) })
      for (const call of message.tool_calls as Array<Record<string, any>>) {
        input.push({
          type: 'function_call',
          call_id: String(call.id || randomUUID()),
          name: String(call.function?.name || ''),
          arguments: String(call.function?.arguments || '{}'),
        })
      }
      continue
    }
    input.push({ role: role === 'assistant' ? 'assistant' : 'user', content: String(message.content || '') })
  }
  return { instructions: system, input }
}

function appendOpenAIResponseOutput(
  payload: Record<string, any>,
  result: ProviderStreamResult,
  calls: Map<string, { id: string; name: string; args: string }>,
  input: ProviderStreamInput
): void {
  if (!Array.isArray(payload.output) && typeof payload.output_text === 'string') {
    result.content += payload.output_text
    input.onText(payload.output_text)
  }
  for (const output of Array.isArray(payload.output) ? payload.output : []) {
    if (output?.type === 'function_call') {
      const key = String(output.id || output.call_id || randomUUID())
      const id = String(output.call_id || output.id || key)
      calls.set(key, { id, name: String(output.name || ''), args: typeof output.arguments === 'string' ? output.arguments : JSON.stringify(output.arguments || {}) })
      continue
    }
    const parts = Array.isArray(output?.content) ? output.content : Array.isArray(output?.summary) ? output.summary : []
    for (const part of parts) {
      const text = contentText(part?.text ?? part?.content ?? part?.refusal)
      if (!text) continue
      if (output?.type === 'reasoning' || part?.type === 'reasoning_text' || part?.type === 'summary_text') {
        result.reasoning += text
        input.onReasoning(text)
      } else {
        result.content += text
        input.onText(text)
      }
    }
  }
  result.usage = usageFromOpenAI(payload.usage) || result.usage
  result.finishReason = String(payload.status || result.finishReason || '')
}

const openAIResponsesAdapter: ProviderAdapter = {
  async stream(input) {
    const { instructions, input: requestInput } = openAIInput(input.messages)
    const body: Record<string, unknown> = {
      model: input.profile.model,
      input: requestInput,
      stream: true,
      tools: input.tools.map((tool) => ({ type: 'function', name: tool.function.name, description: tool.function.description, parameters: tool.function.parameters })),
    }
    if (input.maxOutputTokens !== undefined) body.max_output_tokens = input.maxOutputTokens
    if (instructions) body.instructions = instructions
    const response = await fetch(endpoint(input.profile.baseUrl, '/responses'), {
      method: 'POST', headers: authHeaders(input.profile, 'openai'), body: JSON.stringify(body), signal: input.signal,
    })
    if (!response.ok) return readError(response)
    const result = emptyResult()
    const meta = responseMeta(response)
    const calls = new Map<string, { id: string; name: string; args: string }>()
    for await (const item of providerEvents(response, meta)) {
      const data = item.data as Record<string, any>
      if (data.error) throw responseParseError(providerError(data) || 'AI 服务返回错误', meta)
      if (item.event === 'response.output_text.delta' || data.type === 'response.output_text.delta') {
        const text = String(data.delta || '')
        result.content += text
        input.onText(text)
      } else if (item.event === 'response.reasoning_summary_text.delta' || data.type === 'response.reasoning_summary_text.delta') {
        const text = String(data.delta || '')
        result.reasoning += text
        input.onReasoning(text)
      } else if (item.event === 'response.output_item.added' || data.type === 'response.output_item.added') {
        const output = data.item
        if (output?.type === 'function_call') {
          const key = String(output.id || output.call_id || randomUUID())
          const id = String(output.call_id || output.id || key)
          calls.set(key, { id, name: String(output.name || ''), args: String(output.arguments || '') })
        }
      } else if (item.event === 'response.function_call_arguments.delta' || data.type === 'response.function_call_arguments.delta') {
        const id = String(data.call_id || data.item_id || '')
        const call = calls.get(id)
        if (call) call.args += String(data.delta || '')
      } else if (item.event === 'response.completed' || data.type === 'response.completed') {
        result.usage = usageFromOpenAI(data.response?.usage || data.usage)
        result.finishReason = String(data.response?.status || 'completed')
        if (!result.content && !result.reasoning && calls.size === 0 && data.response) {
          appendOpenAIResponseOutput(data.response, result, calls, input)
        }
      } else if (Array.isArray(data.output) || typeof data.output_text === 'string') {
        appendOpenAIResponseOutput(data, result, calls, input)
      }
    }
    result.toolCalls = Array.from(calls.values()).filter((call) => call.name).map((call) => ({ id: call.id, name: call.name, args: parseArgs(call.args) }))
    result.responseMeta = meta
    return result
  },
  async listModels(profile, signal) { return listOpenAIModels(profile, signal) },
}

function parseArgs(value: string): Record<string, unknown> {
  try { return JSON.parse(value || '{}') as Record<string, unknown> } catch { return { _raw: value } }
}

function openAIChatBody(input: ProviderStreamInput): Record<string, unknown> {
  const body: Record<string, unknown> = {
    model: input.profile.model,
    messages: input.messages,
    stream: true,
  }
  if (input.maxOutputTokens !== undefined) body.max_tokens = input.maxOutputTokens
  if (input.tools.length > 0) {
    body.tools = toolDefinitions(input)
    body.tool_choice = 'auto'
  }
  if (input.profile.providerId === 'deepseek' || /deepseek/i.test(input.profile.model)) {
    body.stream_options = { include_usage: true }
    body.reasoning_effort = input.reasoningEffort
  }
  return body
}

const openAICompatibleAdapter: ProviderAdapter = {
  async stream(input) {
    const response = await fetch(endpoint(input.profile.baseUrl, '/chat/completions'), {
      method: 'POST', headers: authHeaders(input.profile, 'openai-compatible'), body: JSON.stringify(openAIChatBody(input)), signal: input.signal,
    })
    if (!response.ok) return readError(response)
    const result = emptyResult()
    const meta = responseMeta(response)
    const calls = new Map<number, { id: string; name: string; args: string }>()
    for await (const item of providerEvents(response, meta)) {
      const rawChunk = item.data as any
      const chunk = rawChunk?.data?.choices ? rawChunk.data : rawChunk
      if (chunk?.error) throw responseParseError(providerError(chunk) || 'AI 服务返回错误', meta)
      result.usage = usageFromOpenAI(chunk?.usage) || result.usage
      const choice = chunk?.choices?.[0]
      if (!choice) continue
      result.finishReason = choice.finish_reason || result.finishReason
      const delta = choice.delta || choice.message || choice
      const text = contentText(delta.content ?? delta.text)
      if (text) { result.content += text; input.onText(text) }
      const reasoning = contentText(delta.reasoning_content ?? delta.reasoning)
      if (reasoning) { result.reasoning += reasoning; input.onReasoning(reasoning) }
      for (const toolCall of Array.isArray(delta.tool_calls) ? delta.tool_calls : []) {
        const index = Number(toolCall.index ?? 0)
        const current = calls.get(index) || { id: String(toolCall.id || randomUUID()), name: '', args: '' }
        if (toolCall.id) current.id = String(toolCall.id)
        if (toolCall.function?.name) current.name += String(toolCall.function.name)
        if (typeof toolCall.function?.arguments === 'string') current.args += toolCall.function.arguments
        else if (toolCall.function?.arguments && typeof toolCall.function.arguments === 'object') current.args = JSON.stringify(toolCall.function.arguments)
        calls.set(index, current)
      }
      if (delta.function_call?.name) {
        const current = calls.get(0) || { id: `call_${randomUUID()}`, name: '', args: '' }
        current.name += String(delta.function_call.name)
        if (typeof delta.function_call.arguments === 'string') current.args += delta.function_call.arguments
        calls.set(0, current)
      }
    }
    result.toolCalls = Array.from(calls.values()).filter((call) => call.name).map((call) => ({ id: call.id, name: call.name, args: parseArgs(call.args) }))
    result.responseMeta = meta
    return result
  },
  async listModels(profile, signal) { return listOpenAIModels(profile, signal) },
}

async function listOpenAIModels(profile: ProviderProfile, signal?: AbortSignal): Promise<string[]> {
  const payload = await requestJson(endpoint(profile.baseUrl, '/models'), { method: 'GET', headers: authHeaders(profile, 'openai-compatible'), signal })
  const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload?.models) ? payload.models : []
  return Array.from(new Set(rows.map((item: any) => String(item?.id || item?.name || '').replace(/^models\//, '').trim()).filter(Boolean)))
}

function anthropicMessages(messages: Array<Record<string, unknown>>) {
  let system = ''
  const result: Array<Record<string, unknown>> = []
  for (const message of messages) {
    const role = String(message.role || '')
    if (role === 'system') { system += `${system ? '\n\n' : ''}${String(message.content || '')}`; continue }
    if (role === 'tool') {
      result.push({ role: 'user', content: [{ type: 'tool_result', tool_use_id: String(message.tool_call_id || ''), content: String(message.content || '') }] })
      continue
    }
    if (role === 'assistant' && Array.isArray(message.tool_calls)) {
      const content: Array<Record<string, unknown>> = []
      if (message.content) content.push({ type: 'text', text: String(message.content) })
      for (const call of message.tool_calls as Array<Record<string, any>>) {
        content.push({ type: 'tool_use', id: String(call.id || randomUUID()), name: String(call.function?.name || ''), input: parseArgs(String(call.function?.arguments || '{}')) })
      }
      result.push({ role: 'assistant', content })
      continue
    }
    result.push({ role: role === 'assistant' ? 'assistant' : 'user', content: String(message.content || '') })
  }
  return { system, messages: result }
}

const anthropicAdapter: ProviderAdapter = {
  async stream(input) {
    const converted = anthropicMessages(input.messages)
    const body: Record<string, unknown> = {
      model: input.profile.model,
      messages: converted.messages,
      stream: true,
    }
    // Anthropic currently requires max_tokens in Messages requests. This is
    // an adapter protocol requirement, not a user-facing Weport setting.
    body.max_tokens = input.maxOutputTokens ?? 32768
    if (converted.system) body.system = converted.system
    if (input.tools.length > 0) body.tools = input.tools.map((tool) => ({ name: tool.function.name, description: tool.function.description, input_schema: tool.function.parameters }))
    const response = await fetch(endpoint(input.profile.baseUrl, '/messages'), {
      method: 'POST', headers: authHeaders(input.profile, 'anthropic'), body: JSON.stringify(body), signal: input.signal,
    })
    if (!response.ok) return readError(response)
    const result = emptyResult()
    const meta = responseMeta(response)
    const calls = new Map<number, { id: string; name: string; args: string }>()
    for await (const item of providerEvents(response, meta)) {
      const data = item.data as any
      if (data?.type === 'error' || data?.error) throw responseParseError(providerError(data) || 'AI 服务返回错误', meta)
      if (data.type === 'message_start') result.usage = usageFromAnthropic(data.message?.usage)
      if (data.type === 'content_block_start' && data.content_block?.type === 'tool_use') {
        calls.set(Number(data.index), { id: String(data.content_block.id || randomUUID()), name: String(data.content_block.name || ''), args: '' })
      }
      if (data.type === 'content_block_delta') {
        if (data.delta?.type === 'text_delta') { const text = String(data.delta.text || ''); result.content += text; input.onText(text) }
        if (data.delta?.type === 'thinking_delta') { const text = String(data.delta.thinking || ''); result.reasoning += text; input.onReasoning(text) }
        if (data.delta?.type === 'input_json_delta') { const call = calls.get(Number(data.index)); if (call) call.args += String(data.delta.partial_json || '') }
      }
      if (data.type === 'message_delta') {
        result.finishReason = String(data.delta?.stop_reason || '')
        if (data.usage) result.usage = { ...(result.usage || emptyUsage()), ...usageFromAnthropic(data.usage) }
      }
      if (data.type === 'message' && Array.isArray(data.content)) {
        for (const [index, block] of data.content.entries()) {
          if (block?.type === 'text' && typeof block.text === 'string') { result.content += block.text; input.onText(block.text) }
          if (block?.type === 'thinking' && typeof block.thinking === 'string') { result.reasoning += block.thinking; input.onReasoning(block.thinking) }
          if (block?.type === 'tool_use') calls.set(index, { id: String(block.id || randomUUID()), name: String(block.name || ''), args: JSON.stringify(block.input || {}) })
        }
        result.finishReason = String(data.stop_reason || result.finishReason || '')
        result.usage = usageFromAnthropic(data.usage) || result.usage
      }
    }
    result.toolCalls = Array.from(calls.values()).filter((call) => call.name).map((call) => ({ id: call.id, name: call.name, args: parseArgs(call.args) }))
    result.responseMeta = meta
    return result
  },
  async listModels(profile, signal) {
    const payload = await requestJson(endpoint(profile.baseUrl, '/models'), { method: 'GET', headers: authHeaders(profile, 'anthropic'), signal })
    const rows = Array.isArray(payload?.data) ? payload.data : []
    return Array.from(new Set(rows.map((item: any) => String(item?.id || '').trim()).filter(Boolean)))
  },
}

function emptyUsage() { return { promptTokens: 0, completionTokens: 0, reasoningTokens: 0, totalTokens: 0, promptCacheHitTokens: 0 } }

function usageFromAnthropic(usage: any) {
  if (!usage) return undefined
  const prompt = Number(usage.input_tokens) || 0
  const completion = Number(usage.output_tokens) || 0
  return { promptTokens: prompt, completionTokens: completion, reasoningTokens: 0, totalTokens: prompt + completion, promptCacheHitTokens: Number(usage.cache_read_input_tokens) || 0 }
}

function googleContents(messages: Array<Record<string, unknown>>) {
  let system = ''
  const contents: Array<Record<string, unknown>> = []
  for (const message of messages) {
    const role = String(message.role || '')
    if (role === 'system') { system += `${system ? '\n\n' : ''}${String(message.content || '')}`; continue }
    const parts: Array<Record<string, unknown>> = []
    if (role === 'tool') {
      parts.push({ functionResponse: { name: String(message.toolName || message.tool_call_id || 'tool'), response: { result: String(message.content || '') } } })
      contents.push({ role: 'user', parts })
      continue
    }
    if (role === 'assistant' && Array.isArray(message.tool_calls)) {
      if (message.content) parts.push({ text: String(message.content) })
      for (const call of message.tool_calls as Array<Record<string, any>>) parts.push({ functionCall: { name: String(call.function?.name || ''), args: parseArgs(String(call.function?.arguments || '{}')) } })
      contents.push({ role: 'model', parts })
      continue
    }
    parts.push({ text: String(message.content || '') })
    contents.push({ role: role === 'assistant' ? 'model' : 'user', parts })
  }
  return { system, contents }
}

const googleAdapter: ProviderAdapter = {
  async stream(input) {
    const converted = googleContents(input.messages)
    const body: Record<string, unknown> = { contents: converted.contents }
    if (converted.system) body.systemInstruction = { parts: [{ text: converted.system }] }
    if (input.maxOutputTokens !== undefined) body.generationConfig = { maxOutputTokens: input.maxOutputTokens }
    if (input.tools.length > 0) body.tools = [{ functionDeclarations: input.tools.map((tool) => ({ name: tool.function.name, description: tool.function.description, parameters: tool.function.parameters })) }]
    const url = endpoint(input.profile.baseUrl, `/models/${encodeURIComponent(input.profile.model)}:streamGenerateContent?alt=sse`)
    const response = await fetch(url, { method: 'POST', headers: authHeaders(input.profile, 'google'), body: JSON.stringify(body), signal: input.signal })
    if (!response.ok) return readError(response)
    const result = emptyResult()
    const meta = responseMeta(response)
    for await (const item of providerEvents(response, meta)) {
      const payload = item.data as any
      if (payload?.error) throw responseParseError(providerError(payload) || 'AI 服务返回错误', meta)
      const parts = payload?.candidates?.[0]?.content?.parts || []
      for (const part of parts) {
        if (part.thought === true && typeof part.text === 'string') { result.reasoning += part.text; input.onReasoning(part.text) }
        else if (typeof part.text === 'string') { result.content += part.text; input.onText(part.text) }
        if (part.functionCall) result.toolCalls.push({ id: `call_${randomUUID()}`, name: String(part.functionCall.name || ''), args: (part.functionCall.args || {}) as Record<string, unknown> })
      }
      const usage = payload?.usageMetadata
      if (usage) result.usage = { promptTokens: Number(usage.promptTokenCount) || 0, completionTokens: Number(usage.candidatesTokenCount) || 0, reasoningTokens: Number(usage.thoughtsTokenCount) || 0, totalTokens: Number(usage.totalTokenCount) || 0, promptCacheHitTokens: Number(usage.cachedContentTokenCount) || 0 }
      result.finishReason = String(payload?.candidates?.[0]?.finishReason || result.finishReason || '')
    }
    result.responseMeta = meta
    return result
  },
  async listModels(profile, signal) {
    const payload = await requestJson(endpoint(profile.baseUrl, '/models'), { method: 'GET', headers: authHeaders(profile, 'google'), signal })
    const rows = Array.isArray(payload?.models) ? payload.models : []
    return Array.from(new Set(rows.map((item: any) => String(item?.name || '').replace(/^models\//, '').trim()).filter(Boolean)))
  },
}

export function getProviderAdapter(profile: ProviderProfile): ProviderAdapter {
  if (profile.protocol === 'openai') return openAIResponsesAdapter
  if (profile.protocol === 'anthropic') return anthropicAdapter
  if (profile.protocol === 'google') return googleAdapter
  return openAICompatibleAdapter
}

export function makeDefaultProfile(input: { providerId: string; protocol?: ProviderProfile['protocol']; name?: string; baseUrl?: string; model?: string; apiKey?: string }): ProviderProfile {
  const catalog = getProviderCatalogEntry(input.providerId)
  const protocol = input.protocol || catalog?.protocol || 'openai-compatible'
  return {
    id: `profile-${randomUUID()}`,
    name: input.name || catalog?.name || 'AI 服务',
    providerId: input.providerId || 'custom',
    protocol,
    baseUrl: normalizeBaseUrl(input.baseUrl || catalog?.baseUrl || ''),
    model: input.model || catalog?.defaultModel || '',
    apiKey: input.apiKey || '',
    createdAt: Date.now(),
    updatedAt: Date.now(),
  }
}
