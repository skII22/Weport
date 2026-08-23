import { appendFileSync, existsSync, mkdirSync, renameSync, rmSync, statSync } from 'fs'
import { dirname, join } from 'path'
import { inspect } from 'util'
import { app } from 'electron'

const MAX_LOG_BYTES = 5 * 1024 * 1024
const MAX_LOG_FILES = 7
let installed = false
let logPath = ''
let approximateBytes = 0

function redact(text: string): string {
  return text
    .replace(/(authorization\s*[:=]\s*bearer\s+)[^\s,"']+/gi, '$1[REDACTED]')
    .replace(/((?:api[-_]?key|token|decryptKey|imageAesKey|password)\s*["']?\s*[:=]\s*["']?)[^\s,"'}]+/gi, '$1[REDACTED]')
    .replace(/(Bearer\s+)[A-Za-z0-9._~+/=-]+/g, '$1[REDACTED]')
    .replace(/((?:fileName|filename)\s*:\s*)'[^']*'/gi, "$1'[FILE]'")
    .replace(/((?:fileName|filename)\s*:\s*)"[^"]*"/gi, '$1"[FILE]"')
    .replace(/\b[a-f0-9]{32,64}\b/gi, '[HASH]')
    .replace(/(?<![A-Za-z])[A-Za-z]:[\\/][^\r\n,'"}\]]+/g, '[PATH]')
    .replace(/\/(?:Users|home)\/[^\r\n,'"}\]]+/g, '[PATH]')
}

function shouldPersist(args: unknown[]): boolean {
  const text = args.map(formatArg).join(' ')
  return !(
    text.includes('[Export][File] 附件候选未命中') ||
    text.includes('[ChatService] 表情包数据库未命中')
  )
}

function formatArg(value: unknown): string {
  if (typeof value === 'string') return value
  if (value instanceof Error) return value.stack || value.message
  return inspect(value, { depth: 4, maxArrayLength: 40, breakLength: 160, compact: true })
}

function rotateLogs(): void {
  if (!logPath) return
  try {
    if (!existsSync(logPath) || statSync(logPath).size < MAX_LOG_BYTES) return
    const oldest = `${logPath}.${MAX_LOG_FILES - 1}`
    if (existsSync(oldest)) rmSync(oldest, { force: true })
    for (let index = MAX_LOG_FILES - 2; index >= 1; index -= 1) {
      const source = `${logPath}.${index}`
      if (existsSync(source)) renameSync(source, `${logPath}.${index + 1}`)
    }
    renameSync(logPath, `${logPath}.1`)
    approximateBytes = 0
  } catch {
    // Runtime logging must never interrupt application startup or export.
  }
}

function append(level: string, args: unknown[]): void {
  if (!logPath) return
  try {
    if (!shouldPersist(args)) return
    if (approximateBytes >= MAX_LOG_BYTES) rotateLogs()
    const message = redact(args.map(formatArg).join(' ')).slice(0, 16_000)
    const line = `${new Date().toISOString()} [${level}] ${message}\n`
    appendFileSync(logPath, line, 'utf8')
    approximateBytes += Buffer.byteLength(line)
  } catch {
    // Do not write through console here: console is the source being wrapped.
  }
}

export function installRuntimeFileLogging(): string {
  if (installed) return logPath
  installed = true
  logPath = join(app.getPath('logs'), 'weport-main.log')
  mkdirSync(dirname(logPath), { recursive: true })
  approximateBytes = existsSync(logPath) ? statSync(logPath).size : 0
  rotateLogs()

  for (const level of ['log', 'warn', 'error'] as const) {
    const original = console[level].bind(console)
    console[level] = (...args: unknown[]) => {
      append(level.toUpperCase(), args)
      original(...args)
    }
  }
  append('INFO', [`[RuntimeLog] started pid=${process.pid} packaged=${app.isPackaged}`])
  return logPath
}

export function getRuntimeLogPath(): string {
  return logPath || join(app.getPath('logs'), 'weport-main.log')
}
