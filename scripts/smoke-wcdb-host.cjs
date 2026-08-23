const fs = require('node:fs')
const path = require('node:path')
const { spawn } = require('node:child_process')

if (process.platform !== 'win32') {
  console.log('[smoke-wcdb-host] skipped: Windows only')
  process.exit(0)
}

const appRoot = path.resolve(process.argv[2] || path.join('release', 'win-unpacked'))
const appExe = path.join(appRoot, 'Weport.exe')
const hostExe = path.join(appRoot, 'WeFlow.exe')
const hostScript = path.join(appRoot, 'resources', 'host', 'wcdbHost.js')
const hostLibs = path.join(appRoot, 'resources', 'host', 'libs')
const resourcesPath = path.join(appRoot, 'resources', 'resources')
const required = [
  appExe,
  hostScript,
  path.join(hostLibs, 'koffi', 'package.json'),
  path.join(hostLibs, '@koromix', 'koffi-win32-x64'),
  path.join(resourcesPath, 'wcdb', 'win32', 'x64', 'wcdb_api.dll'),
]

for (const filePath of required) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Packaged WCDB host dependency is missing: ${filePath}`)
  }
}

if (fs.existsSync(hostExe)) fs.unlinkSync(hostExe)
fs.linkSync(appExe, hostExe)

const env = {
  ...process.env,
  ELECTRON_RUN_AS_NODE: '1',
  WEFLOW_WORKER: '1',
  WEPORT_RESOURCES_PATH: resourcesPath,
  WCDB_RESOURCES_PATH: resourcesPath,
  NODE_PATH: hostLibs,
  PATH: [
    appRoot,
    path.join(resourcesPath, 'wcdb', 'win32', 'x64'),
    path.join(resourcesPath, 'runtime', 'win32'),
    process.env.PATH || '',
  ].join(path.delimiter),
}
const child = spawn(hostExe, [hostScript], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'], windowsHide: true, env })
let stderr = ''
let stage = 0
let finished = false

function finish(error) {
  if (finished) return
  finished = true
  clearTimeout(timer)
  try { child.kill() } catch { /* noop */ }
  if (error) {
    console.error(`[smoke-wcdb-host] failed: ${error.message}${stderr ? `\n${stderr}` : ''}`)
    process.exitCode = 1
  } else {
    console.log('[smoke-wcdb-host] packaged host and WCDB DLL passed')
  }
}

child.stderr.on('data', (chunk) => { stderr += chunk.toString() })
child.on('error', finish)
child.on('exit', (code, signal) => {
  if (!finished) finish(new Error(`host exited early (code=${code}, signal=${signal || 'none'})`))
})
child.on('message', (message) => {
  if (message?.error) return finish(new Error(message.error))
  if (stage === 0) {
    stage = 1
    child.send({
      id: 2,
      type: 'testConnection',
      payload: { accountDir: path.join(appRoot, '__missing_account__'), hexKey: '00'.repeat(32) },
    })
    return
  }
  const expectedFailure = message?.result?.success === false && String(message?.result?.error || '').includes('-3001')
  finish(expectedFailure ? null : new Error(`unexpected DLL probe response: ${JSON.stringify(message)}`))
})

const timer = setTimeout(() => finish(new Error('host probe timed out after 10 seconds')), 10_000)
child.send({ id: 1, type: 'setPaths', payload: { resourcesPath, userDataPath: appRoot } })
