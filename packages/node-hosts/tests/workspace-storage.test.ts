import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { mkdtemp, mkdir, access, rm, realpath } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { configureWorkspaceStorage, commandLogReadTool, workspaceStorageInUse, suspendWorkspaceStorage } from '@janus-agent/agent-core'
import { JobManager } from '../src/jobs.js'
import { createCommandRunTool } from '../src/command.js'

let base: string, cwd: string, logs: string, manager: JobManager
beforeEach(async () => {
  base = await realpath(await mkdtemp(join(tmpdir(), 'janus-host-storage-')))
  cwd = join(base, 'project'); logs = join(base, 'profile', 'logs')
  await mkdir(cwd)
  configureWorkspaceStorage(async () => ({ root: join(base, 'profile'), logsPath: logs, checkpointsPath: join(base, 'profile/checkpoints'), launchPath: join(base, 'profile/launch.json') }))
  manager = new JobManager()
})
afterEach(async () => {
  await manager.dispose()
  await vi.waitFor(() => expect(workspaceStorageInUse(cwd)).toBe(false))
  configureWorkspaceStorage()
  await rm(base, { recursive: true, force: true })
})
function context() { return { workspaceId: 'w', workspaceRoot: cwd, signal: new AbortController().signal } }

it('runs sync commands in workspace cwd and reads external private logs through bounded IDs', async () => {
  const output = await createCommandRunTool(manager).execute({ workspaceId: 'w', program: 'node', args: ['-e', 'console.log(process.cwd())'] }, context()) as any
  expect(output.stdout.trim().toLowerCase()).toBe(cwd.toLowerCase())
  expect(output.logPath).toBeUndefined()
  expect(output.logReadTool).toBe('command.read-log')
  const page = await commandLogReadTool.execute({ workspaceId: 'w', logId: output.logId, maxBytes: 32 }, context()) as any
  expect(page.text).toContain('# command.run node')
  expect(page.nextOffset).toBe(32)
  expect(workspaceStorageInUse(cwd)).toBe(false)
  await expect(access(join(cwd, '.janusX'))).rejects.toThrow()
})

it('keeps background writers protected through log flush and uses the injected directory', async () => {
  const output = await createCommandRunTool(manager).execute({ workspaceId: 'w', program: 'node', background: true, args: ['-e', 'setTimeout(()=>console.log("private-background"),150)'] }, context()) as any
  expect(workspaceStorageInUse(cwd)).toBe(true)
  expect(() => suspendWorkspaceStorage(cwd)).toThrow('commands')
  await vi.waitFor(async () => expect((await manager.poll(output.projectId)).exited).toBe(true))
  await vi.waitFor(() => expect(workspaceStorageInUse(cwd)).toBe(false))
  const page = await commandLogReadTool.execute({ workspaceId: 'w', logId: output.logId }, context()) as any
  expect(page.text).toContain('private-background')
  expect(page.text).toContain('--- exit:')
  await expect(access(join(cwd, '.janusX'))).rejects.toThrow()
})

it('releases storage after background spawn failure and refuses startup during a switch', async () => {
  const resume = suspendWorkspaceStorage(cwd)
  await expect(manager.start({ workspaceRoot: cwd, cwd, cwdDisplay: '', program: 'node', args: [], env: {}, label: 'blocked' })).rejects.toThrow('migration')
  resume()
  const output = await manager.start({ workspaceRoot: cwd, cwd, cwdDisplay: '', program: 'not-a-real-janus-program', args: [], env: {}, label: 'failure' })
  await vi.waitFor(() => expect(workspaceStorageInUse(cwd)).toBe(false))
  expect((await manager.poll(output.projectId)).output.join('\n')).toContain('spawn error:')
})
