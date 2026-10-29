// Note: .agents/notes/agent/workspace-io.md
import { join } from 'node:path'
import { workspaceIoKey } from './workspace-io'

export interface WorkspaceStorage {
  root: string
  launchPath: string
  checkpointsPath: string
  logsPath: string
  retentionStartedAt?: string
}
export type WorkspaceStorageResolver = (cwd: string, write?: boolean) => Promise<WorkspaceStorage>
let resolver: WorkspaceStorageResolver | undefined
const users = new Map<string, number>()
const switching = new Set<string>()
const key = workspaceIoKey

/** Hosts configure once, before creating sessions. CLI defaults retain their own policy. */
export function configureWorkspaceStorage(value?: WorkspaceStorageResolver): void { resolver = value }
export function resolveWorkspaceStorage(cwd: string, write = false): Promise<WorkspaceStorage> {
  if (resolver) return resolver(cwd, write)
  const root = join(cwd, '.janusX')
  return Promise.resolve({ root, launchPath: join(root, 'janusX.launch.json'), checkpointsPath: join(root, 'checkpoints'), logsPath: join(root, 'logs') })
}

/** Protect long-lived command writers between cooperative queue operations. */
export function useWorkspaceStorage(cwd: string): () => void {
  assertWorkspaceStorageAvailable(cwd)
  const id = key(cwd)
  users.set(id, (users.get(id) ?? 0) + 1)
  let released = false
  return () => {
    if (released) return
    released = true
    const remaining = (users.get(id) ?? 1) - 1
    if (remaining) users.set(id, remaining)
    else users.delete(id)
  }
}
export function workspaceStorageInUse(cwd: string): boolean { return (users.get(key(cwd)) ?? 0) > 0 }
export function assertWorkspaceStorageAvailable(cwd: string): void {
  const path = key(cwd)
  if ([...switching].some(root => path === root || path.startsWith(root + (process.platform === 'win32' ? '\\' : '/')))) throw new Error('Workspace storage migration is running; retry after it finishes')
}
export function suspendWorkspaceStorage(cwd: string): () => void {
  assertWorkspaceStorageAvailable(cwd)
  if (workspaceStorageInUse(cwd)) throw new Error('Close workspace commands before migrating')
  const id = key(cwd)
  switching.add(id)
  return () => { switching.delete(id) }
}
