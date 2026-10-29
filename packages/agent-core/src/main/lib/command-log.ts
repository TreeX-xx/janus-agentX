import { lstat, open, realpath } from 'node:fs/promises'
import { basename, join, relative, resolve } from 'node:path'
import type { RegisteredTool } from '../agent/runtime/registry'
import { resolveWorkspaceStorage, useWorkspaceStorage } from './workspace-storage'
import { workspaceIo } from './workspace-io'

/** IDs are filenames within one resolved workspace, never arbitrary userData paths. */
export const commandLogReadTool: RegisteredTool = {
  name: 'command.read-log',
  description: 'Read a bounded byte page from a command log in the active workspace. Pass logId from command.run or project.process-output; continue with nextOffset.',
  actionRisk: 'read',
  inputSchema: { type: 'object', properties: {
    workspaceId: { type: 'string' }, logId: { type: 'string' }, offset: { type: 'number' }, maxBytes: { type: 'number' },
  }, required: ['workspaceId', 'logId'], additionalProperties: false },
  async execute(input, context) {
    if (input.workspaceId !== context.workspaceId) throw new Error('Log belongs to another workspace')
    const id = input.logId
    if (typeof id !== 'string' || !/^[\w.-]+\.log$/.test(id) || basename(id) !== id) throw new Error('Invalid command log id')
    const offset = Number(input.offset ?? 0), maxBytes = Number(input.maxBytes ?? 16384)
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 65536) throw new Error('Invalid log page bounds')
    return workspaceIo.run(context.workspaceRoot, async () => {
      const release = useWorkspaceStorage(context.workspaceRoot)
      try {
        const storage = await resolveWorkspaceStorage(context.workspaceRoot)
        const root = await realpath(storage.logsPath), path = join(root, id)
        const normalized = (value: string) => process.platform === 'win32' ? resolve(value).toLowerCase() : resolve(value)
        if ((await lstat(storage.logsPath)).isSymbolicLink() || normalized(root) !== normalized(storage.logsPath)) throw new Error('Linked log directories are unavailable')
        if ((await lstat(path)).isSymbolicLink() || relative(root, await realpath(path)) !== id) throw new Error('Linked command logs are unavailable')
        const file = await open(path, 'r')
        try {
          const buffer = Buffer.allocUnsafe(maxBytes)
          const { bytesRead } = await file.read(buffer, 0, maxBytes, offset)
          const size = (await file.stat()).size
          return { workspaceId: context.workspaceId, logId: id, text: buffer.subarray(0, bytesRead).toString('utf8'), offset, nextOffset: offset + bytesRead, truncated: offset + bytesRead < size }
        } finally { await file.close() }
      } finally { release() }
    }, { signal: context.signal })
  },
}
