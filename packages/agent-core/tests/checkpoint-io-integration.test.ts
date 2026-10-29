import { afterEach, describe, expect, it, vi } from 'vitest'
import { mkdtemp, mkdir, writeFile, readFile, readdir, rm, access } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createHash } from 'node:crypto'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { finished } from 'node:stream/promises'
import { CheckpointManager } from '../src/main/agent/checkpoint/checkpoint-manager'
import { workspaceIo, writeIoFile, readIoFile, createIoWriteStream } from '../src/main/lib/workspace-io'

const roots: string[] = []
async function fixture() { const root = await mkdtemp(join(tmpdir(), 'checkpoint-io-')); roots.push(root); return root }
afterEach(async () => {
  for (const root of roots.splice(0)) {
    // Drain same-directory idle collection before removing only this test fixture.
    await workspaceIo.idle(root, async () => {})
    await rm(root, { recursive: true, force: true })
  }
})

describe('checkpoint queue with real files', () => {
  it('does not walk ignored files when Git reports an empty visible tree', async () => {
    const root = await fixture(), manager = new CheckpointManager()
    await promisify(execFile)('git', ['init', '-q'], { cwd: root })
    await promisify(execFile)('git', ['config', 'core.excludesFile', '.git/info/exclude'], { cwd: root })
    await writeFile(join(root, '.git/info/exclude'), '*.log\n')
    await writeFile(join(root, 'ignored.log'), 'not captured')
    const cp = await manager.createCheckpoint({ cwd: root, terminalId: 't', engine: 'manual', prompt: 'empty' })
    expect(cp.filesSnapshot).toEqual({})
  })

  it('preserves blob data if a checkpoint metadata file is unreadable', async () => {
    const root = await fixture(), manager = new CheckpointManager()
    const storage = join(root, '.janusX/checkpoints')
    await mkdir(join(storage, 'blobs'), { recursive: true })
    await writeFile(join(storage, 'unknown.json'), '{broken')
    await writeFile(join(storage, 'blobs', 'unknown-reference'), 'keep')
    await manager.initialize(root)
    await workspaceIo.idle(root, async () => {})
    expect(await readFile(join(storage, 'blobs', 'unknown-reference'), 'utf8')).toBe('keep')
  })

  it('cancels a queued capture without creating storage or publishing a checkpoint', async () => {
    const root = await fixture(), manager = new CheckpointManager(), abort = new AbortController()
    let release!: () => void
    const active = workspaceIo.run(root, () => new Promise<void>(resolve => { release = resolve }))
    const capture = manager.createCheckpoint({ cwd: root, terminalId: 't', engine: 'manual', prompt: 'cancel', signal: abort.signal })
    const rejected = expect(capture).rejects.toBeDefined()
    abort.abort()
    await rejected
    release()
    await active
    await expect(access(join(root, '.janusX'))).rejects.toThrow()
    expect(await manager.listCheckpoints({ cwd: root })).toEqual([])
  })

  it('lets a new checkpoint publish between GC batches without deleting its reused blob', async () => {
    const root = await fixture(), manager = new CheckpointManager()
    const storage = join(root, '.janusX/checkpoints')
    await mkdir(join(storage, 'blobs'), { recursive: true })
    const source = Buffer.from('retained source')
    const retainedHash = createHash('sha1').update(source).digest('hex')
    for (let i = 0; i < 65; i++) await writeFile(join(storage, 'blobs', `0-${i.toString().padStart(2, '0')}`), 'orphan')
    await writeFile(join(storage, 'blobs', retainedHash), source)
    await writeFile(join(root, 'source.txt'), source)
    await manager.initialize(root)
    const state = [...(manager as any).workspaceStates.values()][0]
    const removed: string[] = []
    const deletion = state.blobStore.delete.bind(state.blobStore)
    let release!: () => void
    const pause = new Promise<void>(resolve => { release = resolve })
    vi.spyOn(state.blobStore, 'delete').mockImplementation(async (hash: string) => {
      removed.push(hash)
      await deletion(hash)
      if (removed.length === 1) await pause
    })
    await vi.waitFor(() => expect(removed.length).toBe(1))
    const pendingCapture = manager.createCheckpoint({ cwd: root, terminalId: 't', engine: 'manual', prompt: 'reuse' })
    release()
    await pendingCapture
    await vi.waitFor(async () => expect(await readdir(join(storage, 'blobs'))).toEqual([retainedHash]))
    expect(await readFile(join(storage, 'blobs', retainedHash))).toEqual(source)
  })

  it('closes a stream destroyed while its first open is still waiting in the queue', async () => {
    const root = await fixture(), path = join(root, 'aborted.log')
    let release!: () => void
    const blocker = workspaceIo.run(root, () => new Promise<void>(resolve => { release = resolve }))
    const stream = createIoWriteStream(root, path)
    const done = finished(stream).catch(error => error)
    stream.write('header')
    stream.destroy()
    release()
    await blocker
    await done
    expect(stream.closed).toBe(true)
    // Windows refuses this removal while a file handle remains open.
    await rm(path)
  })

  it('shares cold initialization, lists metadata without a source scan or new directory', async () => {
    const root = await fixture(), manager = new CheckpointManager()
    const scan = vi.spyOn(manager as any, 'listSnapshotFilePaths')
    const load = vi.spyOn(manager as any, 'loadIndex')
    await Promise.all(Array.from({ length: 20 }, () => manager.initialize(root)))
    expect(load).toHaveBeenCalledTimes(1)
    expect(await manager.listCheckpoints({ cwd: root })).toEqual([])
    expect(await manager.getChangedFileCounts([], root)).toEqual({})
    expect(scan).not.toHaveBeenCalled()
    await expect(access(join(root, '.janusX'))).rejects.toThrow()
  })

  it('serializes 8 terminals, retains all published blobs across idle GC, and restores Unicode files', async () => {
    const root = await fixture(), manager = new CheckpointManager()
    await mkdir(join(root, 'src'))
    for (let i = 0; i < 70; i++) await writeFile(join(root, 'src', `${i}-中文.ts`), `export const n = ${i}\n`)
    const cps = await Promise.all(Array.from({ length: 8 }, (_, i) => manager.createCheckpoint({
      cwd: root, terminalId: `t${i}`, engine: 'manual', prompt: `turn ${i}`,
    })))
    expect(new Set(cps.map(cp => cp.conversationIndex)).size).toBe(8)
    expect(cps.every(cp => Object.keys(cp.filesSnapshot).length === 70)).toBe(true)
    await workspaceIo.idle(root, async () => {})
    for (const entry of Object.values(cps[7].filesSnapshot)) {
      expect(await readFile(join(root, '.janusX/checkpoints/blobs', entry.hash), 'utf8')).toContain('export const')
    }
    await writeFile(join(root, 'src', '0-中文.ts'), 'changed')
    const scans = vi.spyOn(manager as any, 'captureWorkspaceHashIndex')
    const counts = await Promise.all(Array.from({ length: 20 }, () => manager.getChangedFileCounts(cps.map(cp => cp.id), root)))
    expect(scans).toHaveBeenCalledTimes(1)
    expect(counts.every(count => count[cps[0].id] === 1)).toBe(true)
    await manager.restoreCheckpoint(cps[7].id, root)
    expect(await readFile(join(root, 'src', '0-中文.ts'), 'utf8')).toContain('export const n = 0')
  })

  it('does not publish a partial checkpoint when a file fails, and queue remains usable', async () => {
    const root = await fixture(), manager = new CheckpointManager()
    await writeFile(join(root, 'ok.txt'), 'ok')
    const blob = vi.spyOn(manager as any, 'addFileToSnapshot').mockRejectedValueOnce(new Error('disk failed'))
    await expect(manager.createCheckpoint({ cwd: root, terminalId: 't', engine: 'manual', prompt: 'fail' })).rejects.toThrow('disk failed')
    expect(await manager.listCheckpoints({ cwd: root })).toEqual([])
    blob.mockRestore()
    await manager.createCheckpoint({ cwd: root, terminalId: 't', engine: 'manual', prompt: 'retry' })
    expect(await manager.listCheckpoints({ cwd: root })).toHaveLength(1)
  })

  it('bounds growing file reads and atomically replaces chunked output without temporary leftovers', async () => {
    const root = await fixture(), path = join(root, 'large.log')
    const chunks = Array.from({ length: 40 }, (_, i) => Buffer.alloc(64 * 1024, i))
    await writeIoFile(root, path, chunks, true)
    const result = await readIoFile(path)
    expect(result.length).toBe(40 * 64 * 1024)
    expect(createHash('sha256').update(result).digest('hex')).toBe(createHash('sha256').update(Buffer.concat(chunks)).digest('hex'))
    await expect(readIoFile(path, 1024)).rejects.toThrow('capture limit')
    await writeIoFile(root, path, ['new'], true)
    expect(await readFile(path, 'utf8')).toBe('new')
    expect(await readdir(root)).toEqual(['large.log'])
  })
})
