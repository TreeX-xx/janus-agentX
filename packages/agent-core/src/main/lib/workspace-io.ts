// Note: .agents/notes/agent/workspace-io.md
import { AsyncLocalStorage } from 'node:async_hooks'
import { setImmediate as immediate, setTimeout as delay } from 'node:timers/promises'
import { open, rename, unlink } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import { resolve, dirname } from 'node:path'
import { Writable } from 'node:stream'
import { realpathSync } from 'node:fs'
import type { FileHandle } from 'node:fs/promises'

export const IO_BATCH_FILES = 32
export const IO_WAITING_TASKS = 64
export const IO_BACKGROUND_REST_MS = 20

/** Existing directory aliases share one transaction; missing paths retain a stable lexical key. */
export function workspaceIoKey(path: string): string {
  let absolute = resolve(path)
  try { absolute = realpathSync.native(absolute) } catch { /* Not created or unavailable yet. */ }
  return process.platform === 'win32' ? absolute.toLowerCase() : absolute
}

export interface IoOptions { signal?: AbortSignal; background?: boolean; releaseBetweenBatches?: boolean }
export interface IoBatch {
  check(): void
  step(): Promise<void>
  yield(): Promise<void>
  each<T>(items: Iterable<T> | AsyncIterable<T>, visit: (item: T) => Promise<void>): Promise<void>
}
interface Job {
  key: string
  options: IoOptions
  start: () => void
  resume?: () => void
  cleanup: () => void
  begun: boolean
}

/** Cooperative batches hold their directory transaction, but release the I/O lane.
 * Nested operations reuse the transaction; callers must not spawn detached work in it.
 */
export class WorkspaceIoQueue {
  private context = new AsyncLocalStorage<IoBatch>()
  private jobs: Job[] = []
  private owners = new Map<string, Job>()
  private busy = false
  private outstanding = 0
  private backgroundRest: Promise<void> | undefined

  run<T>(key: string, operation: (batch: IoBatch) => Promise<T>, options: IoOptions = {}): Promise<T> {
    key = workspaceIoKey(key)
    const nested = this.context.getStore()
    if (nested) { nested.check(); return operation(nested) }
    if (options.signal?.aborted) return Promise.reject(options.signal.reason ?? new Error('I/O cancelled'))
    if (this.outstanding >= IO_WAITING_TASKS + 1) return Promise.reject(new Error('Workspace I/O busy (64 waiting tasks)'))
    this.outstanding++
    return new Promise<T>((resolve, reject) => {
      const job: Job = {
        key, options, begun: false, cleanup: () => {},
        start: () => {
          let steps = 0
          const batch: IoBatch = {
            check: () => { options.signal?.throwIfAborted() },
            step: async () => { batch.check(); if (++steps % IO_BATCH_FILES === 0) await batch.yield() },
            yield: async () => {
              batch.check()
              if (options.background && !this.backgroundRest) {
                this.backgroundRest = delay(IO_BACKGROUND_REST_MS).then(() => { this.backgroundRest = undefined; this.pump() })
              }
              await new Promise<void>(resume => {
                job.resume = resume
                this.jobs.push(job)
                if (options.releaseBetweenBatches) this.owners.delete(key)
                this.busy = false
                // Always return control to the event loop, even with no other jobs.
                void immediate().then(() => this.pump())
              })
              batch.check()
            },
            each: async (items, visit) => {
              let count = 0
              for await (const item of items) {
                batch.check()
                await visit(item)
                if (++count % IO_BATCH_FILES === 0) await batch.yield()
              }
            },
          }
          void this.context.run(batch, async () => {
            try { batch.check(); resolve(await operation(batch)) } catch (error) { reject(error) }
            finally {
              job.cleanup()
              this.outstanding--
              this.owners.delete(key)
              this.busy = false
              this.pump()
            }
          })
        },
      }
      const abort = () => {
        if (job.begun) { this.pump(); return }
        const index = this.jobs.indexOf(job)
        if (index >= 0) this.jobs.splice(index, 1)
        job.cleanup()
        this.outstanding--
        reject(options.signal?.reason ?? new Error('I/O cancelled'))
        this.pump()
      }
      options.signal?.addEventListener('abort', abort, { once: true })
      job.cleanup = () => options.signal?.removeEventListener('abort', abort)
      this.jobs.push(job)
      this.pump()
    })
  }

  private pump(): void {
    if (this.busy) return
    const runnable = (job: Job) => (!this.owners.has(job.key) || this.owners.get(job.key) === job)
      && (!job.options.background || !this.backgroundRest || job.options.signal?.aborted)
    // Idle work never overtakes waiting foreground work. Same-directory owners
    // can finish their current transaction so a foreground waiter cannot deadlock GC.
    let index = this.jobs.findIndex(job => runnable(job) && !job.options.background)
    if (index < 0) index = this.jobs.findIndex(runnable)
    if (index < 0) return
    const [job] = this.jobs.splice(index, 1)
    this.busy = true
    if (job.resume) { this.owners.set(job.key, job); const resume = job.resume; job.resume = undefined; resume() }
    else { job.begun = true; this.owners.set(job.key, job); job.start() }
  }

  /** Drops inherited transaction context before scheduling later idle work. */
  idle<T>(key: string, operation: (batch: IoBatch) => Promise<T>): Promise<T> {
    return this.context.exit(() => this.run(key, operation, { background: true, releaseBetweenBatches: true }))
  }

  async step(): Promise<void> { await this.context.getStore()?.step() }
  async yield(): Promise<void> { await this.context.getStore()?.yield() }
}

export const workspaceIo = new WorkspaceIoQueue()

/** Bounded buffers, serialized through the same lane; no giant exit-time concat. */
export async function writeIoFile(key: string, path: string, chunks: Iterable<string | Uint8Array>, atomic = false): Promise<void> {
  await workspaceIo.run(key, async batch => {
    const temporary = atomic ? `${path}.${randomUUID()}.tmp` : path
    const handle = await open(temporary, 'w')
    try {
      let sinceYield = 0
      for (const chunk of chunks) {
        const bytes = typeof chunk === 'string' ? Buffer.from(chunk) : chunk
        for (let offset = 0; offset < bytes.byteLength; offset += 64 * 1024) {
          batch.check()
          const end = Math.min(bytes.byteLength, offset + 64 * 1024)
          let written = offset
          while (written < end) {
            const result = await handle.write(bytes, written, end - written)
            if (!result.bytesWritten) throw new Error('I/O write made no progress')
            written += result.bytesWritten
          }
          sinceYield += end - offset
          if (sinceYield >= 64 * 1024) { sinceYield = 0; await batch.yield() }
        }
      }
      await batch.step()
    } catch (error) {
      await handle.close()
      if (atomic) await unlink(temporary).catch(() => {})
      throw error
    }
    await handle.close()
    if (atomic) await rename(temporary, path).catch(async error => { await unlink(temporary).catch(() => {}); throw error })
  })
}

/** Source files are bounded by the caller's existing file-size policy. */
export async function readIoFile(path: string, maximumBytes = Infinity): Promise<Buffer> {
  return workspaceIo.run(dirname(path), async batch => {
    const handle = await open(path, 'r')
    try {
      const chunks: Buffer[] = []
      let total = 0
      for (;;) {
        batch.check()
        const buffer = Buffer.allocUnsafe(64 * 1024)
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, null)
        if (!bytesRead) break
        total += bytesRead
        if (total > maximumBytes) throw new Error(`File grew beyond capture limit: ${path}`)
        chunks.push(buffer.subarray(0, bytesRead))
        if (bytesRead === buffer.length) await batch.yield()
      }
      return Buffer.concat(chunks)
    } finally { await handle.close() }
  })
}

/** Standard Writable backpressure bounds pending log data while the lane is busy. */
export function createIoWriteStream(key: string, path: string): Writable {
  let handle: FileHandle | undefined
  let pending: Promise<void> = Promise.resolve()
  const close = async () => {
    await pending.catch(() => {})
    const current = handle
    handle = undefined
    await current?.close()
  }
  return new Writable({
    highWaterMark: 64 * 1024,
    write(chunk: Buffer, _encoding, callback) {
      pending = workspaceIo.run(key, async batch => {
        handle ??= await open(path, 'w')
        for (let offset = 0; offset < chunk.length;) {
          const end = Math.min(chunk.length, offset + 64 * 1024)
          const { bytesWritten } = await handle.write(chunk, offset, end - offset)
          if (!bytesWritten) throw new Error('Log write made no progress')
          offset += bytesWritten
          if (offset < chunk.length) await batch.yield()
        }
      })
      void pending.then(() => callback(), error => callback(error))
    },
    final(callback) {
      void close().then(() => callback(), callback)
    },
    destroy(error, callback) {
      void close().then(() => callback(error), () => callback(error))
    },
  })
}
