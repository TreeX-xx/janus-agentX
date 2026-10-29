import { describe, it, expect, vi, afterEach } from 'vitest'
import { WorkspaceIoQueue } from '../src/main/lib/workspace-io'
import { useWorkspaceStorage, workspaceStorageInUse, configureWorkspaceStorageEvents, suspendWorkspaceStorage } from '../src/main/lib/workspace-storage'

const gate = () => { let release!: () => void; const promise = new Promise<void>(resolve => { release = resolve }); return { promise, release } }
afterEach(() => { vi.useRealTimers(); configureWorkspaceStorageEvents() })

it('protects descendants, emits release once and blocks new consumers during maintenance', () => {
  const released = vi.fn(); configureWorkspaceStorageEvents(released)
  const done = useWorkspaceStorage('/janus-fixture/child')
  expect(workspaceStorageInUse('/janus-fixture')).toBe(true)
  expect(workspaceStorageInUse('/janus-fixture-other')).toBe(false)
  expect(() => suspendWorkspaceStorage('/janus-fixture')).toThrow('commands')
  done(); done(); expect(released).toHaveBeenCalledTimes(1)
  const resume = suspendWorkspaceStorage('/janus-fixture')
  try { expect(() => useWorkspaceStorage('/janus-fixture/child')).toThrow('migration') }
  finally { resume() }
  useWorkspaceStorage('/janus-fixture/child')()
})

describe('shared workspace I/O lane', () => {
  it('serializes active batches across directories, but lets another directory run at a boundary', async () => {
    const q = new WorkspaceIoQueue(), hold = gate(), events: string[] = []
    const first = q.run('/a', async batch => { events.push('a1'); await hold.promise; await batch.yield(); events.push('a2') })
    const second = q.run('/b', async () => { events.push('b') })
    const same = q.run('/a', async () => { events.push('a3') })
    expect(events).toEqual(['a1'])
    hold.release()
    await Promise.all([first, second, same])
    expect(events).toEqual(['a1', 'b', 'a2', 'a3'])
  })

  it('allows exactly 64 waiting logical tasks; continuations consume no extra slots', async () => {
    const q = new WorkspaceIoQueue(), hold = gate()
    const first = q.run('/a', async batch => { await hold.promise; await batch.yield() })
    const waiting = Array.from({ length: 64 }, (_, i) => q.run(`/d${i}`, async () => i))
    await expect(q.run('/overflow', async () => {})).rejects.toThrow('busy')
    hold.release()
    await Promise.all([first, ...waiting])
    await expect(q.run('/after', async () => 1)).resolves.toBe(1)
  })

  it('removes cancelled waiters immediately and cancels running work at a boundary', async () => {
    const q = new WorkspaceIoQueue(), hold = gate(), controller = new AbortController(), waitingAbort = new AbortController()
    const active = q.run('/a', async batch => { await hold.promise; await batch.yield(); throw new Error('should not run') }, { signal: controller.signal })
    const visit = vi.fn()
    const waiting = q.run('/b', visit, { signal: waitingAbort.signal })
    const waitingResult = expect(waiting).rejects.toBeDefined()
    waitingAbort.abort()
    await waitingResult
    const result = expect(active).rejects.toBeDefined()
    controller.abort(); hold.release()
    await result
    expect(visit).not.toHaveBeenCalled()
    await expect(q.run('/a', async () => 'ok')).resolves.toBe('ok')
  })

  it('does not let another background scan fill the 20ms rest, but admits foreground work', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout'] })
    const q = new WorkspaceIoQueue(), events: string[] = []
    const first = q.run('/a', async batch => { events.push('a1'); await batch.yield(); events.push('a2') }, { background: true })
    const second = q.run('/b', async () => { events.push('b') }, { background: true })
    await q.run('/c', async () => { events.push('c') })
    expect(events).toEqual(['a1', 'c'])
    await vi.advanceTimersByTimeAsync(19)
    expect(events).toEqual(['a1', 'c'])
    await vi.advanceTimersByTimeAsync(1)
    await Promise.all([first, second])
    expect(events).toEqual(['a1', 'c', 'a2', 'b'])
  })

  it('yields after 32 files and retains directory ownership across the break', async () => {
    const q = new WorkspaceIoQueue(), events: string[] = [], hold = gate()
    const scan = q.run('/a', async batch => {
      await hold.promise
      await batch.each(Array.from({ length: 65 }, (_, i) => i), async i => { events.push(String(i)) })
    })
    const other = q.run('/b', async () => { events.push('other') })
    const mutation = q.run('/a', async () => { events.push('mutation') })
    hold.release(); await Promise.all([scan, other, mutation])
    expect(events.indexOf('other')).toBe(32)
    expect(events.at(-1)).toBe('mutation')
  })

  it('reuses a transaction for nested helpers and survives failed work', async () => {
    const q = new WorkspaceIoQueue()
    await expect(q.run('/a', async () => q.run('/a', async () => { throw new Error('failed') }))).rejects.toThrow('failed')
    await expect(q.run('/a', async () => 2)).resolves.toBe(2)
  })

  it('idle collection releases its directory between batches for new foreground work', async () => {
    const q = new WorkspaceIoQueue(), events: string[] = [], hold = gate()
    const cleanup = q.idle('/a', async batch => { events.push('gc1'); await hold.promise; await batch.yield(); events.push('gc2') })
    const write = q.run('/a', async () => { events.push('write') })
    hold.release()
    await Promise.all([cleanup, write])
    expect(events).toEqual(['gc1', 'write', 'gc2'])
  })

  it('detaches a host maintenance transaction from its caller and can retain its ownership across yields', async () => {
    const q = new WorkspaceIoQueue(), events: string[] = [], hold = gate()
    let maintenance!: Promise<void>
    await q.run('/registry', async () => {
      maintenance = q.idle('/registry', async batch => {
        events.push('maintenance1'); await hold.promise; await batch.yield(); events.push('maintenance2')
      }, false)
      expect(events).toEqual([])
    })
    const record = q.run('/registry', async () => { events.push('record') })
    const foreground = q.run('/other', async () => { events.push('foreground') })
    hold.release(); await Promise.all([maintenance, record, foreground])
    expect(events).toEqual(['maintenance1', 'foreground', 'maintenance2', 'record'])
  })
})
