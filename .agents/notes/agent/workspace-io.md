---
{
  "schema": "harness-note/2",
  "id": "76166581-2cd7-4cd4-85b0-f93f66908cc7",
  "kind": "note",
  "lifecycle": "accepted",
  "created": "2026-10-29",
  "updated": "2026-10-29T07:28:01.650Z",
  "module": "note://62b44166-82f0-41ff-838d-e2b02388ed06/86c2d794-3be5-4807-b0fb-1f63c1baa910",
  "relations": [
    {"type":"related-to","target":"note://972afef3-2fc7-49de-a3ee-7e041225d28c/6b511625-c1d4-4b5b-ba17-c983ee9aacfa"}
  ]
}
---

# Workspace I/O batches

Checkpoint operations and workspace edit/create/delete transactions share the exported process-local WorkspaceIoQueue. Nested helpers reuse their transaction and must run sequentially, without detached work. One batch runs at a time; a directory transaction retains ownership when another directory runs at a boundary. Idle collection releases directory ownership between batches. The limits are 32 files per batch, 64 waiting logical tasks, and a shared 20ms rest between background batches. Suspended continuations retain their existing task slot. Queue saturation rejects explicitly; cancellation removes unstarted work and stops started work at cooperative boundaries.

Initialization shares its in-flight promise and loads only existing metadata, without creating directories. Identical in-flight count queries share one scan; empty requests do not scan. Snapshot capture fails instead of publishing partial file maps. Bounded content reads detect growth and changes during capture. Checkpoint files, indices and restored contents use temporary files and rename. GC runs idle, refreshes references after yielding, and coalesces concurrent mutations into a dirty follow-up pass. Unknown, malformed or unreadable checkpoint metadata disables automatic blob collection for that loaded directory, favoring retention over accidental loss.

readIoFile/writeIoFile use 64 KiB buffers and cooperative yields. createIoWriteStream uses standard Writable backpressure and closes a pending open even if destroyed early. node-hosts command.run streams live output through it, retaining the existing 8 KiB previews and 10 MiB per-stream capture limits. External processes pause when the log writer needs drain. Log failures do not change the command result; missing logs omit logPath.

Main verification: `npm run test --workspace=@janus-agent/agent-core` passes 377 tests in 24 files with one existing skip. `npx vitest run tests/command-tools.test.ts tests/jobs.test.ts` in node-hosts passes 23 tests, including a real 100 MiB subprocess. Both packages build. Real filesystem regressions cover eight terminals, Unicode, cold metadata-only initialization, coalesced queries, failed/cancelled capture, atomic chunk output, GC interleaving and destroyed streams. JanusX adds 70 targeted tests and a reproducible before/after script; its owning Note contains commands and measured results. This is Main-agent verification, without an independent reviewer.

This reduces duplicated work and overlap; it does not cap physical disk utilization or coordinate independent processes. Existing snapshot path policy remains unchanged. Paths/index maps grow with file count; Git output has a 16 MiB bound, and individual hashes, JSON encoding/decoding and diffs still contain synchronous work. Background rest can slow cold scans. Slow disks, remote hosts and long-running multi-instance behavior need later representative validation. Data migration to userData, Hook isolation and lifecycle cleanup policy remain separate JanusX stages.
