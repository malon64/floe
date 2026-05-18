# Decision — CAS-based incremental state locking

## Context

Floe supports incremental ingestion: it tracks which source files have already been processed and skips them on subsequent runs. Originally this was a simple JSON file (`state.json`) recording processed file URIs. This worked for single-process sequential runs but failed in two scenarios:

1. **Concurrent runs** — two parallel Floe processes (e.g., triggered by an orchestrator backfill) could both read the same state, both decide a file needs processing, and process it twice.
2. **Crashed runs** — if a process died mid-run, files it was processing were absent from the `files` map with no indication of what happened.

## Decision

Use **compare-and-swap (CAS)** with a **time-bounded claims** model.

The state file gains a `claims` map alongside the existing `files` map. A claim is `{ run_id, acquired_at, expires_at, size, mtime }` written for each file being processed. Claims expire after 1 hour. All writes use `If-Match` / `If-None-Match` conditional HTTP headers (ETags on S3/ADLS, generation numbers on GCS) so only one concurrent writer wins; others retry up to 5 times. A background `ClaimHeartbeat` thread renews `expires_at` every TTL/3 seconds for long-running processes. `PendingEntityState` is a RAII guard — `commit()` promotes claims to files, `drop()` auto-releases claims on panic.

## Rationale

- **No lock server needed.** The storage layer (S3, GCS, ADLS) is the arbiter via conditional write primitives. Same pattern as DynamoDB conditional writes, Kubernetes `resourceVersion`, and Git ref updates.
- **Crash-safe by expiry.** A crashed process leaves stale claims that disappear after 1 hour without any cleanup job.
- **Backward-compatible.** V1 state files (no `claims` field) are silently upgraded to V2 on read.

## Trade-offs

The 1-hour TTL is a blunt instrument. If a run takes longer than 1 hour and the heartbeat thread fails, claims expire and the files may be reprocessed. The heartbeat mitigates this in practice but does not eliminate the theoretical risk.
