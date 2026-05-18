# ADR-002 — CAS-based incremental state locking

## Status
Accepted

## Context
Floe supports incremental ingestion: it tracks which source files have already been processed and skips them on subsequent runs. Originally this was a simple JSON file (`state.json`) recording processed file URIs. This worked for single-process sequential runs but failed in two scenarios:

1. **Concurrent runs** — two parallel Floe processes (e.g., triggered by an orchestrator backfill) could both read the same state, both decide a file needs processing, and process it twice.
2. **Crashed runs** — if a process died mid-run, files it was processing were simply absent from the `files` map. The next run would reprocess them with no indication of what happened.

## Decision
Use **compare-and-swap (CAS)** with a **time-bounded claims** model for incremental state.

The state file gains a `claims` map alongside the existing `files` map:
- A claim is `{ run_id, acquired_at, expires_at, size, mtime }` written for each file being processed.
- Claims have a TTL of 1 hour. Expired claims are removed on the next read.
- All writes use `If-Match` / `If-None-Match` conditional HTTP headers (ETags on S3/ADLS, generation numbers on GCS). Only one concurrent writer wins; others retry up to 5 times.
- A background `ClaimHeartbeat` thread renews `expires_at` every TTL/3 seconds so long-running processes don't lose their claims.
- `PendingEntityState` is a RAII guard: `commit()` promotes claims → files, `release()` removes claims, `drop()` auto-releases to prevent orphans on panic.

## Rationale

- **No lock server needed.** The storage layer (S3/GCS/ADLS) is the arbiter. This is the same pattern as DynamoDB conditional writes, Kubernetes `resourceVersion`, and Git ref updates.
- **Crash-safe.** Claims expire automatically. A crashed process leaves stale claims that disappear after 1 hour.
- **Concurrent-safe.** Only one process can write a given set of claims. Others either see the claim and skip the file (if claimed), or retry and win on the next CAS attempt.
- **Backward-compatible.** V1 state files (no `claims` field) are silently migrated to V2 on read. No manual migration required.

## Trade-offs

- Adds latency: each run start requires a CAS round-trip to claim files.
- The 1-hour TTL is a blunt instrument — if a run takes more than 1 hour and the heartbeat thread dies, claims expire. The heartbeat mitigates this but does not eliminate the risk entirely.
- CAS retry (up to 5 attempts) means highly concurrent access to the same state file can introduce latency. In practice, Floe runs are not expected to have >5 concurrent writers per entity.

## Consequences
`state/mod.rs` owns all state I/O. The `run/entity/incremental.rs` module wraps the claim lifecycle in a RAII guard. No other modules interact with state directly.
