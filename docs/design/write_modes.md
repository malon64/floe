# Write Modes Refactor Plan (Phase 1)

## Scope

This document covers:
- Assessment of current accepted/rejected/report writing paths.
- A refactor plan to introduce explicit write modes with no behavior change in this PR.
- Initial scaffolding required so append can be implemented in a follow-up PR without wide rewrites.

This phase does **not** implement append writes.

## Current Assessment

### Write Entry Points

Primary runtime entry points:
- `crates/floe-core/src/run/entity/mod.rs`
  - Row-level processing per input file.
  - Entity-level accepted accumulation and final accepted write (Phase C).
- `crates/floe-core/src/run/output.rs`
  - `write_accepted_output(...)` delegates to accepted format adapter.
  - `write_rejected_output(...)` delegates to rejected format adapter.
  - `write_rejected_raw_output(...)` and `write_error_report_output(...)` write side artifacts.

Format adapter registry:
- `crates/floe-core/src/io/format.rs`
  - `accepted_sink_adapter(...)` maps accepted formats (`parquet`, `delta`, `iceberg`).
  - `rejected_sink_adapter(...)` maps rejected formats (`csv`).

Accepted format implementations:
- `crates/floe-core/src/io/write/parquet.rs`
- `crates/floe-core/src/io/write/delta.rs`
- `crates/floe-core/src/io/write/iceberg.rs` (unsupported placeholder)

Rejected format implementation:
- `crates/floe-core/src/io/write/csv.rs`

Storage-agnostic output router:
- `crates/floe-core/src/io/storage/output.rs`

Other write paths (adjacent duplication context):
- `crates/floe-core/src/report/output.rs` (report writes, separate local/cloud routing)
- `crates/floe-core/src/io/storage/archive.rs` (archive copy/delete)

### Current Accepted Output Shape

Accepted outputs are currently **entity-level**, not per-source-file:
- `run/entity/mod.rs` accumulates accepted rows from all input files into one DataFrame.
- One final accepted write occurs per entity.
- File reports then point to the same accepted target URI.

Format-specific shape:
- Parquet accepted sink: treated as a dataset directory with `part-xxxxx.parquet` files.
- Delta accepted sink: treated as a logical Delta table root (transactional `_delta_log`).

This already aligns with the desired target-agnostic model (`csv1 + csv2 -> one accepted target`).

### Where Overwrite Is Implicit Today

Hard-coded mode usage:
- `run/entity/mod.rs` passes overwrite explicitly for accepted write.

Parquet overwrite assumptions:
- `io/write/parquet.rs` clears local output directory/prefix before writing.
- `clear_*_output_prefix(...)` deletes existing objects under target prefix (S3/GCS/ADLS).
- Part names always restart at `part-00000` for each run.
- Empty accepted dataframe still writes one parquet file.

Delta overwrite assumptions:
- `io/write/delta.rs` always uses `SaveMode::Overwrite`.
- This is transactional overwrite (new Delta version), not blind directory replacement.

Path/layout assumptions:
- Accepted parquet always writes to `OutputPlacement::Directory`.
- Rejected outputs are still per-source-file (`{source_stem}_rejected.csv`, `{source_stem}_reject_errors.json`).

### Duplication Hotspots

Parquet storage cleanup duplication:
- `clear_s3_output_prefix`, `clear_gcs_output_prefix`, `clear_adls_output_prefix` are near-identical.

Local vs cloud upload duplication:
- `io/storage/output.rs` already centralizes accepted/rejected side-output placement.
- `report/output.rs` implements a separate local/cloud temp-upload path instead of reusing a common abstraction.

Accepted vs rejected write-path asymmetry:
- Different trait surfaces and context passing patterns increase branching in caller code.

Delta vs parquet divergence:
- Expected because delta uses object_store/delta-rs directly.
- But mode semantics are mixed into format writer implementation (overwrite behavior not isolated).

## Refactor Proposal

### 1) Add explicit write mode to config model

Introduce:
- `WriteMode` enum: `Overwrite | Append`
- Default: `Overwrite`

Add mode to accepted sink config:
- `SinkTarget.write_mode: WriteMode`
- In this phase, parser keeps current YAML surface; mode defaults internally to overwrite.
- Future phase can expose `sink.accepted.write_mode` safely.

Backward compatibility:
- Existing configs keep current behavior with no YAML changes required.

### 2) Separate concerns by layer

Target structure:
- **Mode semantics layer**
  - Decides overwrite/append behavior.
  - Owns cleanup/truncate/target-existence semantics.
- **Format writer layer**
  - Serializes DataFrame to target representation (parquet parts, delta transaction).
  - Does not decide global mode policy.
- **Storage resolution layer**
  - URI/path/key resolution and local/cloud materialization.
  - Kept in `io/storage/*`.
- **Partitioning/chunking layer**
  - Produces write chunks/part metadata for large outputs.
  - Initial interface only in phase 1.

### 3) Writer abstraction for accepted sinks

Proposed interface shape:

```rust
trait AcceptedWriter {
    fn write(&self, request: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput>;
}
```

`AcceptedWriteRequest` should carry:
- target + resolver + cloud client
- entity config
- dataframe
- write mode
- temp dir
- part naming/chunking strategy handle

Implementation strategy:
- Keep existing format adapters in phase 1 and add a thin accepted writer scaffold module.
- Move mode dispatch to one place so append can be added by extending mode handler, not by copying parquet/delta logic.

### 4) Append-difference points (for next PR)

Where append differs from overwrite:
- No unconditional cleanup/deletion of existing parquet prefix.
- Part naming must avoid collisions with existing parts.
- Unique enforcement must gain a hook to compare incoming rows against existing accepted target.
- Delta should switch to append transaction mode with optional merge/dedup strategy hook.
- Reporting should include mode in accepted output summary/metadata.

## Migration Map (Incremental)

1. Introduce `WriteMode` type and default in config model.
2. Pass `entity.sink.accepted.write_mode` through runtime accepted write context.
3. Create writer scaffolding modules:
   - `io/write/modes.rs`
   - `io/write/accepted.rs`
4. Keep adapters and overwrite behavior unchanged (parquet cleanup + delta overwrite remain).
5. Add focused tests validating default overwrite behavior remains identical.
6. Follow-up PR: implement append mode in mode layer and format adapters only.

## Testing Strategy

Phase 1 tests:
- Config parse/default test: accepted write mode defaults to overwrite.
- Existing accepted-output and delta overwrite tests must continue passing.

Writer-trait unit tests without cloud:
- Use `Target::Local` and temp directories.
- Use deterministic test DataFrames and assert:
  - mode dispatch selects overwrite path by default
  - part naming/layout remains unchanged
  - no cloud client required for local path tests

Cloud integrations:
- Keep out of unit tests in this PR to avoid flaky tests.
- Existing object_store/delta unit coverage remains sufficient for overwrite regression safety.
