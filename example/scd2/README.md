# SCD2 Delta Example

Run from repository root.

Phase 1 (bootstrap table):

```bash
floe run -c example/config_delta_merge_scd2_phase1.yml --entities customer_dim
```

Phase 2 (one changed key, one unchanged key, one new key):

```bash
floe run -c example/config_delta_merge_scd2_phase2.yml --entities customer_dim
```

Expected behavior after phase 2:
- `(1, fr)` previous row is closed (`__floe_is_current=false`) and a new current row is inserted.
- `(2, ca)` remains current and unchanged.
- `(3, us)` is inserted as current.

Delta table path:
- `example/scd2/out/accepted/customer_dim_delta`
