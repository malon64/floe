---
name: Onboarding / Codebase tour
about: Start here if you're new to Floe and want a guided overview
title: "[Onboarding] Introduce yourself + questions"
labels: []
---

Welcome! This thread is for newcomers to share context and ask questions.

## Quick intro
- Background:
- What you want to contribute to:
- Any constraints (time/stack):

## Codebase map (short)
- `crates/floe-core`: ingestion engine (formats, checks, run pipeline)
- `crates/floe-cli`: CLI wrapper for config loading + run/validate
- `docs/`: user docs and support matrix
- `example/`: sample configs + inputs

## Architecture highlights
- Storage abstraction: `crates/floe-core/src/io/storage`
- Input formats: `crates/floe-core/src/io/read`
- Output sinks: `crates/floe-core/src/io/write`
- Run pipeline: `crates/floe-core/src/run`
- Reports: `crates/floe-core/src/report`

## How to get started
- Find a `good first issue`
- Ask questions here if the scope or design is unclear
- Check `CONTRIBUTING.md` for testing expectations

## Questions
Ask anything—happy to help.
