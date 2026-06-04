use std::path::Path;
use std::time::Instant;

use polars::prelude::{DataFrame, Series};

use crate::config;
use crate::errors::RunError;
use crate::io::format::AcceptedWriteOutput;
use crate::io::storage::{CloudClient, Target};
use crate::io::write::sink_format::SinkFormat;
use crate::io::write::strategy::merge::keys::default_schema_evolution_summary;
use crate::run::events::{event_time_ms, RunEvent, RunObserver};
use crate::run::RunContext;
use crate::FloeResult;

use super::super::output::{write_accepted_output, AcceptedOutputContext};
use super::EntityPhaseTimings;

const DEFAULT_MAX_BUFFERED_BYTES: u64 = 256 * 1024 * 1024;

/// Buffers accepted `DataFrame`s produced per input file and flushes them once
/// the running estimated byte size meets a configurable threshold. The first
/// flush uses the entity's configured write mode; subsequent flushes are
/// forced to `Append`, mirroring the `rejected_overwrite_used` pattern used
/// for the rejected sink.
///
/// Only used for non-merge write modes. `merge_scd1` / `merge_scd2` need the
/// full per-entity dataset and keep the legacy accumulate-then-write path.
pub(super) struct AcceptedBuffer<'a> {
    target: &'a Target,
    format: &'static dyn SinkFormat,
    entity: &'a config::EntityConfig,
    resolver: &'a config::StorageResolver,
    catalogs: &'a config::CatalogResolver,
    temp_dir: Option<&'a Path>,
    base_write_mode: config::WriteMode,
    flush_threshold_bytes: u64,
    pending_input_count: usize,
    full_refresh: bool,
    observer: &'a dyn RunObserver,
    run_context: &'a RunContext,
    perf_enabled: bool,

    pending: Vec<DataFrame>,
    pending_bytes: u64,
    first_write_done: bool,
    merged_output: AcceptedWriteOutput,
}

pub(super) struct AcceptedBufferConfig<'a> {
    pub(super) target: &'a Target,
    pub(super) format: &'static dyn SinkFormat,
    pub(super) entity: &'a config::EntityConfig,
    pub(super) resolver: &'a config::StorageResolver,
    pub(super) catalogs: &'a config::CatalogResolver,
    pub(super) temp_dir: Option<&'a Path>,
    pub(super) base_write_mode: config::WriteMode,
    pub(super) pending_input_count: usize,
    pub(super) full_refresh: bool,
    pub(super) observer: &'a dyn RunObserver,
    pub(super) run_context: &'a RunContext,
    pub(super) perf_enabled: bool,
}

impl<'a> AcceptedBuffer<'a> {
    pub(super) fn new(cfg: AcceptedBufferConfig<'a>) -> Self {
        let flush_threshold_bytes = cfg
            .entity
            .sink
            .accepted
            .options
            .as_ref()
            .and_then(|options| options.max_size_per_file)
            .unwrap_or(DEFAULT_MAX_BUFFERED_BYTES);
        Self {
            target: cfg.target,
            format: cfg.format,
            entity: cfg.entity,
            resolver: cfg.resolver,
            catalogs: cfg.catalogs,
            temp_dir: cfg.temp_dir,
            base_write_mode: cfg.base_write_mode,
            flush_threshold_bytes,
            pending_input_count: cfg.pending_input_count,
            full_refresh: cfg.full_refresh,
            observer: cfg.observer,
            run_context: cfg.run_context,
            perf_enabled: cfg.perf_enabled,
            pending: Vec::new(),
            pending_bytes: 0,
            first_write_done: false,
            merged_output: AcceptedWriteOutput::default(),
        }
    }

    pub(super) fn add(
        &mut self,
        df: DataFrame,
        cloud: &mut CloudClient,
        phase_timings: &mut EntityPhaseTimings,
    ) -> FloeResult<()> {
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(df.estimated_size() as u64);
        self.pending.push(df);
        if self.pending_bytes >= self.flush_threshold_bytes {
            self.flush(cloud, phase_timings)?;
        }
        Ok(())
    }

    fn flush(
        &mut self,
        cloud: &mut CloudClient,
        phase_timings: &mut EntityPhaseTimings,
    ) -> FloeResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let concat_start = self.perf_enabled.then(Instant::now);
        let frames = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        let mut df = concat_frames(frames)?;
        if let Some(start) = concat_start {
            phase_timings.concat_accepted_ms += start.elapsed().as_millis() as u64;
        }
        let effective_mode = if self.first_write_done {
            config::WriteMode::Append
        } else {
            self.base_write_mode
        };
        self.write_frame(&mut df, effective_mode, cloud, phase_timings)
    }

    fn write_frame(
        &mut self,
        df: &mut DataFrame,
        mode: config::WriteMode,
        cloud: &mut CloudClient,
        phase_timings: &mut EntityPhaseTimings,
    ) -> FloeResult<()> {
        let stem =
            crate::io::storage::paths::build_part_stem(self.merged_output.parts_written as usize);
        let write_start = self.perf_enabled.then(Instant::now);
        let out = write_accepted_output(AcceptedOutputContext {
            format: self.format,
            target: self.target,
            df,
            output_stem: &stem,
            temp_dir: self.temp_dir,
            cloud,
            resolver: self.resolver,
            catalogs: self.catalogs,
            entity: self.entity,
            mode,
        })?;
        if let Some(start) = write_start {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            phase_timings.write_accepted_ms += elapsed_ms;
            match self.entity.sink.accepted.format.as_str() {
                "delta" => phase_timings.write_delta_ms += elapsed_ms,
                "iceberg" => phase_timings.write_iceberg_ms += elapsed_ms,
                _ => {}
            }
        }
        if out.schema_evolution.applied {
            self.observer.on_event(RunEvent::SchemaEvolutionApplied {
                run_id: self.run_context.run_id.clone(),
                entity: self.entity.name.clone(),
                mode: out.schema_evolution.mode.clone(),
                added_columns: out.schema_evolution.added_columns.clone(),
                ts_ms: event_time_ms(),
            });
        }
        self.first_write_done = true;
        self.merged_output.merge_in(out);
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        cloud: &mut CloudClient,
        phase_timings: &mut EntityPhaseTimings,
    ) -> FloeResult<AcceptedWriteOutput> {
        self.flush(cloud, phase_timings)?;
        if self.first_write_done {
            return Ok(self.merged_output);
        }
        let should_write_empty_overwrite = self.base_write_mode == config::WriteMode::Overwrite
            && (self.pending_input_count > 0 || self.full_refresh);
        if should_write_empty_overwrite {
            let mut empty = empty_accepted_frame(self.entity)?;
            self.write_frame(&mut empty, self.base_write_mode, cloud, phase_timings)?;
            Ok(self.merged_output)
        } else {
            Ok(AcceptedWriteOutput {
                files_written: Some(0),
                schema_evolution: default_schema_evolution_summary(
                    self.entity,
                    self.base_write_mode,
                ),
                ..AcceptedWriteOutput::default()
            })
        }
    }
}

pub(super) fn concat_frames(mut frames: Vec<DataFrame>) -> FloeResult<DataFrame> {
    if frames.is_empty() {
        return Err(Box::new(RunError("missing accepted dataframe".to_string())));
    }
    // Pairwise concatenation bounds repeated growth of a single frame compared
    // to strictly left-associative stacking while preserving row order.
    while frames.len() > 1 {
        let mut next = Vec::with_capacity(frames.len().div_ceil(2));
        let mut iter = frames.into_iter();
        while let Some(mut left) = iter.next() {
            if let Some(right) = iter.next() {
                left.vstack_mut(&right).map_err(|err| {
                    Box::new(RunError(format!("failed to concat accepted rows: {err}")))
                })?;
            }
            next.push(left);
        }
        frames = next;
    }
    frames
        .pop()
        .ok_or_else(|| Box::new(RunError("missing accepted dataframe".to_string())).into())
}

pub(super) fn empty_accepted_frame(entity: &config::EntityConfig) -> FloeResult<DataFrame> {
    let normalize_strategy = crate::checks::normalize::resolve_normalize_strategy(entity)?;
    let columns = crate::checks::normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize_strategy.as_deref(),
    );
    let series = columns
        .into_iter()
        .map(|column| {
            let dtype = config::parse_data_type(&column.column_type)?;
            Ok(Series::full_null(column.name.into(), 0, &dtype).into())
        })
        .collect::<FloeResult<Vec<_>>>()?;
    DataFrame::new(series).map_err(|err| {
        Box::new(RunError(format!(
            "failed to build empty accepted dataframe: {err}"
        )))
        .into()
    })
}
