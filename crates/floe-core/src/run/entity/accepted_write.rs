use std::path::Path;
use std::time::Instant;

use polars::prelude::{DataFrame, Series};

use crate::errors::RunError;
use crate::{config, io, FloeResult};

use super::super::output::{write_accepted_output, AcceptedOutputContext};
use super::EntityPhaseTimings;
use crate::run::events::{event_time_ms, RunEvent, RunObserver};
use crate::run::RunContext;
use io::storage::Target;

pub(super) struct AcceptedWritePhaseContext<'a> {
    pub(super) run_context: &'a RunContext,
    pub(super) observer: &'a dyn RunObserver,
    pub(super) runtime: &'a mut dyn crate::runtime::Runtime,
    pub(super) entity: &'a config::EntityConfig,
    pub(super) accepted_target: &'a Target,
    pub(super) temp_dir: Option<&'a Path>,
    pub(super) write_mode: config::WriteMode,
    pub(super) perf_enabled: bool,
    pub(super) phase_timings: &'a mut EntityPhaseTimings,
    pub(super) pending_input_count: usize,
    pub(super) accepted_accum: Vec<DataFrame>,
}

pub(super) fn run_accepted_write_phase(
    context: AcceptedWritePhaseContext<'_>,
) -> FloeResult<io::format::AcceptedWriteOutput> {
    let AcceptedWritePhaseContext {
        run_context,
        observer,
        runtime,
        entity,
        accepted_target,
        temp_dir,
        write_mode,
        perf_enabled,
        phase_timings,
        pending_input_count,
        accepted_accum,
    } = context;

    let default_schema_evolution =
        crate::io::write::strategy::merge::shared::default_schema_evolution_summary(
            entity, write_mode,
        );
    if pending_input_count == 0 {
        return Ok(io::format::AcceptedWriteOutput {
            files_written: Some(0),
            schema_evolution: default_schema_evolution,
            ..io::format::AcceptedWriteOutput::default()
        });
    }
    if accepted_accum.is_empty() && write_mode != config::WriteMode::Overwrite {
        return Ok(io::format::AcceptedWriteOutput {
            files_written: Some(0),
            schema_evolution: default_schema_evolution,
            ..io::format::AcceptedWriteOutput::default()
        });
    }

    let mut accepted_df = if accepted_accum.is_empty() {
        empty_accepted_frame(entity)?
    } else {
        let concat_start = perf_enabled.then(Instant::now);
        let accepted_df = concat_accepted_frames(accepted_accum)?;
        if let Some(start) = concat_start {
            phase_timings.concat_accepted_ms += start.elapsed().as_millis() as u64;
        }
        accepted_df
    };

    let output_stem = io::storage::paths::build_part_stem(0);
    let accepted_format = runtime.sink_format(entity.sink.accepted.format.as_str())?;
    let write_accepted_start = perf_enabled.then(Instant::now);
    let accepted_output = write_accepted_output(AcceptedOutputContext {
        format: accepted_format,
        target: accepted_target,
        df: &mut accepted_df,
        output_stem: &output_stem,
        temp_dir,
        cloud: runtime.storage(),
        resolver: &run_context.storage_resolver,
        catalogs: &run_context.catalog_resolver,
        entity,
        mode: write_mode,
    })?;
    if let Some(start) = write_accepted_start {
        let elapsed_ms = start.elapsed().as_millis() as u64;
        phase_timings.write_accepted_ms += elapsed_ms;
        match entity.sink.accepted.format.as_str() {
            "delta" => phase_timings.write_delta_ms += elapsed_ms,
            "iceberg" => phase_timings.write_iceberg_ms += elapsed_ms,
            _ => {}
        }
    }

    if accepted_output.schema_evolution.applied {
        observer.on_event(RunEvent::SchemaEvolutionApplied {
            run_id: run_context.run_id.clone(),
            entity: entity.name.clone(),
            mode: accepted_output.schema_evolution.mode.clone(),
            added_columns: accepted_output.schema_evolution.added_columns.clone(),
            ts_ms: event_time_ms(),
        });
    }
    Ok(accepted_output)
}

fn concat_accepted_frames(mut frames: Vec<DataFrame>) -> FloeResult<DataFrame> {
    if frames.is_empty() {
        return Err(Box::new(RunError("missing accepted dataframe".to_string())));
    }
    // Pairwise concatenation bounds repeated growth of a single frame compared to
    // strictly left-associative stacking while preserving row order.
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

fn empty_accepted_frame(entity: &config::EntityConfig) -> FloeResult<DataFrame> {
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
