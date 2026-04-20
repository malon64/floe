use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::config::IncrementalMode;
use crate::io::format::InputFile;
use crate::report::{
    FileMismatch, FileOutput, FileReport, FileStatus, FileValidation, MismatchAction,
};
use crate::run::RunContext;
use crate::state::{
    read_entity_state, resolve_entity_state_path, write_entity_state_atomic, EntityFileState,
    EntityState,
};
use crate::{config, report, warnings, FloeResult};

pub(super) struct IncrementalContext {
    pub(super) pending_inputs: Vec<InputFile>,
    pub(super) skipped_reports: Vec<FileReport>,
    pub(super) pending_state: Option<PendingEntityState>,
}

pub(super) fn prepare_incremental_context(
    context: &RunContext,
    entity: &config::EntityConfig,
    input_files: Vec<InputFile>,
) -> FloeResult<IncrementalContext> {
    if entity.incremental_mode != IncrementalMode::File {
        return Ok(IncrementalContext {
            pending_inputs: input_files,
            skipped_reports: Vec::new(),
            pending_state: None,
        });
    }

    let resolved_state = resolve_entity_state_path(&context.storage_resolver, entity)?;
    let state_path = resolved_state.local_path.ok_or_else(|| {
        Box::new(crate::ConfigError(format!(
            "entity.name={} incremental_mode=file requires a local state path",
            entity.name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let existing =
        read_entity_state(&state_path)?.unwrap_or_else(|| EntityState::new(&entity.name));

    let mut pending_inputs = Vec::new();
    let mut skipped_reports = Vec::new();
    let mut pending_entries = BTreeMap::new();

    for input_file in input_files {
        match existing.files.get(&input_file.source_uri) {
            Some(recorded) if file_state_matches(recorded, &input_file) => {
                skipped_reports.push(build_skipped_report(input_file.source_uri.clone(), None));
            }
            Some(recorded) => {
                let message = format!(
                    "entity.name={} incremental_mode=file skipping previously ingested file with changed metadata: {} (recorded size={:?}, mtime={:?}; current size={:?}, mtime={:?})",
                    entity.name,
                    input_file.source_uri,
                    recorded.size,
                    recorded.mtime,
                    input_file.source_size,
                    input_file.source_mtime,
                );
                warnings::emit(
                    &context.run_id,
                    Some(&entity.name),
                    Some(&input_file.source_uri),
                    Some("incremental_file_changed"),
                    &message,
                );
                skipped_reports.push(build_skipped_report(
                    input_file.source_uri.clone(),
                    Some(message),
                ));
            }
            None => {
                pending_entries.insert(
                    input_file.source_uri.clone(),
                    PendingFileState {
                        size: input_file.source_size,
                        mtime: input_file.source_mtime.clone(),
                    },
                );
                pending_inputs.push(input_file);
            }
        }
    }

    Ok(IncrementalContext {
        pending_inputs,
        skipped_reports,
        pending_state: Some(PendingEntityState {
            path: state_path,
            entity_name: entity.name.clone(),
            base_state: existing,
            pending_entries,
        }),
    })
}

fn file_state_matches(recorded: &EntityFileState, input_file: &InputFile) -> bool {
    recorded.size == input_file.source_size && recorded.mtime == input_file.source_mtime
}

fn build_skipped_report(input_file: String, warning: Option<String>) -> FileReport {
    let warnings = u64::from(warning.is_some());
    FileReport {
        input_file,
        status: FileStatus::Success,
        row_count: 0,
        accepted_count: 0,
        rejected_count: 0,
        mismatch: FileMismatch {
            declared_columns_count: 0,
            input_columns_count: 0,
            missing_columns: Vec::new(),
            extra_columns: Vec::new(),
            mismatch_action: MismatchAction::None,
            error: None,
            warning,
        },
        output: FileOutput {
            accepted_path: None,
            rejected_path: None,
            errors_path: None,
            archived_path: None,
        },
        validation: FileValidation {
            errors: 0,
            warnings,
            rules: Vec::new(),
        },
    }
}

struct PendingFileState {
    size: Option<u64>,
    mtime: Option<String>,
}

pub(super) struct PendingEntityState {
    path: PathBuf,
    entity_name: String,
    base_state: EntityState,
    pending_entries: BTreeMap<String, PendingFileState>,
}

impl PendingEntityState {
    pub(super) fn commit(&self) -> FloeResult<()> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }

        let processed_at = report::now_rfc3339();
        let mut state = self.base_state.clone();
        if state.schema.is_empty() {
            state.schema = crate::state::ENTITY_STATE_SCHEMA_V1.to_string();
        }
        if state.entity.is_empty() {
            state.entity = self.entity_name.clone();
        }
        state.updated_at = Some(processed_at.clone());
        for (source_uri, pending) in &self.pending_entries {
            state.files.insert(
                source_uri.clone(),
                EntityFileState {
                    processed_at: processed_at.clone(),
                    size: pending.size,
                    mtime: pending.mtime.clone(),
                },
            );
        }
        write_entity_state_atomic(&self.path, &state)
    }
}
