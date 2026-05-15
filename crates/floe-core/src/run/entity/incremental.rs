use crate::config::IncrementalMode;
use crate::io::format::InputFile;
use crate::io::storage::CloudClient;
use crate::report::{
    FileMismatch, FileOutput, FileReport, FileStatus, FileValidation, MismatchAction,
};
use crate::run::RunContext;
use crate::state::{
    claim_entity_inputs, promote_claimed_entity_state, release_claimed_entity_state,
    ClaimedEntityState, EntityFileState,
};
use crate::{config, warnings, FloeResult};

pub(super) struct IncrementalContext {
    pub(super) pending_inputs: Vec<InputFile>,
    pub(super) skipped_reports: Vec<FileReport>,
    pub(super) pending_state: Option<PendingEntityState>,
}

pub(super) fn prepare_incremental_context(
    context: &RunContext,
    cloud: &mut CloudClient,
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

    let mut skipped_reports = Vec::new();
    let claim_outcome = claim_entity_inputs(
        &context.storage_resolver,
        cloud,
        entity,
        &context.run_id,
        input_files,
    )?;

    for (input_file, recorded) in &claim_outcome.already_processed {
        if file_state_matches(recorded, input_file) {
            skipped_reports.push(build_skipped_report(input_file.source_uri.clone(), None));
        } else {
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
    }
    for source_uri in claim_outcome.active_claims {
        let message = format!(
            "entity.name={} incremental_mode=file skipping file claimed by another active run: {}",
            entity.name, source_uri
        );
        warnings::emit(
            &context.run_id,
            Some(&entity.name),
            Some(&source_uri),
            Some("incremental_file_claimed"),
            &message,
        );
        skipped_reports.push(build_skipped_report(source_uri, Some(message)));
    }

    Ok(IncrementalContext {
        pending_inputs: claim_outcome.pending_inputs,
        skipped_reports,
        pending_state: claim_outcome
            .claimed_state
            .map(|claimed| PendingEntityState { claimed }),
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

pub(super) struct PendingEntityState {
    claimed: ClaimedEntityState,
}

impl PendingEntityState {
    pub(super) fn commit(
        &self,
        context: &RunContext,
        cloud: &mut CloudClient,
        entity: &config::EntityConfig,
    ) -> FloeResult<()> {
        promote_claimed_entity_state(
            &context.storage_resolver,
            cloud,
            entity,
            &context.run_id,
            &self.claimed,
        )
    }

    pub(super) fn release(
        &self,
        context: &RunContext,
        cloud: &mut CloudClient,
        entity: &config::EntityConfig,
    ) -> FloeResult<()> {
        release_claimed_entity_state(
            &context.storage_resolver,
            cloud,
            entity,
            &context.run_id,
            &self.claimed,
        )
    }
}
