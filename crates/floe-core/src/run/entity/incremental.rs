use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::config::IncrementalMode;
use crate::config::StorageResolver;
use crate::io::format::InputFile;
use crate::io::storage::CloudClient;
use crate::report::{
    FileMismatch, FileOutput, FileReport, FileStatus, FileValidation, MismatchAction,
};
use crate::run::RunContext;
use crate::state::{
    claim_all_entity_inputs, claim_entity_inputs, promote_claimed_entity_state,
    release_claimed_entity_state, renew_claimed_entity_state, ClaimedEntityState, EntityFileState,
    CLAIM_TTL_SECONDS,
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
    if context.full_refresh {
        let pending_state = if entity.incremental_mode == IncrementalMode::File {
            claim_all_entity_inputs(
                &context.storage_resolver,
                cloud,
                entity,
                &context.run_id,
                input_files.clone(),
            )?
            .map(|claimed| {
                PendingEntityState::new(
                    claimed,
                    context.storage_resolver.clone(),
                    entity.name.clone(),
                    context.run_id.clone(),
                )
            })
        } else {
            None
        };
        return Ok(IncrementalContext {
            pending_inputs: input_files,
            skipped_reports: Vec::new(),
            pending_state,
        });
    }

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
            skipped_reports.push(build_skipped_report(
                input_file.source_uri.clone(),
                "already_ingested",
                None,
            ));
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
                "already_ingested_changed",
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
        skipped_reports.push(build_skipped_report(
            source_uri,
            "active_claim",
            Some(message),
        ));
    }

    Ok(IncrementalContext {
        pending_inputs: claim_outcome.pending_inputs,
        skipped_reports,
        pending_state: claim_outcome.claimed_state.map(|claimed| {
            PendingEntityState::new(
                claimed,
                context.storage_resolver.clone(),
                entity.name.clone(),
                context.run_id.clone(),
            )
        }),
    })
}

fn file_state_matches(recorded: &EntityFileState, input_file: &InputFile) -> bool {
    recorded.size == input_file.source_size && recorded.mtime == input_file.source_mtime
}

fn build_skipped_report(
    input_file: String,
    skip_reason: &str,
    warning: Option<String>,
) -> FileReport {
    let warnings = u64::from(warning.is_some());
    FileReport {
        input_file,
        status: FileStatus::Skipped,
        row_count: 0,
        accepted_count: 0,
        rejected_count: 0,
        skip_reason: Some(skip_reason.to_string()),
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
    resolver: StorageResolver,
    entity_name: String,
    run_id: String,
    heartbeat: Option<ClaimHeartbeat>,
    finalized: bool,
}

impl PendingEntityState {
    fn new(
        claimed: ClaimedEntityState,
        resolver: StorageResolver,
        entity_name: String,
        run_id: String,
    ) -> Self {
        let heartbeat = ClaimHeartbeat::start(
            resolver.clone(),
            entity_name.clone(),
            run_id.clone(),
            claimed.clone(),
        );
        Self {
            claimed,
            resolver,
            entity_name,
            run_id,
            heartbeat: Some(heartbeat),
            finalized: false,
        }
    }

    pub(super) fn commit(
        &mut self,
        _context: &RunContext,
        cloud: &mut CloudClient,
        _entity: &config::EntityConfig,
    ) -> FloeResult<()> {
        self.stop_heartbeat();
        promote_claimed_entity_state(
            &self.resolver,
            cloud,
            &self.entity_name,
            &self.run_id,
            &self.claimed,
        )?;
        self.finalized = true;
        Ok(())
    }

    pub(super) fn release(
        &mut self,
        _context: &RunContext,
        cloud: &mut CloudClient,
        _entity: &config::EntityConfig,
    ) -> FloeResult<()> {
        self.stop_heartbeat();
        release_claimed_entity_state(
            &self.resolver,
            cloud,
            &self.entity_name,
            &self.run_id,
            &self.claimed,
        )?;
        self.finalized = true;
        Ok(())
    }

    fn stop_heartbeat(&mut self) {
        if let Some(mut heartbeat) = self.heartbeat.take() {
            heartbeat.stop();
        }
    }
}

impl Drop for PendingEntityState {
    fn drop(&mut self) {
        self.stop_heartbeat();
        if self.finalized {
            return;
        }
        let mut cloud = CloudClient::new();
        let _ = release_claimed_entity_state(
            &self.resolver,
            &mut cloud,
            &self.entity_name,
            &self.run_id,
            &self.claimed,
        );
    }
}

struct ClaimHeartbeat {
    stop_tx: Option<Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl ClaimHeartbeat {
    fn start(
        resolver: StorageResolver,
        entity_name: String,
        run_id: String,
        claimed: ClaimedEntityState,
    ) -> Self {
        let (stop_tx, stop_rx) = mpsc::channel();
        let interval = claim_renewal_interval();
        let handle = thread::spawn(move || {
            while stop_rx.recv_timeout(interval).is_err() {
                let mut cloud = CloudClient::new();
                let _ = renew_claimed_entity_state(
                    &resolver,
                    &mut cloud,
                    &entity_name,
                    &run_id,
                    &claimed,
                );
            }
        });
        Self {
            stop_tx: Some(stop_tx),
            handle: Some(handle),
        }
    }

    fn stop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ClaimHeartbeat {
    fn drop(&mut self) {
        self.stop();
    }
}

fn claim_renewal_interval() -> Duration {
    let ttl = CLAIM_TTL_SECONDS.max(1) as u64;
    Duration::from_secs((ttl / 3).clamp(1, 300))
}
