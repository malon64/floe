use clap::{Parser, Subcommand, ValueEnum};
use floe_core::{
    load_config, resolve_config_location, run_with_base, set_observer, validate_with_base,
    FloeResult, RunEvent, RunObserver, RunOptions, ValidateOptions,
};
use std::sync::Arc;

const VERSION: &str = env!("FLOE_VERSION");
const ROOT_LONG_ABOUT: &str = concat!(
    "Floe is a single-node, config-driven ingestion runner. It loads one YAML config\n",
    "and executes each entity in order, producing per-entity and summary reports.\n",
    "\n",
    "Config (v",
    env!("CARGO_PKG_VERSION"),
    ") structure:\n",
    "  version\n",
    "  metadata\n",
    "  report.path\n",
    "  entities[]\n",
    "\n",
    "entity:\n",
    "  name\n",
    "  metadata\n",
    "  source { format, path, options, cast_mode }\n",
    "  sink { accepted, rejected }\n",
    "  policy { severity }\n",
    "  schema { normalize_columns, columns[] }\n",
);

const RUN_LONG_ABOUT: &str = r#"Run all configured entities sequentially (or restrict with --entities).

Example config snippet:
  report:
    path: ./reports
  entities:
    - name: customers
      source: { format: csv, path: ./data/customers.csv }
      sink:
        accepted: { format: parquet, path: ./out/customers.parquet }

Example:
  floe run -c example/config.yml

Reports are written to:
  <report.path>/run_<run_id>/run.summary.json
  <report.path>/run_<run_id>/<entity.name>/run.json
"#;

const VALIDATE_LONG_ABOUT: &str = concat!(
    "Validate a configuration file before running.\n",
    "\n",
    "Validation checks:\n",
    "  - YAML parsing\n",
    "  - schema validation against the v",
    env!("CARGO_PKG_VERSION"),
    " structure\n",
    "\n",
    "Example:\n",
    "  floe validate -c example/config.yml\n",
);

mod output;

#[derive(Parser, Debug)]
#[command(
    name = "floe",
    version = VERSION,
    about = "YAML-driven technical ingestion tool",
    long_about = ROOT_LONG_ABOUT
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(about = "Validate a config file", long_about = VALIDATE_LONG_ABOUT)]
    Validate {
        #[arg(short, long, help = "Path or URI to the Floe config file")]
        config: String,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Comma-separated list of entity names"
        )]
        entities: Vec<String>,
    },
    #[command(about = "Run the ingestion pipeline", long_about = RUN_LONG_ABOUT)]
    Run {
        #[arg(short, long, help = "Path or URI to the Floe config file")]
        config: String,
        #[arg(long, help = "Optional run id (defaults to a generated value)")]
        run_id: Option<String>,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Comma-separated list of entity names"
        )]
        entities: Vec<String>,
        #[arg(
            long,
            conflicts_with = "verbose",
            help = "Suppress non-essential output"
        )]
        quiet: bool,
        #[arg(long, conflicts_with = "quiet", help = "Enable verbose output")]
        verbose: bool,
        #[arg(
            long,
            value_enum,
            default_value_t = LogFormat::Off,
            help = "Log format for run events (off|text|json)"
        )]
        log_format: LogFormat,
    },
}

#[derive(Clone, Debug, ValueEnum)]
enum LogFormat {
    Off,
    Text,
    Json,
}

struct CliObserver {
    format: LogFormat,
}

impl RunObserver for CliObserver {
    fn on_event(&self, event: RunEvent) {
        match self.format {
            LogFormat::Json => {
                if let Ok(line) = serde_json::to_string(&event) {
                    println!("{line}");
                }
            }
            LogFormat::Text => eprintln!("{}", format_event_text(&event)),
            LogFormat::Off => {}
        }
    }
}

fn format_event_text(event: &RunEvent) -> String {
    match event {
        RunEvent::RunStarted {
            run_id,
            config,
            report_base,
            ..
        } => format!(
            "run_start run_id={} config={} report_base={}",
            run_id,
            config,
            report_base.as_deref().unwrap_or("disabled")
        ),
        RunEvent::EntityStarted { name, .. } => format!("\nentity_start name={name}"),
        RunEvent::FileStarted { entity, input, .. } => {
            format!("  file_start entity={entity} input={input}")
        }
        RunEvent::FileFinished {
            entity,
            input,
            status,
            rows,
            accepted,
            rejected,
            elapsed_ms,
            ..
        } => format!(
            "  file_end entity={} input={} status={} rows={} accepted={} rejected={} elapsed_ms={}",
            entity, input, status, rows, accepted, rejected, elapsed_ms
        ),
        RunEvent::EntityFinished {
            name,
            status,
            files,
            rows,
            accepted,
            rejected,
            warnings,
            errors,
            ..
        } => format!(
            "entity_end name={} status={} files={} rows={} accepted={} rejected={} warnings={} errors={}",
            name, status, files, rows, accepted, rejected, warnings, errors
        ),
        RunEvent::RunFinished {
            status,
            exit_code,
            summary_uri,
            ..
        } => format!(
            "\nrun_end status={} exit_code={} summary={}",
            status,
            exit_code,
            summary_uri.as_deref().unwrap_or("disabled")
        ),
    }
}

fn main() -> FloeResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate { config, entities } => {
            let config_location = resolve_config_location(&config)?;
            let options = ValidateOptions { entities };
            validate_with_base(&config_location.path, config_location.base.clone(), options)?;
            let config = load_config(&config_location.path)?;
            println!("Config valid: {}", config_location.display);
            println!("Version: {}", config.version);
            println!(
                "Report: {}",
                config
                    .report
                    .as_ref()
                    .map(|report| report.path.as_str())
                    .unwrap_or("(disabled)")
            );
            println!("Entities: {}", config.entities.len());
            for entity in &config.entities {
                println!("Entity: {}", entity.name);
                println!(
                    "  Source: {} ({})",
                    entity.source.format, entity.source.path
                );
                println!(
                    "  Sink accepted: {} ({})",
                    entity.sink.accepted.format, entity.sink.accepted.path
                );
                if let Some(rejected) = &entity.sink.rejected {
                    println!("  Sink rejected: {} ({})", rejected.format, rejected.path);
                }
                println!("  Severity: {}", entity.policy.severity);
            }
            println!("Next: floe run -c {}", config_location.display);
            Ok(())
        }
        Command::Run {
            config,
            run_id,
            entities,
            quiet,
            verbose,
            log_format,
        } => {
            let config_location = resolve_config_location(&config)?;
            let options = RunOptions { run_id, entities };
            if !matches!(log_format, LogFormat::Off) {
                let _ = set_observer(Arc::new(CliObserver {
                    format: log_format.clone(),
                }));
            }
            if !matches!(log_format, LogFormat::Json) {
                print_plan(&config_location.path, &config_location.display, &options)?;
            }
            let outcome =
                run_with_base(&config_location.path, config_location.base.clone(), options)?;
            if !matches!(log_format, LogFormat::Json) {
                let mode = if quiet {
                    output::OutputMode::Quiet
                } else if verbose {
                    output::OutputMode::Verbose
                } else {
                    output::OutputMode::Default
                };
                println!("{}", output::format_run_output(&outcome, mode));
            }
            Ok(())
        }
    }
}

fn print_plan(
    config_path: &std::path::Path,
    display_path: &str,
    options: &RunOptions,
) -> FloeResult<()> {
    let config = load_config(config_path)?;
    let resolver = floe_core::config::StorageResolver::from_path(&config, config_path)?;
    let report_base = config
        .report
        .as_ref()
        .map(|report| report.path.as_str())
        .unwrap_or("(disabled)");

    println!("Config: {display_path}");
    println!("Report: {report_base}");
    println!("Entities: {}", config.entities.len());

    let selected = if options.entities.is_empty() {
        config.entities.iter().collect::<Vec<_>>()
    } else {
        config
            .entities
            .iter()
            .filter(|entity| options.entities.contains(&entity.name))
            .collect::<Vec<_>>()
    };

    let mut cloud = floe_core::io::storage::CloudClient::new();
    let temp_dir = tempfile::TempDir::new().ok();

    for entity in selected {
        let adapter = floe_core::io::format::input_adapter(entity.source.format.as_str())?;
        let resolved = resolver.resolve_path(
            &entity.name,
            "source.storage",
            entity.source.storage.as_deref(),
            &entity.source.path,
        )?;
        let target = floe_core::io::storage::Target::from_resolved(&resolved)?;
        let files = list_inputs(
            &resolver,
            &mut cloud,
            adapter,
            &target,
            entity,
            temp_dir.as_ref().map(|dir| dir.path()),
        )?;

        println!("  - {}", entity.name);
        println!(
            "    source: format={} storage={} path={}",
            entity.source.format, resolved.storage, resolved.uri
        );
        println!("    inputs: {}", files.len());
        let max_list = 10;
        for file in files.iter().take(max_list) {
            println!("      - {file}");
        }
        if files.len() > max_list {
            println!("      ... +{}", files.len() - max_list);
        }
    }
    println!();
    Ok(())
}

fn list_inputs(
    resolver: &floe_core::config::StorageResolver,
    cloud: &mut floe_core::io::storage::CloudClient,
    adapter: &dyn floe_core::io::format::InputAdapter,
    target: &floe_core::io::storage::Target,
    entity: &floe_core::config::EntityConfig,
    _temp_dir: Option<&std::path::Path>,
) -> FloeResult<Vec<String>> {
    match target {
        floe_core::io::storage::Target::Local { storage, .. } => {
            let resolved = adapter.resolve_local_inputs(
                resolver.config_dir(),
                &entity.name,
                &entity.source,
                storage,
            )?;
            Ok(resolved
                .files
                .iter()
                .map(|path| path.display().to_string())
                .collect())
        }
        floe_core::io::storage::Target::S3 { storage, .. } => {
            let client = cloud.client_for(resolver, storage, entity)?;
            let (_, key) = target
                .s3_parts()
                .ok_or_else(|| Box::new(std::io::Error::other("s3 target missing bucket")))?;
            list_remote_inputs(client, adapter, key)
        }
        floe_core::io::storage::Target::Gcs { storage, .. } => {
            let client = cloud.client_for(resolver, storage, entity)?;
            let (_, key) = target
                .gcs_parts()
                .ok_or_else(|| Box::new(std::io::Error::other("gcs target missing bucket")))?;
            list_remote_inputs(client, adapter, key)
        }
        floe_core::io::storage::Target::Adls { storage, .. } => {
            let client = cloud.client_for(resolver, storage, entity)?;
            let (_, _, key) = target
                .adls_parts()
                .ok_or_else(|| Box::new(std::io::Error::other("adls target missing container")))?;
            list_remote_inputs(client, adapter, key)
        }
    }
}

fn list_remote_inputs(
    client: &dyn floe_core::io::storage::StorageClient,
    adapter: &dyn floe_core::io::format::InputAdapter,
    prefix: &str,
) -> FloeResult<Vec<String>> {
    let suffixes = adapter.suffixes()?;
    let refs = client.list(prefix)?;
    let refs = floe_core::io::storage::filter_by_suffixes(refs, &suffixes);
    let refs = floe_core::io::storage::stable_sort_refs(refs);
    Ok(refs.into_iter().map(|obj| obj.uri).collect())
}
