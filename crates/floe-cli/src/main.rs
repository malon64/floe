use clap::{Parser, Subcommand};
use floe_core::{
    build_common_manifest_json, load_config, resolve_config_location, run_with_base,
    validate_with_base, FloeResult, RunOptions, ValidateOptions,
};
use std::io::Write;

use crate::logging::LogFormat;

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

const MANIFEST_LONG_ABOUT: &str = concat!(
    "Generate orchestrator manifest JSON from a validated Floe config.\n",
    "\n",
    "Examples:\n",
    "  floe manifest generate -c example/config.yml --output manifest.common.json\n",
    "  floe manifest generate -c example/config.yml --output -\n",
);

mod logging;
mod manifest_output;
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
        #[arg(long, help = "Resolve and print inputs/outputs without executing")]
        dry_run: bool,
    },
    #[command(
        about = "Generate orchestrator manifest JSON",
        long_about = MANIFEST_LONG_ABOUT
    )]
    Manifest {
        #[command(subcommand)]
        command: ManifestCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ManifestCommand {
    #[command(about = "Generate a manifest JSON file")]
    Generate {
        #[arg(short, long, help = "Path or URI to the Floe config file")]
        config: String,
        #[arg(short, long, help = "Output path for manifest JSON, or '-' for stdout")]
        output: String,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Optional comma-separated list of entity names"
        )]
        entities: Vec<String>,
    },
}

fn main() -> FloeResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate { config, entities } => {
            let config_location = match resolve_config_location(&config) {
                Ok(location) => location,
                Err(err) => {
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: {err}");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }
            };

            let options = ValidateOptions {
                entities: entities.clone(),
            };

            let validation_result =
                validate_with_base(&config_location.path, config_location.base.clone(), options);

            match validation_result {
                Ok(()) => {
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
                    let _ = std::io::stdout().lock().flush();
                    Ok(())
                }
                Err(err) => {
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: {err}");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }
            }
        }
        Command::Run {
            config,
            run_id,
            entities,
            quiet,
            verbose,
            log_format,
            dry_run,
        } => {
            let started_at = floe_core::report::now_rfc3339();
            let computed_run_id =
                run_id.unwrap_or_else(|| floe_core::report::run_id_from_timestamp(&started_at));

            let options = RunOptions {
                run_id: Some(computed_run_id.clone()),
                entities,
                dry_run,
            };
            logging::install_observer(log_format.clone());

            let config_location = match resolve_config_location(&config) {
                Ok(location) => location,
                Err(err) => {
                    logging::emit_failed_run_events(&computed_run_id, err.as_ref(), &log_format);
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: {err}");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }
            };

            let outcome =
                match run_with_base(&config_location.path, config_location.base.clone(), options) {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        logging::emit_failed_run_events(
                            &computed_run_id,
                            err.as_ref(),
                            &log_format,
                        );
                        let mut err_out = std::io::stderr().lock();
                        let _ = writeln!(err_out, "Error: {err}");
                        let _ = err_out.flush();
                        std::process::exit(1);
                    }
                };
            let mode = if quiet {
                output::OutputMode::Quiet
            } else if verbose {
                output::OutputMode::Verbose
            } else {
                output::OutputMode::Default
            };
            let summary = output::format_run_output(&outcome, mode, dry_run);

            match log_format {
                LogFormat::Json => {
                    let mut err = std::io::stderr().lock();
                    let _ = writeln!(err, "{summary}");
                    let _ = err.flush();
                }
                LogFormat::Text | LogFormat::Off => {
                    let mut out = std::io::stdout().lock();
                    let _ = writeln!(out, "{summary}");
                    let _ = out.flush();
                }
            }

            std::process::exit(outcome.summary.run.exit_code);
        }
        Command::Manifest { command } => match command {
            ManifestCommand::Generate {
                config,
                output,
                entities,
            } => {
                let config_location = match resolve_config_location(&config) {
                    Ok(location) => location,
                    Err(err) => {
                        let mut err_out = std::io::stderr().lock();
                        let _ = writeln!(err_out, "Error: {err}");
                        let _ = err_out.flush();
                        std::process::exit(1);
                    }
                };

                let options = ValidateOptions {
                    entities: entities.clone(),
                };
                if let Err(err) =
                    validate_with_base(&config_location.path, config_location.base.clone(), options)
                {
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: {err}");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }

                let config = load_config(&config_location.path)?;
                let manifest_json =
                    build_common_manifest_json(&config_location, &config, &entities)?;
                manifest_output::write_manifest(&output, &manifest_json)?;
                if output != "-" {
                    let mut out = std::io::stdout().lock();
                    let _ = writeln!(out, "Manifest written: {output}");
                    let _ = out.flush();
                }
                Ok(())
            }
        },
    }
}
