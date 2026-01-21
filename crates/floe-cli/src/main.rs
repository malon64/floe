use clap::{Parser, Subcommand};
use floe_core::{load_config, run, validate, FloeResult, RunOptions, ValidateOptions};
use std::path::PathBuf;

const VERSION: &str = env!("FLOE_VERSION");
const ROOT_LONG_ABOUT: &str = r#"Floe is a single-node, config-driven ingestion runner. It loads one YAML config
and executes each entity in order, producing per-entity and summary reports.

Config (v0.1.0) structure:
  version
  metadata
  report.path
  entities[]

entity:
  name
  metadata
  source { format, path, options, cast_mode }
  sink { accepted, rejected }
  policy { severity }
  schema { normalize_columns, columns[] }
"#;

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
  <report.path>/run_<run_id>/<entity.name>/run.json
"#;

const VALIDATE_LONG_ABOUT: &str = r#"Validate a configuration file before running.

Validation checks:
  - YAML parsing
  - schema validation against the v0.1.0 structure

Example:
  floe validate -c example/config.yml
"#;

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
        #[arg(short, long, help = "Path to the Floe config file")]
        config: PathBuf,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Comma-separated list of entity names"
        )]
        entities: Vec<String>,
    },
    #[command(about = "Run the ingestion pipeline", long_about = RUN_LONG_ABOUT)]
    Run {
        #[arg(short, long, help = "Path to the Floe config file")]
        config: PathBuf,
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
    },
}

fn main() -> FloeResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate { config, entities } => {
            let config_path = resolve_path(config)?;
            let options = ValidateOptions { entities };
            validate(&config_path, options)?;
            let config = load_config(&config_path)?;
            println!("Config valid: {}", config_path.display());
            println!("Version: {}", config.version);
            println!("Report: {}", config.report.path);
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
            println!("Next: floe run -c {}", config_path.display());
            Ok(())
        }
        Command::Run {
            config,
            run_id,
            entities,
            quiet,
            verbose,
        } => {
            let config_path = resolve_path(config)?;
            let options = RunOptions { run_id, entities };
            let outcome = run(&config_path, options)?;
            let mode = if quiet {
                output::OutputMode::Quiet
            } else if verbose {
                output::OutputMode::Verbose
            } else {
                output::OutputMode::Default
            };
            println!("{}", output::format_run_output(&outcome, mode));
            Ok(())
        }
    }
}

fn resolve_path(path: PathBuf) -> FloeResult<PathBuf> {
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(std::fs::canonicalize(absolute)?)
}
