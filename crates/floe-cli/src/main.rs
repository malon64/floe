use clap::{Parser, Subcommand};
use floe_core::{
    load_config, resolve_config_location, run_with_base, validate_with_base, FloeResult,
    RunOptions, ValidateOptions,
};

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
    },
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
        } => {
            let config_location = resolve_config_location(&config)?;
            let options = RunOptions { run_id, entities };
            let outcome =
                run_with_base(&config_location.path, config_location.base.clone(), options)?;
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
