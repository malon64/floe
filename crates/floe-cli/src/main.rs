use clap::{Parser, Subcommand};
use floe_core::config::{ConfigBase, StorageDefinition};
use floe_core::io::storage::{self, StorageClient};
use floe_core::{
    load_config, run_with_base, validate_with_base, FloeResult, RunOptions, ValidateOptions,
};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

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

struct ConfigLocation {
    path: PathBuf,
    base: ConfigBase,
    display: String,
    _temp_dir: Option<TempDir>,
}

fn resolve_config_location(input: &str) -> FloeResult<ConfigLocation> {
    if is_remote_uri(input) {
        let temp_dir = TempDir::new()?;
        let local_path = download_remote_config(input, temp_dir.path())?;
        let base = ConfigBase::remote_from_uri(temp_dir.path().to_path_buf(), input)?;
        Ok(ConfigLocation {
            path: local_path,
            base,
            display: input.to_string(),
            _temp_dir: Some(temp_dir),
        })
    } else {
        let path = PathBuf::from(input);
        let absolute = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };
        let canonical = std::fs::canonicalize(&absolute)?;
        let base = ConfigBase::local_from_path(&canonical);
        Ok(ConfigLocation {
            path: canonical.clone(),
            base,
            display: canonical.display().to_string(),
            _temp_dir: None,
        })
    }
}

fn download_remote_config(uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
    if uri.starts_with("s3://") {
        let location = storage::s3::parse_s3_uri(uri)?;
        let client = storage::s3::S3Client::new(location.bucket, None)?;
        return client.download_to_temp(uri, temp_dir);
    }
    if uri.starts_with("gs://") {
        let location = storage::gcs::parse_gcs_uri(uri)?;
        let client = storage::gcs::GcsClient::new(location.bucket)?;
        return client.download_to_temp(uri, temp_dir);
    }
    if uri.starts_with("abfs://") {
        let location = storage::adls::parse_adls_uri(uri)?;
        let definition = StorageDefinition {
            name: "config".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some(location.account),
            container: Some(location.container),
            prefix: None,
        };
        let client = storage::adls::AdlsClient::new(&definition)?;
        return client.download_to_temp(uri, temp_dir);
    }
    Err(format!("unsupported config uri: {}", uri).into())
}

fn is_remote_uri(value: &str) -> bool {
    value.starts_with("s3://") || value.starts_with("gs://") || value.starts_with("abfs://")
}
