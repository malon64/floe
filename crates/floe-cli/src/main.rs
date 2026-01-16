use clap::{Parser, Subcommand};
use floe_core::{load_config, run, validate, FloeResult, RunOptions, ValidateOptions};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "floe", version, about = "YAML-driven technical ingestion tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Validate {
        #[arg(short, long)]
        config: PathBuf,
        #[arg(long, value_delimiter = ',')]
        entities: Vec<String>,
    },
    Run {
        #[arg(short, long)]
        config: PathBuf,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long, value_delimiter = ',')]
        entities: Vec<String>,
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
            println!("Entities: {}", config.entities.len());
            for entity in &config.entities {
                println!("Entity: {}", entity.name);
                println!("  Source: {} ({})", entity.source.format, entity.source.path);
                println!(
                    "  Sink accepted: {} ({})",
                    entity.sink.accepted.format, entity.sink.accepted.path
                );
                if let Some(rejected) = &entity.sink.rejected {
                    println!(
                        "  Sink rejected: {} ({})",
                        rejected.format, rejected.path
                    );
                }
                println!("  Report: {}", entity.sink.report.path);
                println!("  Severity: {}", entity.policy.severity);
            }
            println!("Next: floe run -c {}", config_path.display());
            Ok(())
        }
        Command::Run {
            config,
            run_id,
            entities,
        } => {
            let config_path = resolve_path(config)?;
            let options = RunOptions {
                run_id,
                entities,
            };
            run(&config_path, options)?;
            println!("run completed");
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
