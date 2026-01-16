use clap::{Parser, Subcommand};
use floe_core::{run, validate, FloeResult, RunOptions, ValidateOptions};
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
            println!(
                "Your config file at location {}, is well formatted.\n\rYou can now run 'floe run' command",
                config_path.to_str().unwrap_or("default")
            );
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
            println!("run accepted (not implemented yet)");
            Ok(())
        }
    }
}

fn resolve_path(path: PathBuf) -> FloeResult<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
