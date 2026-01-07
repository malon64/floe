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
        #[arg(long)]
        entity: Option<String>,
    },
    Run {
        #[arg(short, long)]
        config: PathBuf,
        #[arg(long = "in")]
        input: Option<PathBuf>,
        #[arg(long = "out")]
        output: Option<PathBuf>,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        entity: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    let result: FloeResult<()> = match cli.command {
        Command::Validate { config, entity } => {
            let options = ValidateOptions { entity };
            match validate(&config, options) {
                Ok(()) => {
                    println!(
                        "Your config file at location {}, is well formatted.\n\rYou can now run 'floe run' command",
                        config.to_str().unwrap_or("default")
                    );
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }

        Command::Run {
            config,
            input,
            output,
            run_id,
            entity,
        } => {
            let options = RunOptions {
                input,
                output,
                run_id,
                entity,
            };
            match run(&config, options) {
                Ok(()) => {
                    println!("run accepted (not implemented yet)");
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
