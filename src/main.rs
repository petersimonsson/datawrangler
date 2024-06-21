mod args;
mod command_parser;

use std::{fs::canonicalize, io, str::FromStr};

use anyhow::{anyhow, Result};
use clap::Parser;
use command_parser::LoadCommand;
use crossterm::{
    cursor::MoveTo,
    execute,
    style::Stylize,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use datafusion::prelude::*;
use rustyline::{
    completion::FilenameCompleter, error::ReadlineError, Completer, CompletionType, Config,
    EditMode, Editor, Helper, Highlighter, Hinter, Validator,
};

use crate::{args::Args, command_parser::Command};

#[derive(Helper, Hinter, Highlighter, Validator, Completer)]
struct ReadLineHelper {
    #[rustyline(Completer)]
    completer: FilenameCompleter,
}

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let _args = Args::parse();

    execute!(io::stdout(), EnterAlternateScreen, MoveTo(0, 0))?;

    let version = env!("CARGO_PKG_VERSION");

    println!("DataWrangler v{}", version);
    println!();
    println!("Load a CSV or Parquet file into a table using the LOAD command, eg:");
    println!("LOAD data.csv INTO data");
    println!();

    let config = SessionConfig::default().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let prompt_text = ">> ".dark_green().to_string();

    let helper = ReadLineHelper {
        completer: FilenameCompleter::new(),
    };
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Vi)
        .build();
    let mut prompt = Editor::with_config(config)?;
    prompt.set_helper(Some(helper));

    loop {
        let buf = prompt.readline(&prompt_text);

        match buf {
            Ok(buf) => {
                let command = Command::from_str(&buf);

                match command {
                    Ok(Command::Quit) => break,
                    Ok(Command::Load(data)) => load_file(&ctx, &data)
                        .await
                        .unwrap_or_else(|e| println!("Error: {e}")),
                    Ok(Command::NotFound) => match ctx.sql(&buf).await {
                        Ok(df) => df.show().await.unwrap_or_else(|e| println!("Error: {e}")),
                        Err(e) => println!("Error: {e}"),
                    },
                    Err(e) => println!("Error: {e}"),
                }

                prompt.add_history_entry(&buf)?;
            }
            Err(ReadlineError::Eof) => break,
            Err(ReadlineError::Interrupted) => {}
            Err(e) => println!("Error: {:?}", e),
        }

        println!();
    }

    execute!(io::stdout(), LeaveAlternateScreen)?;

    Ok(())
}

async fn load_file(ctx: &SessionContext, data: &LoadCommand) -> Result<()> {
    let path = shellexpand::full(data.path())?.to_string();
    let path = canonicalize(path)?;
    let file = path
        .file_name()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    let table = file.replace('.', "_");
    let table = data.table().unwrap_or(&table);

    if let Some((_, file_ext)) = file.rsplit_once('.') {
        match file_ext {
            "csv" => {
                let options = CsvReadOptions::new().has_header(data.has_header());
                ctx.register_csv(table, path.to_str().unwrap(), options)
                    .await?;
            }
            "parquet" => {
                ctx.register_parquet(table, path.to_str().unwrap(), ParquetReadOptions::default())
                    .await?
            }
            _ => return Err(anyhow!("Unsupported file format: {}", file_ext)),
        }

        println!("Loaded {} as {}", data.path(), table);
    }

    Ok(())
}
