mod args;
mod command_parser;

use std::{io, str::FromStr};

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
use rustyline::{error::ReadlineError, DefaultEditor};

use crate::{args::Args, command_parser::Command};

#[tokio::main]
async fn main() -> Result<()> {
    let _args = Args::parse();

    execute!(io::stdout(), EnterAlternateScreen, MoveTo(0, 0))?;

    let version = env!("CARGO_PKG_VERSION");

    println!("DataWrangler v{}", version);
    println!();

    let ctx = SessionContext::new();

    let prompt_text = ">> ".dark_green().to_string();
    let mut prompt = DefaultEditor::new()?;

    loop {
        let buf = prompt.readline(&prompt_text);

        match buf {
            Ok(buf) => {
                let command = Command::from_str(&buf)?;

                match command {
                    Command::Quit => break,
                    Command::Load(data) => {
                        if let Err(e) = load_file(&ctx, &data).await {
                            println!("{}", e);
                        }
                    }
                    Command::Show => {
                        if let Err(e) = show_tables(&ctx) {
                            println!("{}", e);
                        }
                    }
                    Command::NotFound => match ctx.sql(&buf).await {
                        Ok(df) => df.show().await?,
                        Err(e) => println!("{}", e),
                    },
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
    let file = data.path().rsplit('/').next().unwrap();
    let table = file.replace('.', "_");
    let table = data.table().unwrap_or(&table);

    if let Some((_, file_ext)) = file.rsplit_once('.') {
        match file_ext {
            "csv" => {
                let options = CsvReadOptions::new().has_header(data.has_header());
                ctx.register_csv(table, data.path(), options).await?;
            }
            "parquet" => {
                ctx.register_parquet(table, data.path(), ParquetReadOptions::default())
                    .await?
            }
            _ => return Err(anyhow!("Unsupported file format: {}", file_ext)),
        }

        println!("Loaded {} as {}", data.path(), table);
    }

    Ok(())
}

fn show_tables(ctx: &SessionContext) -> Result<()> {
    for cat_name in ctx.catalog_names() {
        if let Some(catalog) = ctx.catalog(&cat_name) {
            for schema_name in catalog.schema_names() {
                if let Some(schema) = catalog.schema(&schema_name) {
                    for table in schema.table_names() {
                        println!("{}.{}.{}", &cat_name, &schema_name, table);
                    }
                }
            }
        }
    }

    Ok(())
}
