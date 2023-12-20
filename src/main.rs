mod args;

use std::io;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    cursor::MoveTo,
    execute,
    style::Stylize,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use datafusion::prelude::*;
use rustyline::{error::ReadlineError, DefaultEditor};

use crate::args::Args;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let file = args.file().rsplit('/').next().unwrap();
    let table = file.strip_suffix(".csv").unwrap_or(file);

    execute!(io::stdout(), EnterAlternateScreen, MoveTo(0, 0))?;

    println!("DataWrangler");
    println!("File: {}", args.file());
    println!("Table: {}", table);
    println!();

    let ctx = SessionContext::new();
    let options = CsvReadOptions::new().has_header(!args.no_header());
    ctx.register_csv(table, args.file(), options).await?;

    let prompt_text = ">> ".dark_green().to_string();
    let mut prompt = DefaultEditor::new()?;

    loop {
        let buf = prompt.readline(&prompt_text);

        match buf {
            Ok(buf) => {
                if buf.starts_with("quit") {
                    break;
                }

                prompt.add_history_entry(&buf)?;

                match ctx.sql(&buf).await {
                    Ok(df) => df.show().await?,
                    Err(e) => println!("{}", e),
                }
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
