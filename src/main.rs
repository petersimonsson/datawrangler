mod args;
mod prompt;

use std::io;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    cursor::MoveTo,
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use datafusion::prelude::*;

use crate::{args::Args, prompt::Prompt};

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

    let mut prompt = Prompt::new("SQL>");

    loop {
        let buf = prompt.prompt()?;

        if buf.starts_with("quit") {
            break;
        }

        match ctx.sql(&buf).await {
            Ok(df) => df.show().await?,
            Err(e) => println!("{}", e),
        }
        println!();
    }

    execute!(io::stdout(), LeaveAlternateScreen)?;

    Ok(())
}
