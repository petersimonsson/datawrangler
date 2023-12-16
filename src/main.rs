mod args;

use std::io;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use datafusion::prelude::*;

use crate::args::Args;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let table = args.file().strip_suffix(".csv").unwrap_or(args.file());

    execute!(io::stdout(), EnterAlternateScreen)?;

    println!("DataWrangler");
    println!("File: {}", args.file());
    println!("Table: {}", table);
    println!();

    let ctx = SessionContext::new();
    let options = CsvReadOptions::new().has_header(false);
    ctx.register_csv(table, args.file(), options).await?;

    loop {
        let mut buf = String::new();
        io::stdin().read_line(&mut buf)?;

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
