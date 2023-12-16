use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Args {
    #[arg(short, long)]
    /// CSV file does not have a header
    no_header: bool,
    /// CSV file to operate on
    file: Box<str>,
}

impl Args {
    pub fn no_header(&self) -> bool {
        self.no_header
    }

    pub fn file(&self) -> &str {
        &self.file
    }
}
