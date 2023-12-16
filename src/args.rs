use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Args {
    file: Box<str>,
}

impl Args {
    pub fn file(&self) -> &str {
        &self.file
    }
}
