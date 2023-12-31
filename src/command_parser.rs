use std::str::FromStr;

use nom::{
    bytes::complete::{is_not, tag_no_case},
    character::complete::{alpha1, multispace0},
    combinator::{all_consuming, opt},
    error::Error,
    sequence::{delimited, preceded},
    Finish, IResult,
};
use thiserror::Error;

#[derive(Debug)]
pub enum Command {
    NotFound,
    Quit,
    Load(LoadCommand),
    Show,
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Command parsing failed")]
    ParseError(#[from] Error<String>),
}

impl FromStr for Command {
    type Err = CommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (remaining, command) = match delimited(multispace0, alpha1, multispace0)(s).finish() {
            Ok(res) => res,
            Err(Error { input, code }) => {
                return Err(Error {
                    input: input.to_string(),
                    code,
                }
                .into())
            }
        };

        let command = command.to_lowercase();

        Ok(match command.as_str() {
            "quit" => Self::Quit,
            "load" => Self::Load(LoadCommand::from_str(remaining)?),
            "show" => Self::Show,
            &_ => Self::NotFound,
        })
    }
}

#[derive(Debug)]
pub struct LoadCommand {
    has_header: bool,
    path: Box<str>,
    table: Option<Box<str>>,
}

impl FromStr for LoadCommand {
    type Err = CommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (_, command) = match all_consuming(parse_load_command)(s).finish() {
            Ok(res) => res,
            Err(Error { input, code }) => {
                return Err(Error {
                    input: input.to_string(),
                    code,
                }
                .into())
            }
        };

        Ok(command)
    }
}

impl LoadCommand {
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }
}

fn parse_load_command(s: &str) -> IResult<&str, LoadCommand> {
    let (remaining, path) = delimited(multispace0, is_not(" \t\r\n"), multispace0)(s)?;
    let (remaining, without_header) = opt(tag_no_case("without header"))(remaining)?;
    let (remaining, table) = opt(delimited(
        multispace0,
        preceded(tag_no_case("as "), is_not(" \t\r\n")),
        multispace0,
    ))(remaining)?;

    Ok((
        remaining,
        LoadCommand {
            has_header: without_header.is_none(),
            path: path.into(),
            table: table.map(Box::from),
        },
    ))
}
