use std::io;

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    style::Print,
};

#[derive(Debug)]
pub struct Prompt {
    prompt_text: Box<str>,
    history: Vec<Box<str>>,
}

impl Prompt {
    pub fn new(prompt_text: &str) -> Self {
        Prompt {
            prompt_text: prompt_text.into(),
            history: Vec::default(),
        }
    }

    pub fn prompt(&mut self) -> Result<String> {
        let mut buf = String::new();
        let mut current = self.history.len() as i16;

        execute!(io::stdout(), Print(&self.prompt_text), Print(' '))?;

        loop {
            match event::read()? {
                Event::Key(ev) => match ev.code {
                    KeyCode::Up => {
                        if current > 0 {
                            current = current - 1;
                            buf = self.history[current as usize].clone().into();
                        }
                    }
                    KeyCode::Down => {
                        if self.history.len() > 0 && current < (self.history.len() - 1) as i16 {
                            current = current + 1;
                            buf = self.history[current as usize].clone().into();
                        }
                    }
                    KeyCode::Char(c) => buf.push(c),
                    KeyCode::Enter => break,
                    _ => {}
                },
                _ => {}
            }
        }

        self.history.push(buf.clone().into());

        Ok(buf)
    }
}
