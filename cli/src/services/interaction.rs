use macc_core::service::interaction::InteractionHandler;
use macc_core::{MaccError, Result};

#[derive(Clone, Copy, Debug, Default)]
pub struct CliInteraction;

impl InteractionHandler for CliInteraction {
    fn info(&self, message: &str) {
        println!("{}", message);
    }

    fn warn(&self, message: &str) {
        eprintln!("{}", message);
    }

    fn error(&self, message: &str) {
        eprintln!("{}", message);
    }

    fn confirm_yes_no(&self, prompt: &str) -> Result<bool> {
        use std::io::{self, Write};
        print!("{}", prompt);
        io::stdout().flush().map_err(|e| MaccError::Io {
            path: "stdout".into(),
            action: "flush prompt".into(),
            source: e,
        })?;
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| MaccError::Io {
                path: "stdin".into(),
                action: "read confirmation".into(),
                source: e,
            })?;
        let value = input.trim().to_ascii_lowercase();
        Ok(value == "y" || value == "yes")
    }
}
