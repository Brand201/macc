use crate::Result;

pub trait InteractionHandler {
    fn info(&self, _message: &str) {}
    fn warn(&self, _message: &str) {}
    fn error(&self, _message: &str) {}
    fn confirm(&self, prompt: &str) -> Result<bool> {
        self.confirm_yes_no(prompt)
    }
    fn confirm_yes_no(&self, _prompt: &str) -> Result<bool> {
        Ok(false)
    }
}
