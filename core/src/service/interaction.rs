use crate::Result;
use serde::{Deserialize, Serialize};

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

    fn interaction_mode(&self) -> InteractionMode {
        InteractionMode::Blocking
    }

    fn request_input(&self, request: &InteractionRequest) -> Result<InteractionDecision> {
        match self.interaction_mode() {
            InteractionMode::Blocking => {
                let response = match request.kind {
                    InteractionKind::ConfirmYesNo => {
                        InteractionResponse::Bool(self.confirm_yes_no(&request.prompt)?)
                    }
                    InteractionKind::TextLine => {
                        return Ok(InteractionDecision::WaitingForUser(WaitingForUser {
                            request_id: request.request_id.clone(),
                            reason: Some(
                                "text input requires a non-blocking handler implementation".into(),
                            ),
                        }))
                    }
                };
                Ok(InteractionDecision::Resolved(response))
            }
            InteractionMode::Deferred => Ok(InteractionDecision::WaitingForUser(
                WaitingForUser::from_request(request),
            )),
        }
    }

    fn resume_input(&self, _request_id: &str, _response: &InteractionResponse) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionMode {
    Blocking,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    ConfirmYesNo,
    TextLine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionRequest {
    pub request_id: String,
    pub prompt: String,
    pub kind: InteractionKind,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionResponse {
    Bool(bool),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionDecision {
    Resolved(InteractionResponse),
    WaitingForUser(WaitingForUser),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingForUser {
    pub request_id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

impl WaitingForUser {
    pub fn from_request(request: &InteractionRequest) -> Self {
        Self {
            request_id: request.request_id.clone(),
            reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct BlockingConfirmOnly;

    impl InteractionHandler for BlockingConfirmOnly {
        fn confirm_yes_no(&self, _prompt: &str) -> Result<bool> {
            Ok(true)
        }
    }

    struct DeferredHandler;

    impl InteractionHandler for DeferredHandler {
        fn interaction_mode(&self) -> InteractionMode {
            InteractionMode::Deferred
        }
    }

    #[test]
    fn blocking_confirm_request_is_resolved_immediately() {
        let handler = BlockingConfirmOnly;
        let request = InteractionRequest {
            request_id: "req-1".into(),
            prompt: "Continue?".into(),
            kind: InteractionKind::ConfirmYesNo,
            scope: Some("coordinator".into()),
        };

        let decision = handler.request_input(&request).expect("request input");
        assert_eq!(
            decision,
            InteractionDecision::Resolved(InteractionResponse::Bool(true))
        );
    }

    #[test]
    fn deferred_mode_returns_waiting_for_user() {
        let handler = DeferredHandler;
        let request = InteractionRequest {
            request_id: "req-2".into(),
            prompt: "Type value".into(),
            kind: InteractionKind::TextLine,
            scope: Some("wizard".into()),
        };

        let decision = handler.request_input(&request).expect("request input");
        assert_eq!(
            decision,
            InteractionDecision::WaitingForUser(WaitingForUser {
                request_id: "req-2".into(),
                reason: None,
            })
        );
    }
}
