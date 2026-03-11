use crate::coordinator::{RuntimeStatus, WorkflowState};
use crate::coordinator_storage::CoordinatorStorageTransfer;
use crate::{MaccError, Result};
use std::collections::BTreeMap;

pub struct WorkflowTransitionArgs {
    pub from: WorkflowState,
    pub to: WorkflowState,
}

pub struct RuntimeTransitionArgs {
    pub from: RuntimeStatus,
    pub to: RuntimeStatus,
}

pub struct RuntimeStatusFromEventArgs {
    pub event_type: String,
    pub status: String,
}

pub struct StorageSyncArgs {
    pub direction: CoordinatorStorageTransfer,
}

fn parse_flag_kv_pairs(
    args: &[String],
    usage: &str,
    allowed_keys: &[&str],
) -> Result<BTreeMap<String, String>> {
    if !args.len().is_multiple_of(2) {
        return Err(MaccError::Validation(format!(
            "Invalid arguments. Usage: {}",
            usage
        )));
    }
    let mut map = BTreeMap::new();
    for pair in args.chunks_exact(2) {
        let key = pair[0].as_str();
        if !key.starts_with("--") {
            return Err(MaccError::Validation(format!(
                "Unexpected argument '{}'. Usage: {}",
                key, usage
            )));
        }
        let normalized = key.trim_start_matches("--");
        if !allowed_keys
            .iter()
            .any(|candidate| candidate == &normalized)
        {
            return Err(MaccError::Validation(format!(
                "Unknown arg '{}'. Usage: {}",
                key, usage
            )));
        }
        map.insert(normalized.to_string(), pair[1].clone());
    }
    Ok(map)
}

impl TryFrom<&[String]> for WorkflowTransitionArgs {
    type Error = MaccError;

    fn try_from(args: &[String]) -> std::result::Result<Self, Self::Error> {
        let usage = "macc coordinator validate-transition --from <state> --to <state>";
        let map = parse_flag_kv_pairs(args, usage, &["from", "to"])?;
        let from = map
            .get("from")
            .ok_or_else(|| MaccError::Validation(format!("Missing --from. Usage: {}", usage)))?
            .parse::<WorkflowState>()
            .map_err(MaccError::Validation)?;
        let to = map
            .get("to")
            .ok_or_else(|| MaccError::Validation(format!("Missing --to. Usage: {}", usage)))?
            .parse::<WorkflowState>()
            .map_err(MaccError::Validation)?;
        Ok(Self { from, to })
    }
}

impl TryFrom<&[String]> for RuntimeTransitionArgs {
    type Error = MaccError;

    fn try_from(args: &[String]) -> std::result::Result<Self, Self::Error> {
        let usage = "macc coordinator validate-runtime-transition --from <status> --to <status>";
        let map = parse_flag_kv_pairs(args, usage, &["from", "to"])?;
        let from = map
            .get("from")
            .ok_or_else(|| MaccError::Validation(format!("Missing --from. Usage: {}", usage)))?
            .parse::<RuntimeStatus>()
            .map_err(MaccError::Validation)?;
        let to = map
            .get("to")
            .ok_or_else(|| MaccError::Validation(format!("Missing --to. Usage: {}", usage)))?
            .parse::<RuntimeStatus>()
            .map_err(MaccError::Validation)?;
        Ok(Self { from, to })
    }
}

impl TryFrom<&[String]> for RuntimeStatusFromEventArgs {
    type Error = MaccError;

    fn try_from(args: &[String]) -> std::result::Result<Self, Self::Error> {
        let usage =
            "macc coordinator runtime-status-from-event --type <event_type> --status <status>";
        let map = parse_flag_kv_pairs(args, usage, &["type", "status"])?;
        let event_type = map
            .get("type")
            .cloned()
            .ok_or_else(|| MaccError::Validation(format!("Missing --type. Usage: {}", usage)))?;
        let status = map.get("status").cloned().unwrap_or_default();
        Ok(Self { event_type, status })
    }
}

impl TryFrom<&[String]> for StorageSyncArgs {
    type Error = MaccError;

    fn try_from(args: &[String]) -> std::result::Result<Self, Self::Error> {
        let usage = "macc coordinator storage-sync --direction <import|export|verify>";
        let map = parse_flag_kv_pairs(args, usage, &["direction"])?;
        let direction = map
            .get("direction")
            .ok_or_else(|| MaccError::Validation(format!("Missing --direction. Usage: {}", usage)))?
            .parse::<CoordinatorStorageTransfer>()
            .map_err(MaccError::Validation)?;
        Ok(Self { direction })
    }
}

pub fn parse_coordinator_extra_kv_args(extra_args: &[String]) -> Result<BTreeMap<String, String>> {
    if !extra_args.len().is_multiple_of(2) {
        return Err(MaccError::Validation(
            "Unexpected argument list; expected '--key value' pairs.".into(),
        ));
    }
    let mut map = BTreeMap::new();
    for pair in extra_args.chunks_exact(2) {
        let key = pair[0].as_str();
        if !key.starts_with("--") {
            return Err(MaccError::Validation(format!(
                "Unexpected argument '{}'; expected '--key value' pairs.",
                key
            )));
        }
        map.insert(key.trim_start_matches("--").to_string(), pair[1].clone());
    }
    Ok(map)
}
