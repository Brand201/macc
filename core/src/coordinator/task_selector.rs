use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, Default)]
pub struct TaskSelectorConfig {
    pub enabled_tools: Vec<String>,
    pub tool_priority: Vec<String>,
    pub max_parallel_per_tool: HashMap<String, usize>,
    pub tool_specializations: HashMap<String, Vec<String>>,
    pub max_parallel: usize,
    pub default_tool: String,
    pub default_base_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedTask {
    pub id: String,
    pub title: String,
    pub tool: String,
    pub base_branch: String,
}

pub fn select_next_ready_task(
    registry: &Value,
    config: &TaskSelectorConfig,
) -> Option<SelectedTask> {
    let tasks = registry.get("tasks")?.as_array()?;

    let active_tasks: Vec<&Value> = tasks
        .iter()
        .filter(|t| is_active_state(task_state(t)))
        .collect();
    if config.max_parallel > 0 && active_tasks.len() >= config.max_parallel {
        return None;
    }

    let merged_ids: HashSet<String> = tasks
        .iter()
        .filter(|t| task_state(t) == "merged")
        .filter_map(|t| t.get("id").and_then(Value::as_str).map(ToOwned::to_owned))
        .collect();

    let mut active_by_tool: HashMap<String, usize> = HashMap::new();
    for task in active_tasks {
        if let Some(tool) = task.get("tool").and_then(Value::as_str) {
            *active_by_tool.entry(tool.to_string()).or_insert(0) += 1;
        }
    }

    let resource_locks = registry
        .get("resource_locks")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let mut candidates: Vec<(i32, String, String, SelectedTask)> = Vec::new();

    for task in tasks {
        if task_state(task) != "todo" {
            continue;
        }
        if task.get("worktree").is_some() && !task.get("worktree").unwrap().is_null() {
            continue;
        }

        let task_id = task.get("id").and_then(Value::as_str).unwrap_or_default();
        if task_id.is_empty() {
            continue;
        }

        if !dependencies_ready(task, &merged_ids) {
            continue;
        }
        if !resources_available(task, task_id, &resource_locks) {
            continue;
        }

        let Some(tool) = pick_tool(task, config, &active_by_tool) else {
            continue;
        };

        let title = task
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let base_branch = task
            .get("base_branch")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| config.default_base_branch.as_str())
            .to_string();
        let category = task
            .get("category")
            .and_then(Value::as_str)
            .unwrap_or("zzz")
            .to_string();
        let priority = parse_priority(task.get("priority"));

        candidates.push((
            priority,
            category,
            task_id.to_string(),
            SelectedTask {
                id: task_id.to_string(),
                title,
                tool,
                base_branch,
            },
        ));
    }

    candidates.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
    candidates.into_iter().next().map(|(_, _, _, s)| s)
}

fn task_state(task: &Value) -> &str {
    task.get("state").and_then(Value::as_str).unwrap_or("todo")
}

fn is_active_state(state: &str) -> bool {
    matches!(
        state,
        "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued"
    )
}

fn parse_priority(priority: Option<&Value>) -> i32 {
    match priority {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(99) as i32,
        Some(Value::String(s)) => {
            let v = s.trim().to_ascii_lowercase();
            match v.as_str() {
                "p0" => 0,
                "p1" => 1,
                "p2" => 2,
                "p3" => 3,
                "p4" => 4,
                _ => v.parse::<i32>().unwrap_or(99),
            }
        }
        _ => 99,
    }
}

fn dependencies_ready(task: &Value, merged_ids: &HashSet<String>) -> bool {
    let deps = task
        .get("dependencies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    deps.iter().all(|dep| {
        dep.as_str()
            .map(ToOwned::to_owned)
            .or_else(|| dep.as_i64().map(|n| n.to_string()))
            .map(|id| merged_ids.contains(&id))
            .unwrap_or(false)
    })
}

fn resources_available(
    task: &Value,
    task_id: &str,
    locks: &serde_json::Map<String, Value>,
) -> bool {
    let resources = task
        .get("exclusive_resources")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    resources.iter().all(|r| {
        let res = r.as_str().unwrap_or_default();
        if res.is_empty() {
            return true;
        }
        let owner = locks
            .get(res)
            .and_then(|v| v.get("task_id"))
            .and_then(Value::as_str)
            .unwrap_or("");
        owner.is_empty() || owner == task_id
    })
}

fn pick_tool(
    task: &Value,
    config: &TaskSelectorConfig,
    active_by_tool: &HashMap<String, usize>,
) -> Option<String> {
    let preference = preference_list(task, config);
    let fallback = fallback_pool(task, config, &preference);

    let mut combined = Vec::new();
    combined.extend(preference.iter().cloned());
    combined.extend(fallback);

    let mut uniq = Vec::new();
    let mut seen = HashSet::new();
    for t in combined {
        if seen.insert(t.clone()) {
            uniq.push(t);
        }
    }

    let enabled_set: Option<HashSet<String>> = if config.enabled_tools.is_empty() {
        None
    } else {
        Some(config.enabled_tools.iter().cloned().collect())
    };

    let pref_rank: BTreeMap<String, usize> = preference
        .iter()
        .enumerate()
        .map(|(i, t)| (t.clone(), i))
        .collect();

    let mut candidates: Vec<(usize, usize, String)> = Vec::new();
    for tool in uniq {
        if let Some(enabled) = &enabled_set {
            if !enabled.contains(&tool) {
                continue;
            }
        }
        if let Some(cap) = config.max_parallel_per_tool.get(&tool) {
            let current = *active_by_tool.get(&tool).unwrap_or(&0);
            if current >= *cap {
                continue;
            }
        }
        let rank = *pref_rank.get(&tool).unwrap_or(&999);
        let load = *active_by_tool.get(&tool).unwrap_or(&0);
        candidates.push((rank, load, tool));
    }

    candidates.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
    candidates.into_iter().next().map(|(_, _, t)| t)
}

fn preference_list(task: &Value, config: &TaskSelectorConfig) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(category) = task.get("category").and_then(Value::as_str) {
        if let Some(tools) = config.tool_specializations.get(category) {
            out.extend(tools.iter().cloned());
        }
    }
    if out.is_empty() {
        if let Some(task_tool) = task.get("tool").and_then(Value::as_str) {
            if !task_tool.is_empty() {
                out.push(task_tool.to_string());
            }
        } else if !config.tool_priority.is_empty() {
            out.extend(config.tool_priority.iter().cloned());
        }
    }
    dedup_and_clean(out)
}

fn fallback_pool(task: &Value, config: &TaskSelectorConfig, preference: &[String]) -> Vec<String> {
    if !config.enabled_tools.is_empty() {
        return config.enabled_tools.clone();
    }

    let mut out = Vec::new();
    out.extend(preference.iter().cloned());
    out.extend(config.tool_priority.iter().cloned());
    if let Some(task_tool) = task.get("tool").and_then(Value::as_str) {
        out.push(task_tool.to_string());
    }
    out.push(config.default_tool.clone());

    let mut specs = BTreeSet::new();
    for v in config.tool_specializations.values() {
        for t in v {
            specs.insert(t.clone());
        }
    }
    out.extend(specs);

    dedup_and_clean(out)
}

fn dedup_and_clean(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for v in values {
        let t = v.trim().to_string();
        if t.is_empty() {
            continue;
        }
        if seen.insert(t.clone()) {
            out.push(t);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn picks_highest_priority_ready_task() {
        let registry = json!({
          "tasks": [
            {"id":"B","title":"B","state":"todo","priority":"2","dependencies":[],"exclusive_resources":[]},
            {"id":"A","title":"A","state":"todo","priority":"1","dependencies":[],"exclusive_resources":[]}
          ],
          "resource_locks": {}
        });
        let cfg = TaskSelectorConfig {
            default_tool: "codex".into(),
            default_base_branch: "master".into(),
            max_parallel: 3,
            ..TaskSelectorConfig::default()
        };
        let selected = select_next_ready_task(&registry, &cfg).expect("selected task");
        assert_eq!(selected.id, "A");
    }

    #[test]
    fn respects_dependencies_and_resource_locks() {
        let registry = json!({
          "tasks": [
            {"id":"DEP","title":"dep","state":"todo","priority":"1","dependencies":["X"],"exclusive_resources":[]},
            {"id":"OK","title":"ok","state":"todo","priority":"2","dependencies":[],"exclusive_resources":["res-a"]},
            {"id":"X","title":"x","state":"merged","priority":"1","dependencies":[],"exclusive_resources":[]}
          ],
          "resource_locks": {
            "res-a": {"task_id":"OTHER"}
          }
        });
        let cfg = TaskSelectorConfig {
            default_tool: "codex".into(),
            default_base_branch: "master".into(),
            max_parallel: 3,
            ..TaskSelectorConfig::default()
        };
        let selected = select_next_ready_task(&registry, &cfg).expect("selected task");
        assert_eq!(selected.id, "DEP");
    }
}
