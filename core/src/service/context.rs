use crate::config::CanonicalConfig;
use crate::service::interaction::InteractionHandler;
use crate::tool::{ToolPerformerSpec, ToolSpec, ToolSpecLoader};
use crate::{load_canonical_config, MaccError, ProjectPaths, Result};

pub fn run_generation(
    paths: &ProjectPaths,
    tool_filter: Option<&str>,
    from_files: &[String],
    dry_run: bool,
    print_prompt: bool,
    reporter: &dyn InteractionHandler,
) -> Result<usize> {
    require_apply_before_context(paths)?;

    let canonical = load_canonical_config(&paths.config_path)?;
    let loader = ToolSpecLoader::new(ToolSpecLoader::default_search_paths(&paths.root));
    let (specs, diagnostics) = loader.load_all_with_embedded();
    crate::service::project::report_diagnostics(&diagnostics, reporter);

    let selected_tools: Vec<String> = if let Some(tool_id) = tool_filter {
        vec![tool_id.to_string()]
    } else {
        canonical.tools.enabled.clone()
    };

    if selected_tools.is_empty() {
        return Err(MaccError::Validation(
            "No tool selected. Enable tools in .macc/macc.yaml or pass --tool <id>.".into(),
        ));
    }

    let mut generated = 0usize;
    let mut missing_tools = Vec::new();
    for tool_id in selected_tools {
        let Some(spec) = specs.iter().find(|s| s.id == tool_id) else {
            missing_tools.push(tool_id.clone());
            reporter.warn(&format!("Skipping '{}': ToolSpec not found.", tool_id));
            continue;
        };
        let performer = spec.performer.as_ref().ok_or_else(|| {
            MaccError::Validation(format!(
                "Tool '{}' has no performer config; cannot generate context via AI tool.",
                tool_id
            ))
        })?;

        let target_rel = resolve_context_target_rel(&canonical, spec);
        let target_abs = paths.root.join(&target_rel);
        let prompt = build_context_prompt(paths, &canonical, spec, &target_rel, from_files)?;

        if print_prompt {
            reporter.info(&format!(
                "\n--- Prompt for {} ({}) ---\n{}\n",
                spec.display_name, spec.id, prompt
            ));
        }

        if dry_run {
            reporter.info(&format!(
                "[dry-run] tool={} target={} prompt_chars={}",
                spec.id,
                target_rel,
                prompt.chars().count()
            ));
            generated += 1;
            continue;
        }

        if let Some(parent) = target_abs.parent() {
            std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
                path: parent.to_string_lossy().into(),
                action: "create context target parent directory".into(),
                source: e,
            })?;
        }

        invoke_context_tool(paths, performer, &prompt)?;

        if !target_abs.is_file() {
            return Err(MaccError::Validation(format!(
                "Tool '{}' completed but did not produce '{}'. Ensure the agent writes that file directly.",
                spec.id, target_rel
            )));
        }

        reporter.info(&format!(
            "Context updated in-place: {} via {}",
            target_rel, spec.display_name
        ));
        generated += 1;
    }

    if generated == 0 {
        if tool_filter.is_some() && !missing_tools.is_empty() {
            return Err(MaccError::Validation(format!(
                "ToolSpec not found for tool '{}'.",
                missing_tools[0]
            )));
        }
        return Err(MaccError::Validation(
            "No context files generated. Check enabled tools and ToolSpecs.".into(),
        ));
    }

    reporter.info(&format!(
        "Context generation complete. Files handled: {}",
        generated
    ));
    Ok(generated)
}

pub fn context_apply_marker_path(paths: &ProjectPaths) -> std::path::PathBuf {
    paths
        .macc_dir
        .join("state")
        .join("context_ready_after_apply")
}

pub fn require_apply_before_context(paths: &ProjectPaths) -> Result<()> {
    let marker = context_apply_marker_path(paths);
    if marker.exists() {
        return Ok(());
    }
    Err(MaccError::Validation(
        "macc context is locked until at least one successful 'macc apply' has completed in this project.".into(),
    ))
}

pub fn mark_apply_completed(paths: &ProjectPaths) -> Result<()> {
    let marker = context_apply_marker_path(paths);
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create apply marker directory".into(),
            source: e,
        })?;
    }
    std::fs::write(&marker, b"applied\n").map_err(|e| MaccError::Io {
        path: marker.to_string_lossy().into(),
        action: "write apply marker".into(),
        source: e,
    })?;
    Ok(())
}

fn resolve_context_target_rel(canonical: &CanonicalConfig, spec: &ToolSpec) -> String {
    if let Some(rel) = context_target_from_tool_settings(canonical, &spec.id) {
        return rel;
    }

    if let Some(md) = spec.gitignore.iter().find_map(|entry| {
        let path = std::path::Path::new(entry);
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext.eq_ignore_ascii_case("md") {
            Some(entry.clone())
        } else {
            None
        }
    }) {
        return md;
    }

    format!("{}.md", spec.id.to_ascii_uppercase().replace('-', "_"))
}

fn context_target_from_tool_settings(canonical: &CanonicalConfig, tool_id: &str) -> Option<String> {
    let config_map_entry = canonical.tools.config.get(tool_id);
    let legacy_entry = canonical.tools.settings.get(tool_id);
    for entry in [config_map_entry, legacy_entry].into_iter().flatten() {
        if let Some(target) = extract_context_file_name_from_json(entry) {
            return Some(target);
        }
    }
    None
}

fn extract_context_file_name_from_json(value: &serde_json::Value) -> Option<String> {
    let context = value.get("context")?;
    let file_name = context.get("fileName")?;
    match file_name {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(items) => items.first()?.as_str().map(|s| s.to_string()),
        _ => None,
    }
}

fn build_context_prompt(
    paths: &ProjectPaths,
    canonical: &CanonicalConfig,
    spec: &ToolSpec,
    target_rel: &str,
    from_files: &[String],
) -> Result<String> {
    let mut sources: Vec<String> = Vec::new();
    for item in from_files {
        if !sources.contains(item) {
            sources.push(item.clone());
        }
    }

    let mut snippets = Vec::new();
    for rel in sources {
        let abs = paths.root.join(&rel);
        if !abs.exists() || !abs.is_file() {
            continue;
        }
        let content = std::fs::read_to_string(&abs).map_err(|e| MaccError::Io {
            path: abs.to_string_lossy().into(),
            action: "read context source file".into(),
            source: e,
        })?;
        let excerpt = truncate_text_for_prompt(&content, 6000);
        snippets.push((rel, excerpt));
    }

    let mut prompt = String::new();
    prompt.push_str(
        "You are a technical audit agent and developer assistant embedded in this repository.\n",
    );
    prompt.push_str(&format!(
        "Your mission is to update `{}` as operational context and a working guide for {} AI agent (or developer) contributing to this project.\n\n",
        target_rel,
        spec.id
    ));
    prompt.push_str(&format!("Tool ID: {}\n", spec.id));
    prompt.push_str(&format!("Tool Name: {}\n", spec.display_name));
    prompt.push_str(&format!("Target file: {}\n", target_rel));
    prompt.push_str(&format!(
        "Enabled tools: {}\n\n",
        canonical.tools.enabled.join(", ")
    ));
    prompt.push_str("Strict constraints\n");
    prompt.push_str("- Rely only on the repository's actual contents (README, docs, folder structure, config, CI, scripts).\n");
    prompt.push_str("- Do not invent anything.\n");
    prompt.push_str("- If information is missing, write: `Unknown (to verify)` + indicate where to find it (files/commands).\n");
    prompt.push_str("- For important statements (setup, commands, CI, tests, env vars, rules), indicate source as: `seen in <path/file>`.\n");
    prompt.push_str("- Priority: security + compliance + quality + maintainability.\n");
    prompt.push_str("- Style: clear, actionable, concise Markdown with checklists.\n\n");
    prompt.push_str("Required method (perform before writing)\n");
    prompt.push_str(
        "1. Scan the folder structure: identify modules, entry points, key directories.\n",
    );
    prompt.push_str("2. Detect the stack: languages, frameworks, dependency management, tooling (lint/format/build).\n");
    prompt.push_str("3. Map workflows: local execution, tests, CI, release.\n");
    prompt.push_str("4. Security audit: secrets, auth, permissions, dependencies, sensitive data, attack surfaces.\n");
    prompt.push_str("5. Compliance audit: licenses, personal data, logs, retention, traceability, requirements (if present).\n");
    prompt.push_str(
        "6. Tests & quality audit: test types, coverage, flakiness, mocks, fixtures, strategy.\n",
    );
    prompt.push_str("7. Synthesis: produce a context file that is immediately usable.\n\n");
    prompt.push_str(
        "Mandatory skill-routing section (must be present in the generated context file)\n",
    );
    prompt.push_str("Add a dedicated `# Project Mandates` section and keep it deterministic.\n");
    prompt.push_str("Separate clearly:\n");
    prompt.push_str("- Global mandates (always valid)\n");
    prompt.push_str("- Mode-specific mandates (active only for the current phase)\n");
    prompt.push_str("Use repository-relative paths only (no absolute paths):\n");
    prompt.push_str("- planning -> `skills/macc-prd-planner/SKILL.md`\n");
    prompt.push_str("- execution -> `skills/macc-performer/SKILL.md`\n");
    prompt.push_str("- review -> `skills/macc-code-reviewer/SKILL.md`\n");
    prompt.push_str("Fallback rule (explicit and mandatory): if a required skill file is absent or inaccessible, stop and report the error.\n");
    prompt.push_str("Add a `## Path validation` subsection with a short checklist that verifies these files exist.\n");
    prompt.push_str("Add `## Architecture Source of Truth` with repository-relative references:\n");
    prompt.push_str("- `skills/macc-performer/docs/ERRORS.md`\n");
    prompt.push_str("- `skills/macc-performer/docs/adr/0000-template.md`\n\n");
    prompt.push_str("Deliverable: write the target file with this exact outline\n");
    prompt.push_str("0. Project Mandates (global + mode-specific + path validation)\n");
    prompt.push_str("1. TL;DR (max 10 lines)\n");
    prompt.push_str("2. Project identity card\n");
    prompt.push_str("3. Stack & tooling (with sources)\n");
    prompt.push_str("4. Architecture & components\n");
    prompt.push_str("5. Reproducible local setup\n");
    prompt.push_str("6. Essential commands (copy/paste)\n");
    prompt.push_str("7. Developer standards (Do / Don't)\n");
    prompt.push_str("8. Test & quality strategy\n");
    prompt.push_str("9. Productivity playbooks (typical tasks)\n");
    prompt.push_str("10. Security (priority)\n");
    prompt.push_str("11. Compliance & governance\n");
    prompt.push_str("12. \"Where to find what\" (agent FAQ)\n");
    prompt.push_str("13. Unknowns & documentation debt\n\n");
    prompt.push_str("Output rules\n");
    prompt.push_str(&format!(
        "- Edit `{}` directly in the repository.\n",
        target_rel
    ));
    prompt.push_str("- Do not return the full file content in output.\n");
    prompt.push_str("- At the end, print a short status line indicating the file was updated.\n");
    prompt.push_str("- Every command must be copyable, exact, and sourced when possible.\n");
    prompt.push_str(
        "- Add Markdown checklists (`- [ ]`) for PR / security / release (if applicable).\n",
    );
    prompt.push_str("- Clearly mark what is observed vs inferred.\n\n");

    if snippets.is_empty() {
        prompt.push_str("Sources:\n- none provided\n");
    } else {
        prompt.push_str("Sources:\n");
        for (rel, excerpt) in snippets {
            prompt.push_str(&format!("\n--- BEGIN SOURCE: {} ---\n", rel));
            prompt.push_str(&excerpt);
            prompt.push_str(&format!("\n--- END SOURCE: {} ---\n", rel));
        }
    }
    Ok(prompt)
}

fn truncate_text_for_prompt(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    input.chars().take(max_chars).collect::<String>()
}

fn invoke_context_tool(
    paths: &ProjectPaths,
    performer: &ToolPerformerSpec,
    prompt: &str,
) -> Result<()> {
    if !command_exists(&performer.command) {
        return Err(MaccError::Validation(format!(
            "Tool command '{}' not found in PATH. Run 'macc doctor' and install/login the tool first.",
            performer.command
        )));
    }

    let mut cmd = std::process::Command::new(&performer.command);
    cmd.current_dir(&paths.root);
    cmd.args(&performer.args);

    let prompt_mode = performer
        .prompt
        .as_ref()
        .map(|p| p.mode.as_str())
        .unwrap_or("stdin");

    match prompt_mode {
        "arg" => {
            let arg = performer
                .prompt
                .as_ref()
                .and_then(|p| p.arg.as_ref())
                .ok_or_else(|| {
                    MaccError::Validation(format!(
                        "Tool '{}' prompt mode is 'arg' but no prompt arg is configured.",
                        performer.command
                    ))
                })?;
            cmd.arg(arg);
            cmd.arg(prompt);
            let output = cmd.output().map_err(|e| MaccError::Io {
                path: performer.command.clone(),
                action: "run tool context generation command".into(),
                source: e,
            })?;
            validate_context_tool_exit(&performer.command, output)
        }
        "stdin" => {
            use std::io::Write;
            let mut child = cmd
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| MaccError::Io {
                    path: performer.command.clone(),
                    action: "spawn tool context generation command".into(),
                    source: e,
                })?;
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(prompt.as_bytes())
                    .map_err(|e| MaccError::Io {
                        path: performer.command.clone(),
                        action: "write prompt to tool stdin".into(),
                        source: e,
                    })?;
            }
            let output = child.wait_with_output().map_err(|e| MaccError::Io {
                path: performer.command.clone(),
                action: "wait for tool context generation command".into(),
                source: e,
            })?;
            validate_context_tool_exit(&performer.command, output)
        }
        other => Err(MaccError::Validation(format!(
            "Unsupported prompt mode '{}' for tool '{}'.",
            other, performer.command
        ))),
    }
}

fn validate_context_tool_exit(command: &str, output: std::process::Output) -> Result<()> {
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        let reason = if !stderr.trim().is_empty() {
            stderr.trim().to_string()
        } else if !stdout.trim().is_empty() {
            stdout.trim().to_string()
        } else {
            format!("exit status {}", output.status)
        };
        return Err(MaccError::Validation(format!(
            "Context generation command '{}' failed: {}",
            command, reason
        )));
    }
    Ok(())
}

fn command_exists(cmd: &str) -> bool {
    std::process::Command::new("sh")
        .arg("-lc")
        .arg(format!("command -v {} >/dev/null 2>&1", cmd))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
