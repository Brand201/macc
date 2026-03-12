use crate::{MaccError, Result};
use std::path::Path;
use std::process::{ExitStatus, Output};

async fn run_git_output_async(current_dir: &Path, args: &[&str], action: &str) -> Result<Output> {
    tokio::process::Command::new("git")
        .args(args)
        .current_dir(current_dir)
        .output()
        .await
        .map_err(|e| MaccError::Io {
            path: current_dir.to_string_lossy().into(),
            action: action.to_string(),
            source: e,
        })
}

async fn run_git_status_async(
    current_dir: &Path,
    args: &[&str],
    action: &str,
) -> Result<ExitStatus> {
    tokio::process::Command::new("git")
        .args(args)
        .current_dir(current_dir)
        .status()
        .await
        .map_err(|e| MaccError::Io {
            path: current_dir.to_string_lossy().into(),
            action: action.to_string(),
            source: e,
        })
}

fn run_git_output(current_dir: &Path, args: &[&str], action: &str) -> Result<Output> {
    std::process::Command::new("git")
        .args(args)
        .current_dir(current_dir)
        .output()
        .map_err(|e| MaccError::Io {
            path: current_dir.to_string_lossy().into(),
            action: action.to_string(),
            source: e,
        })
}

fn run_git_status(current_dir: &Path, args: &[&str], action: &str) -> Result<ExitStatus> {
    std::process::Command::new("git")
        .args(args)
        .current_dir(current_dir)
        .status()
        .map_err(|e| MaccError::Io {
            path: current_dir.to_string_lossy().into(),
            action: action.to_string(),
            source: e,
        })
}

pub fn run_git_output_mapped(current_dir: &Path, args: &[&str], action: &str) -> Result<Output> {
    run_git_output(current_dir, args, action)
}

pub fn worktree_list_porcelain(repo_root: &Path) -> Result<String> {
    let output = run_git_output(
        repo_root,
        &["worktree", "list", "--porcelain"],
        "run git worktree list",
    )?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git worktree list failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn worktree_add(repo_root: &Path, branch: &str, path: &Path, base: &str) -> Result<()> {
    let output = run_git_output(
        repo_root,
        &[
            "worktree",
            "add",
            "-b",
            branch,
            path.to_string_lossy().as_ref(),
            base,
        ],
        "run git worktree add",
    )?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git worktree add failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

pub fn worktree_remove(repo_root: &Path, path: &Path, force: bool) -> Result<()> {
    let path_str = path.to_string_lossy().to_string();
    let args = if force {
        vec!["worktree", "remove", "--force", path_str.as_str()]
    } else {
        vec!["worktree", "remove", path_str.as_str()]
    };
    let output = run_git_output(repo_root, &args, "run git worktree remove")?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git worktree remove failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

pub fn worktree_prune(repo_root: &Path) -> Result<()> {
    let output = run_git_output(repo_root, &["worktree", "prune"], "run git worktree prune")?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git worktree prune failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

pub fn status_porcelain(repo_or_worktree: &Path) -> Result<String> {
    let output = run_git_output(
        repo_or_worktree,
        &["status", "--porcelain"],
        "read git status --porcelain",
    )?;
    if !output.status.success() {
        return Ok(String::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn is_dirty(repo_or_worktree: &Path) -> Result<bool> {
    Ok(!status_porcelain(repo_or_worktree)?.trim().is_empty())
}

pub async fn is_dirty_async(repo_or_worktree: &Path) -> Result<bool> {
    let output = run_git_output_async(
        repo_or_worktree,
        &["status", "--porcelain"],
        "read git status --porcelain",
    )
    .await?;
    if !output.status.success() {
        return Ok(false);
    }
    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

pub fn reset_hard(repo_or_worktree: &Path, target: &str) -> Result<bool> {
    Ok(run_git_status(
        repo_or_worktree,
        &["reset", "--hard", target],
        "run git reset --hard",
    )?
    .success())
}

pub async fn reset_hard_async(repo_or_worktree: &Path, target: &str) -> Result<bool> {
    Ok(run_git_status_async(
        repo_or_worktree,
        &["reset", "--hard", target],
        "run git reset --hard",
    )
    .await?
    .success())
}

pub fn clean_fd(repo_or_worktree: &Path) -> Result<bool> {
    Ok(run_git_status(repo_or_worktree, &["clean", "-fd"], "run git clean -fd")?.success())
}

pub async fn clean_fd_async(repo_or_worktree: &Path) -> Result<bool> {
    Ok(
        run_git_status_async(repo_or_worktree, &["clean", "-fd"], "run git clean -fd")
            .await?
            .success(),
    )
}

pub fn checkout(repo_or_worktree: &Path, branch: &str, force: bool) -> Result<bool> {
    let args = if force {
        vec!["checkout", "-f", branch]
    } else {
        vec!["checkout", branch]
    };
    Ok(run_git_status(repo_or_worktree, &args, "run git checkout")?.success())
}

pub async fn checkout_async(repo_or_worktree: &Path, branch: &str, force: bool) -> Result<bool> {
    let args = if force {
        vec!["checkout", "-f", branch]
    } else {
        vec!["checkout", branch]
    };
    Ok(
        run_git_status_async(repo_or_worktree, &args, "run git checkout")
            .await?
            .success(),
    )
}

pub fn checkout_reset_branch(repo_or_worktree: &Path, branch: &str, force: bool) -> Result<bool> {
    let args = if force {
        vec!["checkout", "-f", "-B", branch, branch]
    } else {
        vec!["checkout", "-B", branch, branch]
    };
    Ok(run_git_status(repo_or_worktree, &args, "run git checkout -B")?.success())
}

pub async fn checkout_reset_branch_async(
    repo_or_worktree: &Path,
    branch: &str,
    force: bool,
) -> Result<bool> {
    let args = if force {
        vec!["checkout", "-f", "-B", branch, branch]
    } else {
        vec!["checkout", "-B", branch, branch]
    };
    Ok(
        run_git_status_async(repo_or_worktree, &args, "run git checkout -B")
            .await?
            .success(),
    )
}

pub fn fetch(repo_or_worktree: &Path, remote: &str) -> Result<bool> {
    Ok(run_git_status(repo_or_worktree, &["fetch", remote], "run git fetch")?.success())
}

pub async fn fetch_async(repo_or_worktree: &Path, remote: &str) -> Result<bool> {
    Ok(
        run_git_status_async(repo_or_worktree, &["fetch", remote], "run git fetch")
            .await?
            .success(),
    )
}

pub fn merge_ff_only(repo_or_worktree: &Path, reference: &str) -> Result<bool> {
    Ok(run_git_status(
        repo_or_worktree,
        &["merge", "--ff-only", reference],
        "run git merge --ff-only",
    )?
    .success())
}

pub fn head_commit(repo_or_worktree: &Path) -> Result<String> {
    let output = run_git_output(repo_or_worktree, &["rev-parse", "HEAD"], "read git head")?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "Failed to resolve HEAD in {}",
            repo_or_worktree.display()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn current_branch(repo_or_worktree: &Path) -> Result<String> {
    let output = run_git_output(
        repo_or_worktree,
        &["rev-parse", "--abbrev-ref", "HEAD"],
        "read git current branch",
    )?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "Failed to resolve current branch in {}",
            repo_or_worktree.display()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn rev_parse_verify(repo_or_worktree: &Path, reference: &str) -> Result<bool> {
    Ok(run_git_status(
        repo_or_worktree,
        &["rev-parse", "--verify", reference],
        "run git rev-parse --verify",
    )?
    .success())
}

pub fn merge_base_is_ancestor(
    repo_or_worktree: &Path,
    ancestor: &str,
    descendant: &str,
) -> Result<bool> {
    Ok(run_git_status(
        repo_or_worktree,
        &["merge-base", "--is-ancestor", ancestor, descendant],
        "run git merge-base --is-ancestor",
    )?
    .success())
}

pub fn checkout_new_branch_from_base(
    repo_or_worktree: &Path,
    branch: &str,
    base_branch: &str,
) -> Result<bool> {
    Ok(run_git_status(
        repo_or_worktree,
        &["checkout", "-B", branch, base_branch],
        "create branch from base",
    )?
    .success())
}

pub async fn merge_ff_only_async(repo_or_worktree: &Path, reference: &str) -> Result<bool> {
    Ok(run_git_status_async(
        repo_or_worktree,
        &["merge", "--ff-only", reference],
        "run git merge --ff-only",
    )
    .await?
    .success())
}

pub async fn head_commit_async(repo_or_worktree: &Path) -> Result<String> {
    let output =
        run_git_output_async(repo_or_worktree, &["rev-parse", "HEAD"], "read git head").await?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "Failed to resolve HEAD in {}",
            repo_or_worktree.display()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn delete_local_branch(repo_root: &Path, branch: &str, force: bool) -> Result<()> {
    let normalized = branch.strip_prefix("refs/heads/").unwrap_or(branch);
    if normalized.is_empty() {
        return Ok(());
    }
    let args = if force {
        vec!["branch", "-D", normalized]
    } else {
        vec!["branch", "-d", normalized]
    };
    let output = run_git_output(repo_root, &args, "run git branch delete")?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git branch delete failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}
