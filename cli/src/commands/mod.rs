use macc_core::config::CanonicalConfig;
use macc_core::{load_canonical_config, ProjectPaths, Result, ToolDescriptor};
use std::path::PathBuf;

pub mod coordinator;
pub mod init;
pub mod plan;
pub mod apply;
pub mod quickstart;
pub mod catalog;
pub mod install;
pub mod tool;
pub mod context;
pub mod doctor;
pub mod migrate;
pub mod backups;
pub mod restore;
pub mod clear;
pub mod worktree;
pub mod logs;

pub trait Command {
    fn run(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct AppContext {
    pub cwd: PathBuf,
    pub engine: crate::services::engine_provider::SharedEngine,
}

impl AppContext {
    pub fn new(cwd: PathBuf, engine: crate::services::engine_provider::SharedEngine) -> Self {
        Self { cwd, engine }
    }

    pub fn project_paths(&self) -> Result<ProjectPaths> {
        macc_core::find_project_root(&self.cwd)
    }

    pub fn ensure_initialized_paths(&self) -> Result<ProjectPaths> {
        crate::services::project::ensure_initialized_paths(&self.cwd)
    }
}

#[derive(Clone)]
pub struct ProjectContext {
    pub paths: ProjectPaths,
    pub canonical: CanonicalConfig,
    pub descriptors: Vec<ToolDescriptor>,
    pub allowed_tools: Vec<String>,
}

impl ProjectContext {
    pub fn load(app: &AppContext) -> Result<Self> {
        let paths = app.project_paths()?;
        let canonical = load_canonical_config(&paths.config_path)?;
        let (descriptors, _diagnostics) = app.engine.list_tools(&paths);
        let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
        Ok(Self {
            paths,
            canonical,
            descriptors,
            allowed_tools,
        })
    }
}
