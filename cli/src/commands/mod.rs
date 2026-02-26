use macc_core::config::CanonicalConfig;
use macc_core::engine::Engine;
use macc_core::{load_canonical_config, ProjectPaths, Result, ToolDescriptor};
use std::path::Path;

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
pub struct ProjectContext {
    pub paths: ProjectPaths,
    pub canonical: CanonicalConfig,
    pub descriptors: Vec<ToolDescriptor>,
    pub allowed_tools: Vec<String>,
}

impl ProjectContext {
    pub fn load<E: Engine>(absolute_cwd: &Path, engine: &E) -> Result<Self> {
        let paths = macc_core::find_project_root(absolute_cwd)?;
        let canonical = load_canonical_config(&paths.config_path)?;
        let (descriptors, _diagnostics) = engine.list_tools(&paths);
        let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
        Ok(Self {
            paths,
            canonical,
            descriptors,
            allowed_tools,
        })
    }
}
