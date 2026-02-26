use crate::commands::Command;
use crate::CatalogCommands;
use macc_core::catalog::{
    load_effective_mcp_catalog, load_effective_skills_catalog, McpCatalog, SkillsCatalog,
};
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct CatalogCommand<'a> {
    cwd: PathBuf,
    command: &'a CatalogCommands,
}

impl<'a> CatalogCommand<'a> {
    pub fn new(cwd: &Path, command: &'a CatalogCommands) -> Self {
        Self { cwd: cwd.to_path_buf(), command }
    }
}

impl<'a> Command for CatalogCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = macc_core::find_project_root(&self.cwd)?;
        match self.command {
            CatalogCommands::Skills { skills_command } => match skills_command {
                crate::CatalogSubCommands::List => {
                    let catalog = load_effective_skills_catalog(&paths)?;
                    crate::services::catalog::list_skills(&catalog);
                    Ok(())
                }
                crate::CatalogSubCommands::Search { query } => {
                    let catalog = load_effective_skills_catalog(&paths)?;
                    crate::services::catalog::search_skills(&catalog, query);
                    Ok(())
                }
                crate::CatalogSubCommands::Add {
                    id,
                    name,
                    description,
                    tags,
                    subpath,
                    kind,
                    url,
                    reference,
                    checksum,
                } => {
                    let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
                    crate::services::catalog::add_skill(
                        &paths,
                        &mut catalog,
                        id.clone(),
                        name.clone(),
                        description.clone(),
                        tags.clone(),
                        subpath.clone(),
                        kind.clone(),
                        url.clone(),
                        reference.clone(),
                        checksum.clone(),
                    )
                }
                crate::CatalogSubCommands::Remove { id } => {
                    let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
                    crate::services::catalog::remove_skill(&paths, &mut catalog, id.clone())
                }
            },
            CatalogCommands::Mcp { mcp_command } => match mcp_command {
                crate::CatalogSubCommands::List => {
                    let catalog = load_effective_mcp_catalog(&paths)?;
                    crate::services::catalog::list_mcp(&catalog);
                    Ok(())
                }
                crate::CatalogSubCommands::Search { query } => {
                    let catalog = load_effective_mcp_catalog(&paths)?;
                    crate::services::catalog::search_mcp(&catalog, query);
                    Ok(())
                }
                crate::CatalogSubCommands::Add {
                    id,
                    name,
                    description,
                    tags,
                    subpath,
                    kind,
                    url,
                    reference,
                    checksum,
                } => {
                    let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
                    crate::services::catalog::add_mcp(
                        &paths,
                        &mut catalog,
                        id.clone(),
                        name.clone(),
                        description.clone(),
                        tags.clone(),
                        subpath.clone(),
                        kind.clone(),
                        url.clone(),
                        reference.clone(),
                        checksum.clone(),
                    )
                }
                crate::CatalogSubCommands::Remove { id } => {
                    let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
                    crate::services::catalog::remove_mcp(&paths, &mut catalog, id.clone())
                }
            },
            CatalogCommands::ImportUrl {
                kind,
                id,
                url,
                name,
                description,
                tags,
            } => crate::services::catalog::import_url(
                &paths,
                kind,
                id.clone(),
                url.clone(),
                name.clone(),
                description.clone(),
                tags.clone(),
            ),
            CatalogCommands::SearchRemote {
                api,
                kind,
                q,
                add,
                add_ids,
            } => crate::services::catalog::run_remote_search(
                &paths,
                api.clone(),
                kind.clone(),
                q.clone(),
                *add,
                add_ids.clone(),
            ),
        }
    }
}
