use crate::commands::AppContext;
use crate::commands::Command;
use crate::CatalogCommands;
use macc_core::catalog::{
    load_effective_mcp_catalog, load_effective_skills_catalog, McpCatalog, SkillsCatalog,
};
use macc_core::Result;
pub struct CatalogCommand<'a> {
    app: AppContext,
    command: &'a CatalogCommands,
}

impl<'a> CatalogCommand<'a> {
    pub fn new(app: AppContext, command: &'a CatalogCommands) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for CatalogCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        match self.command {
            CatalogCommands::Skills { skills_command } => match skills_command {
                crate::CatalogSubCommands::List => {
                    let catalog = load_effective_skills_catalog(&paths)?;
                    crate::services::catalog::list_skills(&self.app.engine, &catalog);
                    Ok(())
                }
                crate::CatalogSubCommands::Search { query } => {
                    let catalog = load_effective_skills_catalog(&paths)?;
                    crate::services::catalog::search_skills(&self.app.engine, &catalog, query);
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
                        &self.app.engine,
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
                    crate::services::catalog::remove_skill(
                        &self.app.engine,
                        &paths,
                        &mut catalog,
                        id.clone(),
                    )
                }
            },
            CatalogCommands::Mcp { mcp_command } => match mcp_command {
                crate::CatalogSubCommands::List => {
                    let catalog = load_effective_mcp_catalog(&paths)?;
                    crate::services::catalog::list_mcp(&self.app.engine, &catalog);
                    Ok(())
                }
                crate::CatalogSubCommands::Search { query } => {
                    let catalog = load_effective_mcp_catalog(&paths)?;
                    crate::services::catalog::search_mcp(&self.app.engine, &catalog, query);
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
                        &self.app.engine,
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
                    crate::services::catalog::remove_mcp(
                        &self.app.engine,
                        &paths,
                        &mut catalog,
                        id.clone(),
                    )
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
                &self.app.engine,
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
                &self.app.engine,
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
