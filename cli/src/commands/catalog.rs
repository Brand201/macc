use crate::commands::catalog_support::{
    list_mcp, list_skills, search_mcp, search_skills, CliCatalogUi, CliCatalogUrlParser,
    CliRemoteSearchProvider,
};
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
                    list_skills(&self.app, &catalog);
                    Ok(())
                }
                crate::CatalogSubCommands::Search { query } => {
                    let catalog = load_effective_skills_catalog(&paths)?;
                    search_skills(&self.app, &catalog, query);
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
                    self.app.engine.catalog_add_skill(
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
                        &CliCatalogUi,
                    )
                }
                crate::CatalogSubCommands::Remove { id } => {
                    let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
                    self.app.engine.catalog_remove_skill(
                        &paths,
                        &mut catalog,
                        id.clone(),
                        &CliCatalogUi,
                    )
                }
            },
            CatalogCommands::Mcp { mcp_command } => match mcp_command {
                crate::CatalogSubCommands::List => {
                    let catalog = load_effective_mcp_catalog(&paths)?;
                    list_mcp(&self.app, &catalog);
                    Ok(())
                }
                crate::CatalogSubCommands::Search { query } => {
                    let catalog = load_effective_mcp_catalog(&paths)?;
                    search_mcp(&self.app, &catalog, query);
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
                    self.app.engine.catalog_add_mcp(
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
                        &CliCatalogUi,
                    )
                }
                crate::CatalogSubCommands::Remove { id } => {
                    let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
                    self.app.engine.catalog_remove_mcp(
                        &paths,
                        &mut catalog,
                        id.clone(),
                        &CliCatalogUi,
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
            } => self.app.engine.catalog_import_url(
                &paths,
                kind,
                id.clone(),
                url.clone(),
                name.clone(),
                description.clone(),
                tags.clone(),
                &CliCatalogUrlParser,
                &CliCatalogUi,
            ),
            CatalogCommands::SearchRemote {
                api,
                kind,
                q,
                add,
                add_ids,
            } => self.app.engine.catalog_run_remote_search(
                &paths,
                &CliRemoteSearchProvider,
                api,
                kind,
                q,
                *add,
                add_ids.as_deref(),
                &CliCatalogUi,
            ),
        }
    }
}
