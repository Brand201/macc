use crate::services::engine_provider::SharedEngine;
use macc_adapter_shared::catalog::{remote_search, SearchKind as RemoteSearchKind};
use macc_core::catalog::{McpCatalog, SkillEntry, SkillsCatalog};
use macc_core::service::interaction::InteractionHandler;
use macc_core::Result;

#[derive(Clone, Copy)]
struct CliCatalogUi;

impl InteractionHandler for CliCatalogUi {
    fn info(&self, message: &str) {
        println!("{}", message);
    }
    fn warn(&self, message: &str) {
        eprintln!("{}", message);
    }
    fn error(&self, message: &str) {
        eprintln!("{}", message);
    }
}

struct CliRemoteSearchProvider;

impl macc_core::catalog::service::CatalogRemoteSearchProvider for CliRemoteSearchProvider {
    fn search_skills(&self, api: &str, query: &str) -> Result<Vec<SkillEntry>> {
        remote_search(api, RemoteSearchKind::Skill, query)
    }

    fn search_mcp(&self, api: &str, query: &str) -> Result<Vec<macc_core::catalog::McpEntry>> {
        remote_search(api, RemoteSearchKind::Mcp, query)
    }
}

struct CliCatalogInstallBackend<'a> {
    engine: &'a SharedEngine,
}

impl macc_core::catalog::service::CatalogInstallBackend for CliCatalogInstallBackend<'_> {
    fn list_tools(
        &self,
        paths: &macc_core::ProjectPaths,
    ) -> (
        Vec<macc_core::tool::ToolDescriptor>,
        Vec<macc_core::tool::ToolDiagnostic>,
    ) {
        self.engine.list_tools(paths)
    }

    fn materialize_fetch_unit(
        &self,
        paths: &macc_core::ProjectPaths,
        fetch_unit: macc_core::resolve::FetchUnit,
    ) -> Result<macc_core::resolve::MaterializedFetchUnit> {
        macc_adapter_shared::fetch::materialize_fetch_unit(paths, fetch_unit)
    }

    fn apply(
        &self,
        paths: &macc_core::ProjectPaths,
        plan: &mut macc_core::plan::ActionPlan,
        allow_user_scope: bool,
    ) -> Result<macc_core::ApplyReport> {
        self.engine.apply(paths, plan, allow_user_scope)
    }
}

struct CliCatalogUrlParser;

impl macc_core::service::catalog::CatalogUrlParser for CliCatalogUrlParser {
    fn normalize_git_input(
        &self,
        value: &str,
    ) -> Option<macc_core::service::catalog::NormalizedGitInput> {
        macc_adapter_shared::url_parsing::normalize_git_input(value).map(|normalized| {
            macc_core::service::catalog::NormalizedGitInput {
                clone_url: normalized.clone_url,
                reference: normalized.reference,
                subpath: normalized.subpath,
            }
        })
    }

    fn validate_http_url(&self, value: &str) -> bool {
        macc_adapter_shared::url_parsing::validate_http_url(value)
    }
}

pub fn run_remote_search(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    api: String,
    kind: String,
    q: String,
    add: bool,
    add_ids: Option<String>,
) -> Result<()> {
    engine.catalog_run_remote_search(
        paths,
        &CliRemoteSearchProvider,
        &api,
        &kind,
        &q,
        add,
        add_ids.as_deref(),
        &CliCatalogUi,
    )
}

pub fn list_skills(engine: &SharedEngine, catalog: &SkillsCatalog) {
    engine.catalog_list_skills(catalog, &CliCatalogUi);
}

pub fn search_skills(engine: &SharedEngine, catalog: &SkillsCatalog, query: &str) {
    engine.catalog_search_skills(catalog, query, &CliCatalogUi);
}

#[allow(clippy::too_many_arguments)]
pub fn add_skill(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
) -> Result<()> {
    engine.catalog_add_skill(
        paths,
        catalog,
        id,
        name,
        description,
        tags,
        subpath,
        kind,
        url,
        reference,
        checksum,
        &CliCatalogUi,
    )
}

pub fn remove_skill(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
) -> Result<()> {
    engine.catalog_remove_skill(paths, catalog, id, &CliCatalogUi)
}

pub fn list_mcp(engine: &SharedEngine, catalog: &McpCatalog) {
    engine.catalog_list_mcp(catalog, &CliCatalogUi);
}

pub fn search_mcp(engine: &SharedEngine, catalog: &McpCatalog, query: &str) {
    engine.catalog_search_mcp(catalog, query, &CliCatalogUi);
}

#[allow(clippy::too_many_arguments)]
pub fn add_mcp(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    catalog: &mut McpCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
) -> Result<()> {
    engine.catalog_add_mcp(
        paths,
        catalog,
        id,
        name,
        description,
        tags,
        subpath,
        kind,
        url,
        reference,
        checksum,
        &CliCatalogUi,
    )
}

pub fn remove_mcp(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    catalog: &mut McpCatalog,
    id: String,
) -> Result<()> {
    engine.catalog_remove_mcp(paths, catalog, id, &CliCatalogUi)
}

pub fn install_skill(
    paths: &macc_core::ProjectPaths,
    tool: &str,
    id: &str,
    engine: &SharedEngine,
) -> Result<()> {
    let backend = CliCatalogInstallBackend { engine };
    engine.catalog_install_skill(paths, tool, id, &backend, &CliCatalogUi)
}

pub fn install_mcp(paths: &macc_core::ProjectPaths, id: &str, engine: &SharedEngine) -> Result<()> {
    let backend = CliCatalogInstallBackend { engine };
    engine.catalog_install_mcp(paths, id, &backend, &CliCatalogUi)
}

pub fn import_url(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    kind: &str,
    id: String,
    url: String,
    name: Option<String>,
    description: String,
    tags: Option<String>,
) -> Result<()> {
    engine.catalog_import_url(
        paths,
        kind,
        id,
        url,
        name,
        description,
        tags,
        &CliCatalogUrlParser,
        &CliCatalogUi,
    )
}
