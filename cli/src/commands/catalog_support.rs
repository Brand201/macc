use crate::commands::AppContext;
use macc_adapter_shared::catalog::{remote_search, SearchKind as RemoteSearchKind};
use macc_core::catalog::{McpCatalog, McpEntry, SkillEntry, SkillsCatalog};
use macc_core::service::interaction::InteractionHandler;
use macc_core::Result;

pub(crate) struct CliCatalogUi;

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

pub(crate) struct CliRemoteSearchProvider;

impl macc_core::catalog::service::CatalogRemoteSearchProvider for CliRemoteSearchProvider {
    fn search_skills(&self, api: &str, query: &str) -> Result<Vec<SkillEntry>> {
        remote_search(api, RemoteSearchKind::Skill, query)
    }

    fn search_mcp(&self, api: &str, query: &str) -> Result<Vec<McpEntry>> {
        remote_search(api, RemoteSearchKind::Mcp, query)
    }
}

pub(crate) struct CliCatalogInstallBackend<'a> {
    pub(crate) app: &'a AppContext,
}

impl macc_core::catalog::service::CatalogInstallBackend for CliCatalogInstallBackend<'_> {
    fn list_tools(
        &self,
        paths: &macc_core::ProjectPaths,
    ) -> (
        Vec<macc_core::tool::ToolDescriptor>,
        Vec<macc_core::tool::ToolDiagnostic>,
    ) {
        self.app.engine.list_tools(paths)
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
        self.app.engine.apply(paths, plan, allow_user_scope)
    }
}

pub(crate) struct CliCatalogUrlParser;

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

pub(crate) fn list_skills(app: &AppContext, catalog: &SkillsCatalog) {
    app.engine.catalog_list_skills(catalog, &CliCatalogUi);
}

pub(crate) fn search_skills(app: &AppContext, catalog: &SkillsCatalog, query: &str) {
    app.engine
        .catalog_search_skills(catalog, query, &CliCatalogUi);
}

pub(crate) fn list_mcp(app: &AppContext, catalog: &McpCatalog) {
    app.engine.catalog_list_mcp(catalog, &CliCatalogUi);
}

pub(crate) fn search_mcp(app: &AppContext, catalog: &McpCatalog, query: &str) {
    app.engine.catalog_search_mcp(catalog, query, &CliCatalogUi);
}
