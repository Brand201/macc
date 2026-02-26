use macc_core::engine::Engine;
use std::sync::Arc;

pub type SharedEngine = Arc<dyn Engine + Send + Sync>;

#[derive(Clone)]
pub struct EngineProvider {
    engine: SharedEngine,
}

impl EngineProvider {
    pub fn new<E>(engine: E) -> Self
    where
        E: Engine + Send + Sync + 'static,
    {
        Self {
            engine: Arc::new(engine),
        }
    }

    pub fn shared(&self) -> SharedEngine {
        Arc::clone(&self.engine)
    }
}
