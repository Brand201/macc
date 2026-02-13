mod adapter;
mod doctor;
mod emit;
mod map;

pub use adapter::CodexAdapter;

inventory::submit! {
    macc_core::tool::AdapterRegistration {
        factory: || std::sync::Arc::new(CodexAdapter)
    }
}
