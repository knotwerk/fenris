use carbon_resources_core::ResourceCatalog;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStage {
    Discover,
    Hash,
    Compress,
    Patch,
}

pub fn stage_count(_catalog: &ResourceCatalog) -> usize {
    4
}
