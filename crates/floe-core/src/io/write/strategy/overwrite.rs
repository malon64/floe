use crate::{config, FloeResult};

use super::parts::PartNameAllocator;
use super::{overwrite_part_allocator, ModeStrategy, PartSpec, WriteContext};

pub struct OverwriteStrategy;

pub static OVERWRITE_STRATEGY: OverwriteStrategy = OverwriteStrategy;

impl ModeStrategy for OverwriteStrategy {
    fn mode(&self) -> config::WriteMode {
        config::WriteMode::Overwrite
    }

    fn part_allocator(
        &self,
        ctx: &mut WriteContext<'_>,
        spec: PartSpec,
    ) -> FloeResult<PartNameAllocator> {
        overwrite_part_allocator(ctx, spec)
    }
}
