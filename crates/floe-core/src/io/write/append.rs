use crate::{config, FloeResult};

use super::mode_strategy::{append_part_allocator, ModeStrategy, PartSpec, WriteContext};
use super::parts::PartNameAllocator;

pub struct AppendStrategy;

pub static APPEND_STRATEGY: AppendStrategy = AppendStrategy;

impl ModeStrategy for AppendStrategy {
    fn mode(&self) -> config::WriteMode {
        config::WriteMode::Append
    }

    fn part_allocator(
        &self,
        ctx: &mut WriteContext<'_>,
        spec: PartSpec,
    ) -> FloeResult<PartNameAllocator> {
        append_part_allocator(ctx, spec)
    }
}
