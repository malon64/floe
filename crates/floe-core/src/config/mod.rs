mod catalog;
mod location;
mod parse;
mod storage;
mod template;
mod types;
mod validate;
pub(crate) mod yaml_decode;

pub use catalog::{CatalogResolver, ResolvedDeltaCatalogTarget, ResolvedIcebergCatalogTarget};
pub use location::{resolve_config_location, ConfigLocation};
pub use storage::{resolve_local_path, ConfigBase, ResolvedPath, StorageResolver};
pub use types::*;

pub use parse::extract_raw_env_vars;
pub(crate) use parse::{parse_catalogs_with_context, parse_config, parse_config_with_vars};
pub(crate) use template::apply_templates_with_vars;
pub(crate) use validate::{extract_first_n, extract_last_n, validate_config};
