mod parse;
mod types;
mod validate;

pub use parse::{parse_profile, parse_profile_from_str};
pub use types::{
    ProfileConfig, ProfileExecution, ProfileMetadata, ProfileRunner, ProfileValidation,
    PROFILE_API_VERSION, PROFILE_KIND,
};
pub use validate::{
    detect_malformed_placeholder, detect_unresolved_placeholders, validate_merged_vars,
    validate_profile,
};
