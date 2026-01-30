use std::path::PathBuf;

use floe_core::config::ResolvedPath;
use floe_core::io::storage::Target;
use floe_core::FloeResult;

#[test]
fn target_from_resolved_local() -> FloeResult<()> {
    let local_path = PathBuf::from("/tmp/floe/input.csv");
    let resolved = ResolvedPath {
        storage: "local".to_string(),
        uri: format!("local://{}", local_path.display()),
        local_path: Some(local_path.clone()),
    };
    let target = Target::from_resolved(&resolved)?;
    assert!(matches!(target, Target::Local { .. }));
    assert_eq!(target.target_uri(), resolved.uri);
    assert!(target.s3_parts().is_none());
    Ok(())
}

#[test]
fn target_from_resolved_s3() -> FloeResult<()> {
    let resolved = ResolvedPath {
        storage: "s3_raw".to_string(),
        uri: "s3://bucket/path/file.csv".to_string(),
        local_path: None,
    };
    let target = Target::from_resolved(&resolved)?;
    assert!(matches!(target, Target::S3 { .. }));
    assert_eq!(target.target_uri(), resolved.uri);
    assert_eq!(target.s3_parts(), Some(("bucket", "path/file.csv")));
    Ok(())
}
