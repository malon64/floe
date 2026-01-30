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
    assert!(target.gcs_parts().is_none());
    assert!(target.adls_parts().is_none());
    Ok(())
}

#[test]
fn target_from_resolved_gcs() -> FloeResult<()> {
    let resolved = ResolvedPath {
        storage: "gcs_raw".to_string(),
        uri: "gs://bucket/path/file.csv".to_string(),
        local_path: None,
    };
    let target = Target::from_resolved(&resolved)?;
    assert!(matches!(target, Target::Gcs { .. }));
    assert_eq!(target.target_uri(), resolved.uri);
    assert_eq!(target.gcs_parts(), Some(("bucket", "path/file.csv")));
    assert!(target.s3_parts().is_none());
    assert!(target.adls_parts().is_none());
    Ok(())
}

#[test]
fn target_from_resolved_adls() -> FloeResult<()> {
    let resolved = ResolvedPath {
        storage: "adls_raw".to_string(),
        uri: "abfs://cont@acct.dfs.core.windows.net/data/file.csv".to_string(),
        local_path: None,
    };
    let target = Target::from_resolved(&resolved)?;
    assert!(matches!(target, Target::Adls { .. }));
    assert_eq!(target.target_uri(), resolved.uri);
    assert_eq!(target.adls_parts(), Some(("cont", "acct", "data/file.csv")));
    assert!(target.s3_parts().is_none());
    assert!(target.gcs_parts().is_none());
    Ok(())
}
