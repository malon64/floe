use crate::{config, io, FloeResult};

#[derive(Debug, Clone)]
pub enum Target {
    Local {
        storage: String,
        uri: String,
        base_path: String,
    },
    S3 {
        storage: String,
        uri: String,
        bucket: String,
        base_key: String,
    },
}

impl Target {
    pub fn from_resolved(resolved: &config::ResolvedPath) -> FloeResult<Self> {
        if let Some(path) = &resolved.local_path {
            return Ok(Target::Local {
                storage: resolved.storage.clone(),
                uri: resolved.uri.clone(),
                base_path: path.display().to_string(),
            });
        }
        let location = io::storage::s3::parse_s3_uri(&resolved.uri)?;
        Ok(Target::S3 {
            storage: resolved.storage.clone(),
            uri: resolved.uri.clone(),
            bucket: location.bucket,
            base_key: location.key,
        })
    }

    pub fn storage(&self) -> &str {
        match self {
            Target::Local { storage, .. } | Target::S3 { storage, .. } => storage.as_str(),
        }
    }

    pub fn target_uri(&self) -> &str {
        match self {
            Target::Local { uri, .. } | Target::S3 { uri, .. } => uri.as_str(),
        }
    }

    pub fn s3_parts(&self) -> Option<(&str, &str)> {
        match self {
            Target::S3 {
                bucket, base_key, ..
            } => Some((bucket.as_str(), base_key.as_str())),
            Target::Local { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn target_from_resolved_local() -> crate::FloeResult<()> {
        let local_path = PathBuf::from("/tmp/floe/input.csv");
        let resolved = crate::config::ResolvedPath {
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
    fn target_from_resolved_s3() -> crate::FloeResult<()> {
        let resolved = crate::config::ResolvedPath {
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
}
