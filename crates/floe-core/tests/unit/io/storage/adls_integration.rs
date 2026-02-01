use floe_core::config;
use floe_core::io::storage::{adls::AdlsClient, StorageClient};
use floe_core::FloeResult;
use std::env;

#[test]
fn adls_list_optional() -> FloeResult<()> {
    if env::var("FLOE_TEST_ADLS").ok().as_deref() != Some("1") {
        return Ok(());
    }
    let account = env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default();
    let container = env::var("AZURE_STORAGE_CONTAINER").unwrap_or_default();
    if account.is_empty() || container.is_empty() {
        return Ok(());
    }
    let definition = config::StorageDefinition {
        name: "adls".to_string(),
        fs_type: "adls".to_string(),
        bucket: None,
        region: None,
        account: Some(account),
        container: Some(container),
        prefix: None,
    };
    let client = AdlsClient::new(&definition)?;
    let refs = client.list("")?;
    let _ = refs.len();
    Ok(())
}
