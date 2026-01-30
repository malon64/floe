use floe_core::io::storage::{join_prefix, stable_sort_refs, ObjectRef};

#[test]
fn join_prefix_handles_empty_edges() {
    assert_eq!(join_prefix("", "file.txt"), "file.txt");
    assert_eq!(join_prefix("root", ""), "root");
    assert_eq!(join_prefix("root/", "/file.txt"), "root/file.txt");
}

#[test]
fn stable_sort_orders_by_uri() {
    let refs = vec![
        ObjectRef {
            uri: "s3://b/zzz".to_string(),
            key: "zzz".to_string(),
            last_modified: None,
            size: None,
        },
        ObjectRef {
            uri: "s3://b/aaa".to_string(),
            key: "aaa".to_string(),
            last_modified: None,
            size: None,
        },
    ];
    let sorted = stable_sort_refs(refs);
    assert_eq!(sorted[0].uri, "s3://b/aaa");
    assert_eq!(sorted[1].uri, "s3://b/zzz");
}
