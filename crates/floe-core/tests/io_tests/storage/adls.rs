use floe_core::io::storage::{filter_by_suffixes, join_prefix, stable_sort_refs, ObjectRef};

#[test]
fn join_prefix_normalizes() {
    assert_eq!(join_prefix("prefix/", "/file.csv"), "prefix/file.csv");
}

#[test]
fn adls_like_refs_sort_and_filter() {
    let refs = vec![
        ObjectRef {
            uri: "abfs://cont@acct.dfs.core.windows.net/data/b.csv".to_string(),
            key: "data/b.csv".to_string(),
            last_modified: None,
            size: None,
        },
        ObjectRef {
            uri: "abfs://cont@acct.dfs.core.windows.net/data/a.csv".to_string(),
            key: "data/a.csv".to_string(),
            last_modified: None,
            size: None,
        },
        ObjectRef {
            uri: "abfs://cont@acct.dfs.core.windows.net/data/c.txt".to_string(),
            key: "data/c.txt".to_string(),
            last_modified: None,
            size: None,
        },
    ];
    let filtered = filter_by_suffixes(refs, &[".csv".to_string()]);
    let sorted = stable_sort_refs(filtered);
    let keys = sorted.into_iter().map(|obj| obj.key).collect::<Vec<_>>();
    assert_eq!(keys, vec!["data/a.csv", "data/b.csv"]);
}
