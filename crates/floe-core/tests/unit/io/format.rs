use floe_core::io::format::input_adapter;

#[test]
fn input_registry_returns_csv_adapter() {
    let adapter = input_adapter("csv").expect("adapter");
    assert_eq!(adapter.format(), "csv");
}

#[test]
fn input_registry_returns_fixed_adapter() {
    let adapter = input_adapter("fixed").expect("adapter");
    assert_eq!(adapter.format(), "fixed");
}
