use floe_core::io::read::xml_selector::{parse_selector, SelectorToken};

#[test]
fn parse_selector_rejects_empty_segment_dot() {
    let err = parse_selector("user..name").expect_err("expected invalid selector");
    assert!(err.message.contains("empty token"));
}

#[test]
fn parse_selector_rejects_empty_segment_leading_slash() {
    let err = parse_selector("/name").expect_err("expected invalid selector");
    assert!(err.message.contains("empty token"));
}

#[test]
fn parse_selector_rejects_empty_segment_trailing_slash() {
    let err = parse_selector("name/").expect_err("expected invalid selector");
    assert!(err.message.contains("empty token"));
}

#[test]
fn parse_selector_accepts_attribute_terminal_token() {
    let tokens = parse_selector("customer.@id").expect("expected valid selector");
    assert_eq!(tokens.len(), 2);
    assert!(matches!(tokens[0], SelectorToken::Element(ref name) if name == "customer"));
    assert!(matches!(tokens[1], SelectorToken::Attribute(ref name) if name == "id"));
}
