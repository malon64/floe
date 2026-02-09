use floe_core::io::read::json_selector::{
    compact_json, evaluate_selector, parse_selector, selector_is_top_level, SelectorToken,
    SelectorValue,
};
use serde_json::json;

#[test]
fn parse_selector_simple_field() {
    let tokens = parse_selector("user").expect("parse selector");
    assert_eq!(tokens, vec![SelectorToken::Field("user".to_string())]);
}

#[test]
fn parse_selector_nested_fields() {
    let tokens = parse_selector("a.b.c").expect("parse selector");
    assert_eq!(
        tokens,
        vec![
            SelectorToken::Field("a".to_string()),
            SelectorToken::Field("b".to_string()),
            SelectorToken::Field("c".to_string())
        ]
    );
}

#[test]
fn parse_selector_with_index() {
    let tokens = parse_selector("a[2]").expect("parse selector");
    assert_eq!(
        tokens,
        vec![
            SelectorToken::Field("a".to_string()),
            SelectorToken::Index(2)
        ]
    );
}

#[test]
fn parse_selector_nested_with_index() {
    let tokens = parse_selector("a.b[0].c").expect("parse selector");
    assert_eq!(
        tokens,
        vec![
            SelectorToken::Field("a".to_string()),
            SelectorToken::Field("b".to_string()),
            SelectorToken::Index(0),
            SelectorToken::Field("c".to_string())
        ]
    );
}

#[test]
fn parse_selector_empty_string_errors() {
    assert!(parse_selector("").is_err());
}

#[test]
fn parse_selector_empty_field_errors() {
    assert!(parse_selector(".a").is_err());
}

#[test]
fn parse_selector_empty_index_errors() {
    assert!(parse_selector("a[]").is_err());
}

#[test]
fn parse_selector_non_numeric_index_errors() {
    assert!(parse_selector("a[foo]").is_err());
}

#[test]
fn parse_selector_unexpected_bracket_errors() {
    assert!(parse_selector("a]").is_err());
}

#[test]
fn evaluate_selector_missing_field_returns_null() {
    let value = json!({"a": 1});
    let tokens = parse_selector("missing").expect("parse selector");
    assert_eq!(evaluate_selector(&value, &tokens), Ok(SelectorValue::Null));
}

#[test]
fn evaluate_selector_missing_index_returns_null() {
    let value = json!({"a": [1]});
    let tokens = parse_selector("a[2]").expect("parse selector");
    assert_eq!(evaluate_selector(&value, &tokens), Ok(SelectorValue::Null));
}

#[test]
fn evaluate_selector_wrong_type_returns_null() {
    let value = json!({"a": 1});
    let tokens = parse_selector("a.b").expect("parse selector");
    assert_eq!(evaluate_selector(&value, &tokens), Ok(SelectorValue::Null));
}

#[test]
fn evaluate_selector_scalar_string() {
    let value = json!({"a": {"b": "ok"}});
    let tokens = parse_selector("a.b").expect("parse selector");
    assert_eq!(
        evaluate_selector(&value, &tokens),
        Ok(SelectorValue::Scalar("ok".to_string()))
    );
}

#[test]
fn evaluate_selector_scalar_number() {
    let value = json!({"a": 42});
    let tokens = parse_selector("a").expect("parse selector");
    assert_eq!(
        evaluate_selector(&value, &tokens),
        Ok(SelectorValue::Scalar("42".to_string()))
    );
}

#[test]
fn evaluate_selector_scalar_bool() {
    let value = json!({"a": true});
    let tokens = parse_selector("a").expect("parse selector");
    assert_eq!(
        evaluate_selector(&value, &tokens),
        Ok(SelectorValue::Scalar("true".to_string()))
    );
}

#[test]
fn evaluate_selector_null() {
    let value = json!({"a": null});
    let tokens = parse_selector("a").expect("parse selector");
    assert_eq!(evaluate_selector(&value, &tokens), Ok(SelectorValue::Null));
}

#[test]
fn evaluate_selector_non_scalar() {
    let value = json!({"a": {"b": 1}});
    let tokens = parse_selector("a").expect("parse selector");
    match evaluate_selector(&value, &tokens) {
        Ok(SelectorValue::NonScalar(val)) => assert!(val.is_object()),
        other => panic!("unexpected selector value: {:?}", other),
    }
}

#[test]
fn compact_json_is_minified() {
    let value = json!({"a": 1, "b": [true, false]});
    let compact = compact_json(&value).expect("compact json");
    assert_eq!(compact, "{\"a\":1,\"b\":[true,false]}");
}

#[test]
fn selector_is_top_level_only_for_simple_fields() {
    assert!(selector_is_top_level("a"));
    assert!(!selector_is_top_level("a.b"));
    assert!(!selector_is_top_level("a[0]"));
}
