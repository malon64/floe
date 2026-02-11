#[derive(Debug, Clone)]
pub struct SelectorError {
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum SelectorToken {
    Element(String),
    Attribute(String),
}

pub fn parse_selector(value: &str) -> Result<Vec<SelectorToken>, SelectorError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(SelectorError {
            message: "selector is empty".to_string(),
        });
    }

    let parts = trimmed
        .split(|ch| ch == '.' || ch == '/')
        .collect::<Vec<_>>();
    if parts.iter().any(|part| part.trim().is_empty()) {
        return Err(SelectorError {
            message: "selector contains empty token".to_string(),
        });
    }

    let mut tokens = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let token = part.trim();
        if token.starts_with('@') {
            if index + 1 != parts.len() {
                return Err(SelectorError {
                    message: "attribute selector must be final token".to_string(),
                });
            }
            let attr = token.trim_start_matches('@').trim();
            if attr.is_empty() {
                return Err(SelectorError {
                    message: "attribute selector is empty".to_string(),
                });
            }
            tokens.push(SelectorToken::Attribute(attr.to_string()));
            continue;
        }
        if token.contains('@') {
            return Err(SelectorError {
                message: format!("invalid selector token: {token}"),
            });
        }
        tokens.push(SelectorToken::Element(token.to_string()));
    }
    Ok(tokens)
}

pub fn is_top_level_selector(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    !trimmed.contains('.') && !trimmed.contains('/') && !trimmed.contains('[')
}

#[cfg(test)]
mod tests {
    use super::{parse_selector, SelectorToken};

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
}
