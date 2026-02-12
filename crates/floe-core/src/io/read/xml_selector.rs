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

    let parts = trimmed.split(['.', '/']).collect::<Vec<_>>();
    if parts.iter().any(|part| part.trim().is_empty()) {
        // Reject malformed selectors like `user..id`, `/id`, or `id/` instead of
        // silently normalizing them to a different path.
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
