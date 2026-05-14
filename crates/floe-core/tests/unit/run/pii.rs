use floe_core::config::PiiStrategy;

mod mask_pattern_math {
    fn apply_mask(
        value: &str,
        pattern: &str,
        first_n: Option<usize>,
        last_n: Option<usize>,
    ) -> String {
        let fn_val = first_n.unwrap_or(0);
        let ln_val = last_n.unwrap_or(0);
        let char_count = value.chars().count();
        if char_count < fn_val + ln_val {
            return value.to_string();
        }
        let prefix: String = value.chars().take(fn_val).collect();
        let suffix: String = value.chars().skip(char_count - ln_val).collect();
        let mut result = pattern.to_string();
        if fn_val > 0 {
            result = result.replace(&format!("{{first{fn_val}}}"), &prefix);
        }
        if ln_val > 0 {
            result = result.replace(&format!("{{last{ln_val}}}"), &suffix);
        }
        result
    }

    #[test]
    fn mask_last4() {
        let out = apply_mask("1234567890", "****{last4}", None, Some(4));
        assert_eq!(out, "****7890");
    }

    #[test]
    fn mask_first2() {
        let out = apply_mask("abcdef", "{first2}****", Some(2), None);
        assert_eq!(out, "ab****");
    }

    #[test]
    fn mask_first2_last4() {
        let out = apply_mask("1234567890", "{first2}...{last4}", Some(2), Some(4));
        assert_eq!(out, "12...7890");
    }

    #[test]
    fn mask_short_value_passthrough() {
        // first2 + last4 = 6 > 4 chars → return unchanged
        let out = apply_mask("1234", "{first2}...{last4}", Some(2), Some(4));
        assert_eq!(out, "1234");
    }

    #[test]
    fn mask_exact_length_last4() {
        let out = apply_mask("12345678", "****{last4}", None, Some(4));
        assert_eq!(out, "****5678");
    }

    #[test]
    fn mask_multibyte_chars_last2() {
        // "éàü" are 2-byte UTF-8 chars; slicing by byte would panic
        let out = apply_mask("éàüXY", "**{last2}", None, Some(2));
        assert_eq!(out, "**XY");
    }

    #[test]
    fn mask_multibyte_chars_first2() {
        let out = apply_mask("éàüXY", "{first2}**", Some(2), None);
        assert_eq!(out, "éà**");
    }
}

#[test]
fn pii_strategy_variants_are_distinct() {
    assert_ne!(PiiStrategy::Hash, PiiStrategy::Drop);
    assert_ne!(PiiStrategy::Nullify, PiiStrategy::Redact);
    assert_ne!(PiiStrategy::Mask, PiiStrategy::Tokenize);
}
