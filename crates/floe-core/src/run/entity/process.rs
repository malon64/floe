pub(super) fn sink_options_warning(entity: &config::EntityConfig) -> Option<String> {
    crate::io::format::sink_options_warning(
        &entity.name,
        entity.sink.accepted.format.as_str(),
        entity.sink.accepted.options.as_ref(),
    )
}

pub(super) fn append_sink_options_warning(rules: &mut Vec<report::RuleSummary>, message: &str) {
    let column = "sink.accepted.options".to_string();
    if let Some(rule) = rules
        .iter_mut()
        .find(|rule| rule.rule == report::RuleName::SchemaError)
    {
        rule.violations += 1;
        rule.severity = report::Severity::Warn;
        if let Some(entry) = rule.columns.iter_mut().find(|entry| entry.column == column) {
            entry.violations += 1;
        } else {
            rule.columns.push(report::ColumnSummary {
                column: column.clone(),
                violations: 1,
                target_type: None,
            });
        }
    } else {
        rules.push(report::RuleSummary {
            rule: report::RuleName::SchemaError,
            severity: report::Severity::Warn,
            violations: 1,
            columns: vec![report::ColumnSummary {
                column: column.clone(),
                violations: 1,
                target_type: None,
            }],
        });
    }

    let _ = message;
}
use crate::{config, report};
