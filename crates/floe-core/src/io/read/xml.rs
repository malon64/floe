use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use polars::prelude::{DataFrame, NamedFrom, Series};
use roxmltree::{Document, Node};

use crate::io::format::{self, FileReadError, InputAdapter, InputFile, ReadInput};
use crate::io::read::xml_selector::{parse_selector, SelectorToken};
use crate::{config, FloeResult};

struct XmlInputAdapter;

static XML_INPUT_ADAPTER: XmlInputAdapter = XmlInputAdapter;

pub(crate) fn xml_input_adapter() -> &'static dyn InputAdapter {
    &XML_INPUT_ADAPTER
}

#[derive(Debug, Clone)]
pub struct XmlReadError {
    pub rule: String,
    pub message: String,
}

impl std::fmt::Display for XmlReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.rule, self.message)
    }
}

impl std::error::Error for XmlReadError {}

struct SelectorPlan {
    source: String,
    tokens: Vec<SelectorToken>,
}

fn build_selector_plan(
    columns: &[config::ColumnConfig],
) -> Result<Vec<SelectorPlan>, XmlReadError> {
    let mut plans = Vec::with_capacity(columns.len());
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        let source = column.source_or_name().to_string();
        if !seen.insert(source.clone()) {
            return Err(XmlReadError {
                rule: "xml_selector_invalid".to_string(),
                message: format!("duplicate xml selector source: {}", source),
            });
        }
        let tokens = parse_selector(&source).map_err(|err| XmlReadError {
            rule: "xml_selector_invalid".to_string(),
            message: format!("invalid selector {}: {}", source, err.message),
        })?;
        plans.push(SelectorPlan { source, tokens });
    }
    Ok(plans)
}

fn split_tag(tag: &str) -> (Option<&str>, &str) {
    if let Some((prefix, local)) = tag.split_once(':') {
        (Some(prefix), local)
    } else {
        (None, tag)
    }
}

fn matches_tag(node: Node<'_, '_>, tag: &str, namespace: Option<&str>) -> bool {
    let (prefix, local) = split_tag(tag);
    let name = node.tag_name();
    if name.name() != local {
        return false;
    }
    if prefix.is_some() {
        if let Some(ns) = namespace {
            return name.namespace() == Some(ns);
        }
        return true;
    }
    namespace.is_none_or(|ns| name.namespace() == Some(ns))
}

fn matches_namespace(node: Node<'_, '_>, namespace: Option<&str>) -> bool {
    namespace.is_none_or(|ns| node.tag_name().namespace() == Some(ns))
}

fn collect_text(node: Node<'_, '_>) -> Option<String> {
    let mut text = String::new();
    for descendant in node.descendants() {
        if descendant.is_text() {
            if let Some(value) = descendant.text() {
                text.push_str(value);
            }
        }
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn resolve_value_node<'a>(
    node: Node<'a, 'a>,
    value_tag: Option<&str>,
    namespace: Option<&str>,
) -> Node<'a, 'a> {
    let Some(value_tag) = value_tag else {
        return node;
    };
    for descendant in node.descendants().skip(1) {
        if descendant.is_element() && matches_tag(descendant, value_tag, namespace) {
            return descendant;
        }
    }
    node
}

fn find_child<'a>(node: Node<'a, 'a>, tag: &str, namespace: Option<&str>) -> Option<Node<'a, 'a>> {
    node.children()
        .find(|child| child.is_element() && matches_tag(*child, tag, namespace))
}

fn evaluate_selector(
    row: Node<'_, '_>,
    tokens: &[SelectorToken],
    namespace: Option<&str>,
    value_tag: Option<&str>,
) -> Option<String> {
    let mut current = row;
    for token in tokens {
        match token {
            SelectorToken::Element(tag) => {
                let next = find_child(current, tag.as_str(), namespace)?;
                current = next;
            }
            SelectorToken::Attribute(attr) => {
                return current
                    .attribute(attr.as_str())
                    .map(|value| value.to_string());
            }
        }
    }
    let value_node = resolve_value_node(current, value_tag, namespace);
    collect_text(value_node)
}

fn read_xml_columns(
    input_path: &Path,
    row_tag: &str,
    namespace: Option<&str>,
) -> Result<Vec<String>, XmlReadError> {
    let content = std::fs::read_to_string(input_path).map_err(|err| XmlReadError {
        rule: "xml_parse_error".to_string(),
        message: format!("failed to read xml at {}: {err}", input_path.display()),
    })?;
    let doc = Document::parse(&content).map_err(|err| XmlReadError {
        rule: "xml_parse_error".to_string(),
        message: format!("xml parse error: {err}"),
    })?;
    let row = doc
        .descendants()
        .find(|node| node.is_element() && matches_tag(*node, row_tag, namespace))
        .ok_or_else(|| XmlReadError {
            rule: "xml_parse_error".to_string(),
            message: format!("row_tag={} not found in xml", row_tag),
        })?;

    let mut names = BTreeSet::new();
    for child in row.children().filter(|node| node.is_element()) {
        if !matches_namespace(child, namespace) {
            continue;
        }
        names.insert(child.tag_name().name().to_string());
    }
    Ok(names.into_iter().collect())
}

fn read_xml_file(
    input_path: &Path,
    columns: &[config::ColumnConfig],
    row_tag: &str,
    namespace: Option<&str>,
    value_tag: Option<&str>,
) -> Result<DataFrame, XmlReadError> {
    let content = std::fs::read_to_string(input_path).map_err(|err| XmlReadError {
        rule: "xml_parse_error".to_string(),
        message: format!("failed to read xml at {}: {err}", input_path.display()),
    })?;
    let doc = Document::parse(&content).map_err(|err| XmlReadError {
        rule: "xml_parse_error".to_string(),
        message: format!("xml parse error: {err}"),
    })?;
    let plans = build_selector_plan(columns)?;
    let mut rows: Vec<BTreeMap<String, Option<String>>> = Vec::new();

    for row in doc
        .descendants()
        .filter(|node| node.is_element() && matches_tag(*node, row_tag, namespace))
    {
        let mut record = BTreeMap::new();
        for plan in &plans {
            let value = evaluate_selector(row, &plan.tokens, namespace, value_tag);
            record.insert(plan.source.clone(), value);
        }
        rows.push(record);
    }

    if rows.is_empty() {
        return Err(XmlReadError {
            rule: "xml_parse_error".to_string(),
            message: format!("row_tag={} produced no rows", row_tag),
        });
    }

    let columns = plans
        .iter()
        .map(|plan| plan.source.clone())
        .collect::<Vec<_>>();
    build_dataframe(&columns, &rows)
}

fn build_dataframe(
    columns: &[String],
    rows: &[BTreeMap<String, Option<String>>],
) -> Result<DataFrame, XmlReadError> {
    let mut series = Vec::with_capacity(columns.len());
    for name in columns {
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            values.push(row.get(name).cloned().unwrap_or(None));
        }
        series.push(Series::new(name.as_str().into(), values).into());
    }

    DataFrame::new(series).map_err(|err| XmlReadError {
        rule: "xml_parse_error".to_string(),
        message: format!("failed to build dataframe: {err}"),
    })
}

fn xml_options(
    entity: &config::EntityConfig,
) -> Result<(String, Option<String>, Option<String>), XmlReadError> {
    let options = entity.source.options.as_ref().ok_or_else(|| XmlReadError {
        rule: "xml_parse_error".to_string(),
        message: "source.options is required for xml input".to_string(),
    })?;
    let row_tag = options
        .row_tag
        .as_deref()
        .ok_or_else(|| XmlReadError {
            rule: "xml_parse_error".to_string(),
            message: "source.options.row_tag is required for xml input".to_string(),
        })?
        .trim()
        .to_string();
    if row_tag.is_empty() {
        return Err(XmlReadError {
            rule: "xml_parse_error".to_string(),
            message: "source.options.row_tag is required for xml input".to_string(),
        });
    }
    let namespace = options
        .namespace
        .as_deref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());
    let value_tag = options
        .value_tag
        .as_deref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());
    Ok((row_tag, namespace, value_tag))
}

impl InputAdapter for XmlInputAdapter {
    fn format(&self) -> &'static str {
        "xml"
    }

    fn read_input_columns(
        &self,
        entity: &config::EntityConfig,
        input_file: &InputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError> {
        let (row_tag, namespace, _value_tag) =
            xml_options(entity).map_err(|err| FileReadError {
                rule: err.rule,
                message: err.message,
            })?;
        read_xml_columns(
            &input_file.source_local_path,
            &row_tag,
            namespace.as_deref(),
        )
        .map_err(|err| FileReadError {
            rule: err.rule,
            message: err.message,
        })
    }

    fn read_inputs(
        &self,
        entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>> {
        let mut inputs = Vec::with_capacity(files.len());
        let (row_tag, namespace, value_tag) = match xml_options(entity) {
            Ok(options) => options,
            Err(err) => {
                for input_file in files {
                    inputs.push(ReadInput::FileError {
                        input_file: input_file.clone(),
                        error: FileReadError {
                            rule: err.rule.clone(),
                            message: err.message.clone(),
                        },
                    });
                }
                return Ok(inputs);
            }
        };

        for input_file in files {
            let path = &input_file.source_local_path;
            let read_result = read_xml_file(
                path,
                columns,
                &row_tag,
                namespace.as_deref(),
                value_tag.as_deref(),
            );
            match read_result {
                Ok(df) => {
                    let input = format::read_input_from_df(
                        input_file,
                        &df,
                        columns,
                        normalize_strategy,
                        collect_raw,
                    )?;
                    inputs.push(input);
                }
                Err(err) => {
                    inputs.push(ReadInput::FileError {
                        input_file: input_file.clone(),
                        error: FileReadError {
                            rule: err.rule,
                            message: err.message,
                        },
                    });
                }
            }
        }
        Ok(inputs)
    }
}
