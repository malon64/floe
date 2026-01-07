#[derive(Debug)]
pub struct RootConfig {
    pub version: String,
    pub metadata: Option<ProjectMetadata>,
    pub entities: Vec<EntityConfig>,
}

#[derive(Debug)]
pub struct ProjectMetadata {
    pub project: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct EntityConfig {
    pub name: String,
    pub metadata: Option<EntityMetadata>,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub policy: PolicyConfig,
    pub schema: SchemaConfig,
}

#[derive(Debug)]
pub struct EntityMetadata {
    pub data_product: Option<String>,
    pub domain: Option<String>,
    pub owner: Option<String>,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct SourceConfig {
    pub format: String,
    pub path: String,
    pub options: Option<SourceOptions>,
    pub cast_mode: Option<String>,
}

#[derive(Debug)]
pub struct SourceOptions {
    pub header: Option<bool>,
    pub separator: Option<String>,
    pub encoding: Option<String>,
    pub null_values: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct SinkConfig {
    pub accepted: SinkTarget,
    pub rejected: SinkTarget,
    pub report: ReportTarget,
}

#[derive(Debug)]
pub struct SinkTarget {
    pub format: String,
    pub path: String,
}

#[derive(Debug)]
pub struct ReportTarget {
    pub path: String,
}

#[derive(Debug)]
pub struct PolicyConfig {
    pub default_severity: Option<String>,
    pub on_schema_error: Option<String>,
    pub quarantine: Option<QuarantineConfig>,
    pub thresholds: Option<ThresholdsConfig>,
}

#[derive(Debug)]
pub struct QuarantineConfig {
    pub mode: Option<String>,
    pub add_reason_columns: Option<bool>,
    pub reason_columns: Option<ReasonColumns>,
}

#[derive(Debug)]
pub struct ReasonColumns {
    pub rule: Option<String>,
    pub column: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug)]
pub struct ThresholdsConfig {
    pub max_reject_rate: Option<f64>,
    pub max_reject_count: Option<u64>,
}

#[derive(Debug)]
pub struct SchemaConfig {
    pub normalize_columns: Option<NormalizeColumnsConfig>,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug)]
pub struct NormalizeColumnsConfig {
    pub enabled: Option<bool>,
    pub strategy: Option<String>,
}

#[derive(Debug)]
pub struct ColumnConfig {
    pub name: String,
    // YAML key is `type`, map manually when parsing.
    pub column_type: String,
    pub nullable: Option<bool>,
    pub unique: Option<bool>,
    pub unique_strategy: Option<String>,
}
