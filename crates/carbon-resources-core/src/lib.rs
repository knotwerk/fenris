use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::c_void;
use std::fs;
use std::io::{Cursor, Read, Write};
use std::os::raw::{c_char, c_int, c_uint, c_ulong};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader as ArrowFileReader;
use arrow::ipc::writer::FileWriter as ArrowFileWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

const LEGACY_BSDIFF_HEADER: &[u8; 16] = b"ENDSLEY/BSDIFF43";
const LEGACY_BSDIFF_HEADER_SIZE: usize = LEGACY_BSDIFF_HEADER.len() + 8;
pub const RESOURCE_CATALOG_ARROW_SCHEMA: &str = "carbon.resources.catalog.arrow.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceCatalogBackend {
    Legacy,
    ArrowIpc,
    Parquet,
}

impl ResourceCatalogBackend {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "legacy" => Some(Self::Legacy),
            "arrow-ipc" => Some(Self::ArrowIpc),
            "parquet" => Some(Self::Parquet),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::ArrowIpc => "arrow-ipc",
            Self::Parquet => "parquet",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRecord {
    pub path: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub size_bytes: u64,
    #[serde(default)]
    pub compressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub binary_operation: Option<u64>,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceCatalog {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub catalog_type: String,
    #[serde(default)]
    pub total_compressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub total_uncompressed_size_bytes: u64,
    pub resources: Vec<ResourceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleResourceRecord {
    pub path: String,
    pub resource_type: String,
    pub location: String,
    pub size_bytes: u64,
    pub compressed_size_bytes: Option<u64>,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleResourceCatalog {
    pub version: String,
    pub catalog_type: String,
    pub total_compressed_size_bytes: Option<u64>,
    pub total_uncompressed_size_bytes: u64,
    pub resource_group_resource: BundleResourceRecord,
    pub chunk_size: u64,
    pub resources: Vec<BundleResourceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyBundleDataResource {
    pub record: BundleResourceRecord,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedLegacyLocalBundle {
    pub catalog: BundleResourceCatalog,
    pub resource_group_resource: LegacyBundleDataResource,
    pub chunks: Vec<LegacyBundleDataResource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyBundleChunkDestination {
    LocalCdn,
    RemoteCdn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchResourceGroupRecord {
    pub path: String,
    pub resource_type: String,
    pub location: String,
    pub size_bytes: u64,
    pub compressed_size_bytes: Option<u64>,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchResourceRecord {
    pub path: String,
    pub resource_type: String,
    pub location: String,
    pub size_bytes: u64,
    pub compressed_size_bytes: Option<u64>,
    pub checksum: String,
    pub target_resource_relative_path: String,
    pub data_offset: u64,
    pub source_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchResourceCatalog {
    pub version: String,
    pub catalog_type: String,
    pub total_compressed_size_bytes: Option<u64>,
    pub total_uncompressed_size_bytes: u64,
    pub resource_group_resource: PatchResourceGroupRecord,
    pub max_input_chunk_size: u64,
    pub removed_resource_relative_paths: Option<Vec<String>>,
    pub resources: Vec<PatchResourceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyUnpackedBundleResource {
    pub path: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnpackedLegacyLocalBundle {
    pub resource_group_resource: LegacyBundleDataResource,
    pub resource_catalog: ResourceCatalog,
    pub resources: Vec<LegacyUnpackedBundleResource>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LegacyRemoteCdnCacheStats {
    pub cache_hits: u64,
    pub downloads: u64,
    pub replaced_bad_cache_entries: u64,
    pub bytes_copied_to_cache: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnpackedLegacyRemoteBundle {
    pub unpacked: UnpackedLegacyLocalBundle,
    pub cache_stats: LegacyRemoteCdnCacheStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyPatchDataResource {
    pub path: String,
    pub resource_type: String,
    pub location: String,
    pub checksum: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyPatchDataSet {
    pub resource_group_resource: LegacyPatchDataResource,
    pub resources: Vec<LegacyPatchDataResource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedLegacyLocalPatch {
    pub catalog: PatchResourceCatalog,
    pub resource_group_resource: LegacyPatchDataResource,
    pub resources: Vec<LegacyPatchDataResource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedLegacyPatchResource {
    pub path: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedLegacyPatchSet {
    pub resource_group_resource: LegacyPatchDataResource,
    pub resource_catalog: ResourceCatalog,
    pub resources: Vec<AppliedLegacyPatchResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterIndexMapping {
    pub filter_file_paths: Vec<String>,
    pub output_index_filename: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegacyResourceResultType {
    FailedToParseYaml,
    MalformedResourceGroup,
    MalformedResourceInput,
    DocumentVersionUnsupported,
}

impl LegacyResourceResultType {
    pub fn as_legacy_name(self) -> &'static str {
        match self {
            Self::FailedToParseYaml => "FAILED_TO_PARSE_YAML",
            Self::MalformedResourceGroup => "MALFORMED_RESOURCE_GROUP",
            Self::MalformedResourceInput => "MALFORMED_RESOURCE_INPUT",
            Self::DocumentVersionUnsupported => "DOCUMENT_VERSION_UNSUPPORTED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyResourceError {
    pub result_type: LegacyResourceResultType,
    pub message: String,
}

impl LegacyResourceError {
    fn new(result_type: LegacyResourceResultType, message: impl Into<String>) -> Self {
        Self {
            result_type,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for LegacyResourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.result_type.as_legacy_name(),
            self.message
        )
    }
}

impl std::error::Error for LegacyResourceError {}

impl ResourceCatalog {
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

impl BundleResourceCatalog {
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

impl PatchResourceCatalog {
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

pub fn resource_catalog_arrow_batch(catalog: &ResourceCatalog) -> Result<RecordBatch, String> {
    let mut metadata = HashMap::new();
    metadata.insert(
        String::from("carbon.schema"),
        String::from(RESOURCE_CATALOG_ARROW_SCHEMA),
    );
    metadata.insert(
        String::from("carbon.catalog.version"),
        catalog.version.clone(),
    );
    metadata.insert(
        String::from("carbon.catalog.type"),
        catalog.catalog_type.clone(),
    );
    metadata.insert(
        String::from("carbon.catalog.total_uncompressed_size_bytes"),
        catalog.total_uncompressed_size_bytes.to_string(),
    );
    if let Some(total) = catalog.total_compressed_size_bytes {
        metadata.insert(
            String::from("carbon.catalog.total_compressed_size_bytes"),
            total.to_string(),
        );
    }

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("catalog_version", DataType::Utf8, false),
            Field::new("catalog_type", DataType::Utf8, false),
            Field::new(
                "catalog_total_compressed_size_bytes",
                DataType::UInt64,
                true,
            ),
            Field::new(
                "catalog_total_uncompressed_size_bytes",
                DataType::UInt64,
                false,
            ),
            Field::new("path", DataType::Utf8, false),
            Field::new("location", DataType::Utf8, false),
            Field::new("size_bytes", DataType::UInt64, false),
            Field::new("compressed_size_bytes", DataType::UInt64, true),
            Field::new("checksum", DataType::Utf8, true),
            Field::new("binary_operation", DataType::UInt64, true),
            Field::new("prefix", DataType::Utf8, true),
        ],
        metadata,
    ));

    let catalog_version = StringArray::from_iter_values(
        (0..catalog.resources.len()).map(|_| catalog.version.as_str()),
    );
    let catalog_type = StringArray::from_iter_values(
        (0..catalog.resources.len()).map(|_| catalog.catalog_type.as_str()),
    );
    let catalog_total_compressed_values =
        vec![catalog.total_compressed_size_bytes; catalog.resources.len()];
    let catalog_total_compressed_size_bytes = UInt64Array::from(catalog_total_compressed_values);
    let catalog_total_uncompressed_size_bytes = UInt64Array::from_iter_values(
        (0..catalog.resources.len()).map(|_| catalog.total_uncompressed_size_bytes),
    );
    let path = StringArray::from_iter_values(catalog.resources.iter().map(|record| &record.path));
    let location =
        StringArray::from_iter_values(catalog.resources.iter().map(|record| &record.location));
    let size_bytes =
        UInt64Array::from_iter_values(catalog.resources.iter().map(|record| record.size_bytes));
    let compressed_size_bytes = UInt64Array::from(
        catalog
            .resources
            .iter()
            .map(|record| record.compressed_size_bytes)
            .collect::<Vec<_>>(),
    );
    let checksum = StringArray::from(
        catalog
            .resources
            .iter()
            .map(|record| record.checksum.as_deref())
            .collect::<Vec<_>>(),
    );
    let binary_operation = UInt64Array::from(
        catalog
            .resources
            .iter()
            .map(|record| record.binary_operation)
            .collect::<Vec<_>>(),
    );
    let prefix = StringArray::from(
        catalog
            .resources
            .iter()
            .map(|record| record.prefix.as_deref())
            .collect::<Vec<_>>(),
    );

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(catalog_version) as ArrayRef,
            Arc::new(catalog_type),
            Arc::new(catalog_total_compressed_size_bytes),
            Arc::new(catalog_total_uncompressed_size_bytes),
            Arc::new(path),
            Arc::new(location),
            Arc::new(size_bytes),
            Arc::new(compressed_size_bytes),
            Arc::new(checksum),
            Arc::new(binary_operation),
            Arc::new(prefix),
        ],
    )
    .map_err(|error| format!("building resource catalog Arrow batch: {error}"))
}

pub fn resource_catalog_from_arrow_batch(batch: &RecordBatch) -> Result<ResourceCatalog, String> {
    let schema = batch.schema();
    let metadata = schema.metadata();
    let has_metadata_schema =
        metadata.get("carbon.schema").map(String::as_str) == Some(RESOURCE_CATALOG_ARROW_SCHEMA);
    let has_column_schema = batch.column_by_name("catalog_version").is_some()
        && batch.column_by_name("catalog_type").is_some()
        && batch
            .column_by_name("catalog_total_uncompressed_size_bytes")
            .is_some();
    if !has_metadata_schema && !has_column_schema {
        return Err(String::from(
            "Arrow batch is not a carbon resource catalog schema",
        ));
    }
    let catalog_version = string_column(batch, "catalog_version").ok();
    let catalog_type = string_column(batch, "catalog_type").ok();
    let catalog_total_compressed_size_bytes =
        u64_column(batch, "catalog_total_compressed_size_bytes").ok();
    let catalog_total_uncompressed_size_bytes =
        u64_column(batch, "catalog_total_uncompressed_size_bytes").ok();
    let path = string_column(batch, "path")?;
    let location = string_column(batch, "location")?;
    let size_bytes = u64_column(batch, "size_bytes")?;
    let compressed_size_bytes = u64_column(batch, "compressed_size_bytes")?;
    let checksum = string_column(batch, "checksum")?;
    let binary_operation = u64_column(batch, "binary_operation")?;
    let prefix = string_column(batch, "prefix")?;

    let mut resources = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        resources.push(ResourceRecord {
            path: required_string(path, row, "path")?,
            location: required_string(location, row, "location")?,
            size_bytes: required_u64(size_bytes, row, "size_bytes")?,
            compressed_size_bytes: optional_u64(compressed_size_bytes, row),
            checksum: optional_string(checksum, row),
            binary_operation: optional_u64(binary_operation, row),
            prefix: optional_string(prefix, row),
        });
    }

    Ok(ResourceCatalog {
        version: metadata
            .get("carbon.catalog.version")
            .cloned()
            .or_else(|| first_optional_string(catalog_version))
            .unwrap_or_else(|| String::from("0.1.0")),
        catalog_type: metadata
            .get("carbon.catalog.type")
            .cloned()
            .or_else(|| first_optional_string(catalog_type))
            .unwrap_or_else(|| String::from("ResourceGroup")),
        total_compressed_size_bytes: metadata
            .get("carbon.catalog.total_compressed_size_bytes")
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| first_optional_u64(catalog_total_compressed_size_bytes)),
        total_uncompressed_size_bytes: metadata
            .get("carbon.catalog.total_uncompressed_size_bytes")
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| first_optional_u64(catalog_total_uncompressed_size_bytes))
            .unwrap_or_else(|| resources.iter().map(|record| record.size_bytes).sum()),
        resources,
    })
}

pub fn resource_catalog_to_arrow_ipc_bytes(catalog: &ResourceCatalog) -> Result<Vec<u8>, String> {
    let batch = resource_catalog_arrow_batch(catalog)?;
    let mut output = Vec::new();
    {
        let mut writer = ArrowFileWriter::try_new(&mut output, batch.schema().as_ref())
            .map_err(|error| format!("creating resource catalog Arrow IPC writer: {error}"))?;
        writer
            .write(&batch)
            .map_err(|error| format!("writing resource catalog Arrow IPC batch: {error}"))?;
        writer
            .finish()
            .map_err(|error| format!("finishing resource catalog Arrow IPC writer: {error}"))?;
    }
    Ok(output)
}

pub fn resource_catalog_from_arrow_ipc_bytes(bytes: &[u8]) -> Result<ResourceCatalog, String> {
    let reader = ArrowFileReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| format!("reading resource catalog Arrow IPC: {error}"))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|error| format!("reading Arrow IPC record batch: {error}"))?);
    }
    resource_catalog_from_batches(&batches)
}

pub fn resource_catalog_to_parquet_bytes(catalog: &ResourceCatalog) -> Result<Vec<u8>, String> {
    let batch = resource_catalog_arrow_batch(catalog)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut output = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties))
            .map_err(|error| format!("creating resource catalog Parquet writer: {error}"))?;
        writer
            .write(&batch)
            .map_err(|error| format!("writing resource catalog Parquet batch: {error}"))?;
        writer
            .close()
            .map_err(|error| format!("finishing resource catalog Parquet writer: {error}"))?;
    }
    Ok(output)
}

pub fn resource_catalog_from_parquet_bytes(bytes: &[u8]) -> Result<ResourceCatalog, String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .map_err(|error| format!("reading resource catalog Parquet metadata: {error}"))?
        .build()
        .map_err(|error| format!("building resource catalog Parquet reader: {error}"))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|error| format!("reading Parquet record batch: {error}"))?);
    }
    resource_catalog_from_batches(&batches)
}

fn resource_catalog_from_batches(batches: &[RecordBatch]) -> Result<ResourceCatalog, String> {
    let mut catalog: Option<ResourceCatalog> = None;
    for batch in batches {
        let next = resource_catalog_from_arrow_batch(batch)?;
        if let Some(catalog) = &mut catalog {
            catalog.resources.extend(next.resources);
            catalog.total_uncompressed_size_bytes = catalog
                .resources
                .iter()
                .map(|record| record.size_bytes)
                .sum();
            catalog.total_compressed_size_bytes = catalog
                .resources
                .iter()
                .map(|record| record.compressed_size_bytes)
                .try_fold(0_u64, |total, value| value.map(|value| total + value));
        } else {
            catalog = Some(next);
        }
    }
    catalog.ok_or_else(|| String::from("resource catalog contained no Arrow record batches"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, String> {
    batch
        .column_by_name(name)
        .ok_or_else(|| format!("missing Arrow resource catalog column {name}"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("Arrow resource catalog column {name} is not Utf8"))
}

fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array, String> {
    batch
        .column_by_name(name)
        .ok_or_else(|| format!("missing Arrow resource catalog column {name}"))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| format!("Arrow resource catalog column {name} is not UInt64"))
}

fn required_string(column: &StringArray, row: usize, name: &str) -> Result<String, String> {
    if column.is_null(row) {
        return Err(format!(
            "required Arrow resource catalog column {name} is null at row {row}"
        ));
    }
    Ok(column.value(row).to_string())
}

fn optional_string(column: &StringArray, row: usize) -> Option<String> {
    (!column.is_null(row)).then(|| column.value(row).to_string())
}

fn first_optional_string(column: Option<&StringArray>) -> Option<String> {
    column.and_then(|column| {
        (column.len() > 0)
            .then(|| optional_string(column, 0))
            .flatten()
    })
}

fn required_u64(column: &UInt64Array, row: usize, name: &str) -> Result<u64, String> {
    if column.is_null(row) {
        return Err(format!(
            "required Arrow resource catalog column {name} is null at row {row}"
        ));
    }
    Ok(column.value(row))
}

fn optional_u64(column: &UInt64Array, row: usize) -> Option<u64> {
    (!column.is_null(row)).then(|| column.value(row))
}

fn first_optional_u64(column: Option<&UInt64Array>) -> Option<u64> {
    column.and_then(|column| {
        (column.len() > 0)
            .then(|| optional_u64(column, 0))
            .flatten()
    })
}

pub fn parse_legacy_yaml_resource_group(input: &str) -> Result<ResourceCatalog, String> {
    parse_legacy_yaml_resource_group_compat(input).map_err(|error| error.to_string())
}

pub fn parse_legacy_yaml_resource_group_compat(
    input: &str,
) -> Result<ResourceCatalog, LegacyResourceError> {
    let value: serde_yaml::Value = serde_yaml::from_str(input).map_err(|error| {
        LegacyResourceError::new(
            LegacyResourceResultType::FailedToParseYaml,
            error.to_string(),
        )
    })?;

    let mapping = value.as_mapping().ok_or_else(|| {
        LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            "expected YAML resource group mapping",
        )
    })?;
    for key in [
        "Version",
        "Type",
        "NumberOfResources",
        "TotalResourcesSizeUnCompressed",
        "Resources",
    ] {
        if !yaml_mapping_contains_key(mapping, key) {
            return Err(LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceGroup,
                format!("missing required resource group parameter: {key}"),
            ));
        }
    }

    let yaml: LegacyYamlResourceGroup = serde_yaml::from_value(value).map_err(|error| {
        LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            error.to_string(),
        )
    })?;
    if yaml.catalog_type != "ResourceGroup" {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!("unexpected resource group type: {}", yaml.catalog_type),
        ));
    }
    let version = normalize_legacy_yaml_version(&yaml.version)?;
    let resources = yaml
        .resources
        .unwrap_or_default()
        .into_iter()
        .map(|resource| {
            let binary_operation = validate_legacy_binary_operation(resource.binary_operation)?;
            Ok(ResourceRecord {
                path: resource.relative_path,
                location: resource.location,
                size_bytes: resource.uncompressed_size,
                compressed_size_bytes: resource.compressed_size,
                checksum: Some(resource.checksum),
                binary_operation,
                prefix: resource.prefix,
            })
        })
        .collect::<Result<Vec<_>, LegacyResourceError>>()?;

    if resources.len() as u64 != yaml.number_of_resources {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!(
                "NumberOfResources expected {}, parsed {}",
                yaml.number_of_resources,
                resources.len()
            ),
        ));
    }

    Ok(ResourceCatalog {
        version,
        catalog_type: yaml.catalog_type,
        total_compressed_size_bytes: yaml.total_resources_size_compressed,
        total_uncompressed_size_bytes: yaml.total_resources_size_uncompressed,
        resources,
    })
}

pub fn parse_legacy_csv_resource_group(input: &str) -> Result<ResourceCatalog, String> {
    parse_legacy_csv_resource_group_compat(input).map_err(|error| error.to_string())
}

pub fn parse_legacy_csv_resource_group_compat(
    input: &str,
) -> Result<ResourceCatalog, LegacyResourceError> {
    let mut resources = Vec::new();
    for (index, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 5 && fields.len() != 6 {
            return Err(LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceInput,
                format!(
                    "line {} expected 5 or 6 CSV fields, got {}",
                    index + 1,
                    fields.len()
                ),
            ));
        }

        let (prefix, path) = split_legacy_resource_path(fields[0]);
        let size_bytes = fields[3].parse::<u64>().map_err(|error| {
            LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceInput,
                format!("line {} invalid uncompressed size: {error}", index + 1),
            )
        })?;
        let compressed_size = fields[4].parse::<u64>().map_err(|error| {
            LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceInput,
                format!("line {} invalid compressed size: {error}", index + 1),
            )
        })?;
        let binary_operation = if fields.len() == 6 {
            let parsed = fields[5].parse::<u64>().map_err(|error| {
                LegacyResourceError::new(
                    LegacyResourceResultType::MalformedResourceInput,
                    format!("line {} invalid binary operation: {error}", index + 1),
                )
            })?;
            validate_legacy_binary_operation(Some(parsed))?
        } else {
            None
        };

        resources.push(ResourceRecord {
            path,
            location: fields[1].to_string(),
            size_bytes,
            compressed_size_bytes: Some(compressed_size),
            checksum: Some(fields[2].to_string()),
            binary_operation,
            prefix,
        });
    }

    let total_uncompressed_size_bytes = resources
        .iter()
        .map(|resource| resource.size_bytes)
        .sum::<u64>();
    let total_compressed_size_bytes = resources
        .iter()
        .map(|resource| resource.compressed_size_bytes.unwrap_or_default())
        .sum::<u64>();

    Ok(ResourceCatalog {
        version: String::from("0.0.0"),
        catalog_type: String::from("ResourceGroup"),
        total_compressed_size_bytes: Some(total_compressed_size_bytes),
        total_uncompressed_size_bytes,
        resources,
    })
}

pub fn parse_legacy_yaml_bundle_resource_group(
    input: &str,
) -> Result<BundleResourceCatalog, String> {
    parse_legacy_yaml_bundle_resource_group_compat(input).map_err(|error| error.to_string())
}

pub fn parse_legacy_yaml_bundle_resource_group_compat(
    input: &str,
) -> Result<BundleResourceCatalog, LegacyResourceError> {
    let value: serde_yaml::Value = serde_yaml::from_str(input).map_err(|error| {
        LegacyResourceError::new(
            LegacyResourceResultType::FailedToParseYaml,
            error.to_string(),
        )
    })?;

    let mapping = value.as_mapping().ok_or_else(|| {
        LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            "expected YAML bundle resource group mapping",
        )
    })?;
    for key in [
        "Version",
        "Type",
        "NumberOfResources",
        "TotalResourcesSizeUnCompressed",
        "ResourceGroupResource",
        "ChunkSize",
        "Resources",
    ] {
        if !yaml_mapping_contains_key(mapping, key) {
            return Err(LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceGroup,
                format!("missing required bundle resource group parameter: {key}"),
            ));
        }
    }

    let yaml: LegacyYamlBundleResourceGroup = serde_yaml::from_value(value).map_err(|error| {
        LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            error.to_string(),
        )
    })?;
    if yaml.catalog_type != "BundleGroup" {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!(
                "unexpected bundle resource group type: {}",
                yaml.catalog_type
            ),
        ));
    }
    let version = normalize_legacy_yaml_version(&yaml.version)?;
    let resource_group_resource = bundle_record_from_legacy(yaml.resource_group_resource)?;
    if resource_group_resource.resource_type != "ResourceGroup" {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!(
                "expected ResourceGroupResource type ResourceGroup, got {}",
                resource_group_resource.resource_type
            ),
        ));
    }

    let resources = yaml
        .resources
        .unwrap_or_default()
        .into_iter()
        .map(bundle_record_from_legacy)
        .collect::<Result<Vec<_>, LegacyResourceError>>()?;
    for resource in &resources {
        if resource.resource_type != "BinaryChunk" {
            return Err(LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceGroup,
                format!(
                    "expected bundle chunk type BinaryChunk, got {}",
                    resource.resource_type
                ),
            ));
        }
    }

    if resources.len() as u64 != yaml.number_of_resources {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!(
                "NumberOfResources expected {}, parsed {}",
                yaml.number_of_resources,
                resources.len()
            ),
        ));
    }

    Ok(BundleResourceCatalog {
        version,
        catalog_type: yaml.catalog_type,
        total_compressed_size_bytes: yaml.total_resources_size_compressed,
        total_uncompressed_size_bytes: yaml.total_resources_size_uncompressed,
        resource_group_resource,
        chunk_size: yaml.chunk_size,
        resources,
    })
}

pub fn parse_legacy_yaml_patch_resource_group(input: &str) -> Result<PatchResourceCatalog, String> {
    parse_legacy_yaml_patch_resource_group_compat(input).map_err(|error| error.to_string())
}

pub fn parse_legacy_yaml_patch_resource_group_compat(
    input: &str,
) -> Result<PatchResourceCatalog, LegacyResourceError> {
    let value: serde_yaml::Value = serde_yaml::from_str(input).map_err(|error| {
        LegacyResourceError::new(
            LegacyResourceResultType::FailedToParseYaml,
            error.to_string(),
        )
    })?;

    let mapping = value.as_mapping().ok_or_else(|| {
        LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            "expected YAML patch resource group mapping",
        )
    })?;
    for key in [
        "Version",
        "Type",
        "NumberOfResources",
        "TotalResourcesSizeUnCompressed",
        "ResourceGroupResource",
        "MaxInputChunkSize",
        "Resources",
    ] {
        if !yaml_mapping_contains_key(mapping, key) {
            return Err(LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceGroup,
                format!("missing required patch resource group parameter: {key}"),
            ));
        }
    }

    let yaml: LegacyYamlPatchResourceGroup = serde_yaml::from_value(value).map_err(|error| {
        LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            error.to_string(),
        )
    })?;
    if yaml.catalog_type != "PatchGroup" {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!(
                "unexpected patch resource group type: {}",
                yaml.catalog_type
            ),
        ));
    }
    let version = normalize_legacy_yaml_version(&yaml.version)?;
    let resource_group_resource = patch_group_record_from_legacy(yaml.resource_group_resource);
    if resource_group_resource.resource_type != "ResourceGroup" {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!(
                "expected ResourceGroupResource type ResourceGroup, got {}",
                resource_group_resource.resource_type
            ),
        ));
    }

    let resources = yaml
        .resources
        .unwrap_or_default()
        .into_iter()
        .map(patch_record_from_legacy)
        .collect::<Vec<_>>();
    for resource in &resources {
        if resource.resource_type != "BinaryPatch" {
            return Err(LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceGroup,
                format!(
                    "expected patch resource type BinaryPatch, got {}",
                    resource.resource_type
                ),
            ));
        }
    }

    if resources.len() as u64 != yaml.number_of_resources {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!(
                "NumberOfResources expected {}, parsed {}",
                yaml.number_of_resources,
                resources.len()
            ),
        ));
    }

    Ok(PatchResourceCatalog {
        version,
        catalog_type: yaml.catalog_type,
        total_compressed_size_bytes: yaml.total_resources_size_compressed,
        total_uncompressed_size_bytes: yaml.total_resources_size_uncompressed,
        resource_group_resource,
        max_input_chunk_size: yaml.max_input_chunk_size,
        removed_resource_relative_paths: yaml.removed_resource_relative_paths,
        resources,
    })
}

pub fn export_legacy_yaml_resource_group(catalog: &ResourceCatalog) -> String {
    let mut output = String::new();
    output.push_str(&format!("Version: {}\n", catalog.version));
    output.push_str(&format!("Type: {}\n", catalog.catalog_type));
    output.push_str(&format!("NumberOfResources: {}\n", catalog.resources.len()));
    if let Some(total_compressed) = catalog.total_compressed_size_bytes {
        output.push_str(&format!(
            "TotalResourcesSizeCompressed: {total_compressed}\n"
        ));
    }
    output.push_str(&format!(
        "TotalResourcesSizeUnCompressed: {}\n",
        catalog.total_uncompressed_size_bytes
    ));

    if catalog.resources.is_empty() {
        output.push_str("Resources: []");
        return output;
    }

    output.push_str("Resources:\n");
    for resource in &catalog.resources {
        output.push_str(&format!("  - RelativePath: {}\n", resource.path));
        output.push_str("    Type: Resource\n");
        output.push_str(&format!("    Location: {}\n", resource.location));
        output.push_str(&format!(
            "    Checksum: {}\n",
            resource.checksum.as_deref().unwrap_or_default()
        ));
        output.push_str(&format!("    UncompressedSize: {}\n", resource.size_bytes));
        if let Some(compressed_size) = resource.compressed_size_bytes {
            output.push_str(&format!("    CompressedSize: {compressed_size}\n"));
        }
        if let Some(binary_operation) = resource.binary_operation {
            output.push_str(&format!("    BinaryOperation: {binary_operation}\n"));
        }
        if let Some(prefix) = &resource.prefix {
            output.push_str(&format!("    Prefix: {prefix}\n"));
        }
    }

    if output.ends_with('\n') {
        output.pop();
    }
    output
}

pub fn export_legacy_yaml_bundle_resource_group(catalog: &BundleResourceCatalog) -> String {
    let mut output = String::new();
    output.push_str(&format!("Version: {}\n", catalog.version));
    output.push_str(&format!("Type: {}\n", catalog.catalog_type));
    output.push_str(&format!("NumberOfResources: {}\n", catalog.resources.len()));
    if let Some(total_compressed) = catalog.total_compressed_size_bytes {
        output.push_str(&format!(
            "TotalResourcesSizeCompressed: {total_compressed}\n"
        ));
    }
    output.push_str(&format!(
        "TotalResourcesSizeUnCompressed: {}\n",
        catalog.total_uncompressed_size_bytes
    ));
    output.push_str("ResourceGroupResource:\n");
    push_legacy_bundle_record_yaml(&mut output, &catalog.resource_group_resource, "  ");
    output.push_str(&format!("ChunkSize: {}\n", catalog.chunk_size));

    if catalog.resources.is_empty() {
        output.push_str("Resources: []");
        return output;
    }

    output.push_str("Resources:\n");
    for resource in &catalog.resources {
        output.push_str(&format!("  - RelativePath: {}\n", resource.path));
        output.push_str(&format!("    Type: {}\n", resource.resource_type));
        output.push_str(&format!("    Location: {}\n", resource.location));
        output.push_str(&format!("    Checksum: {}\n", resource.checksum));
        output.push_str(&format!("    UncompressedSize: {}\n", resource.size_bytes));
        if let Some(compressed_size) = resource.compressed_size_bytes {
            output.push_str(&format!("    CompressedSize: {compressed_size}\n"));
        }
    }

    if output.ends_with('\n') {
        output.pop();
    }
    output
}

pub fn export_legacy_yaml_patch_resource_group(catalog: &PatchResourceCatalog) -> String {
    let mut output = String::new();
    output.push_str(&format!("Version: {}\n", catalog.version));
    output.push_str(&format!("Type: {}\n", catalog.catalog_type));
    output.push_str(&format!("NumberOfResources: {}\n", catalog.resources.len()));
    if let Some(total_compressed) = catalog.total_compressed_size_bytes {
        output.push_str(&format!(
            "TotalResourcesSizeCompressed: {total_compressed}\n"
        ));
    }
    output.push_str(&format!(
        "TotalResourcesSizeUnCompressed: {}\n",
        catalog.total_uncompressed_size_bytes
    ));
    output.push_str("ResourceGroupResource:\n");
    push_legacy_patch_group_record_yaml(&mut output, &catalog.resource_group_resource, "  ");
    output.push_str(&format!(
        "MaxInputChunkSize: {}\n",
        catalog.max_input_chunk_size
    ));
    if let Some(removed_paths) = &catalog.removed_resource_relative_paths {
        if removed_paths.is_empty() {
            output.push_str("RemovedResourceRelativePaths: []\n");
        } else {
            output.push_str("RemovedResourceRelativePaths:\n");
            for path in removed_paths {
                output.push_str(&format!("  - {path}\n"));
            }
        }
    }

    if catalog.resources.is_empty() {
        output.push_str("Resources: []");
        return output;
    }

    output.push_str("Resources:\n");
    for resource in &catalog.resources {
        output.push_str(&format!("  - RelativePath: {}\n", resource.path));
        output.push_str(&format!("    Type: {}\n", resource.resource_type));
        push_legacy_yaml_scalar_line(&mut output, "    ", "Location", &resource.location);
        output.push_str(&format!("    Checksum: {}\n", resource.checksum));
        output.push_str(&format!("    UncompressedSize: {}\n", resource.size_bytes));
        if let Some(compressed_size) = resource.compressed_size_bytes {
            output.push_str(&format!("    CompressedSize: {compressed_size}\n"));
        }
        output.push_str(&format!(
            "    TargetResourceRelativePath: {}\n",
            resource.target_resource_relative_path
        ));
        output.push_str(&format!("    DataOffset: {}\n", resource.data_offset));
        output.push_str(&format!("    SourceOffset: {}\n", resource.source_offset));
    }

    if output.ends_with('\n') {
        output.pop();
    }
    output
}

pub fn export_legacy_csv_resource_group(catalog: &ResourceCatalog) -> String {
    let mut output = String::new();
    let mut resources = catalog.resources.iter().collect::<Vec<_>>();
    resources.sort_by(|left, right| legacy_resource_path(left).cmp(&legacy_resource_path(right)));
    for resource in resources {
        output.push_str(&legacy_resource_path(resource));
        output.push(',');
        output.push_str(&resource.location);
        output.push(',');
        output.push_str(resource.checksum.as_deref().unwrap_or_default());
        output.push(',');
        output.push_str(&resource.size_bytes.to_string());
        output.push(',');
        output.push_str(
            &resource
                .compressed_size_bytes
                .unwrap_or_default()
                .to_string(),
        );
        if let Some(binary_operation) = resource.binary_operation {
            output.push(',');
            output.push_str(&binary_operation.to_string());
        }
        output.push('\n');
    }
    output
}

pub fn create_legacy_local_bundle_from_resource_group(
    catalog: &ResourceCatalog,
    resource_source_directory: impl AsRef<Path>,
    resource_group_relative_path: &str,
    chunk_destination_relative_path: &str,
    chunk_size: u64,
    file_read_chunk_size: u64,
) -> Result<CreatedLegacyLocalBundle, String> {
    create_legacy_bundle_from_resource_group(
        catalog,
        resource_source_directory,
        resource_group_relative_path,
        chunk_destination_relative_path,
        chunk_size,
        file_read_chunk_size,
        LegacyBundleChunkDestination::LocalCdn,
    )
}

pub fn create_legacy_remote_cdn_bundle_from_resource_group(
    catalog: &ResourceCatalog,
    resource_source_directory: impl AsRef<Path>,
    resource_group_relative_path: &str,
    chunk_destination_relative_path: &str,
    chunk_size: u64,
    file_read_chunk_size: u64,
) -> Result<CreatedLegacyLocalBundle, String> {
    create_legacy_bundle_from_resource_group(
        catalog,
        resource_source_directory,
        resource_group_relative_path,
        chunk_destination_relative_path,
        chunk_size,
        file_read_chunk_size,
        LegacyBundleChunkDestination::RemoteCdn,
    )
}

fn create_legacy_bundle_from_resource_group(
    catalog: &ResourceCatalog,
    resource_source_directory: impl AsRef<Path>,
    resource_group_relative_path: &str,
    chunk_destination_relative_path: &str,
    chunk_size: u64,
    file_read_chunk_size: u64,
    chunk_destination: LegacyBundleChunkDestination,
) -> Result<CreatedLegacyLocalBundle, String> {
    if chunk_size == 0 {
        return Err(String::from("invalid chunk size: 0"));
    }
    if file_read_chunk_size == 0 {
        return Err(String::from("invalid file read chunk size: 0"));
    }

    let resource_source_directory = resource_source_directory.as_ref();
    if !resource_source_directory.exists() {
        return Err(format!(
            "resource source directory does not exist: {}",
            resource_source_directory.display()
        ));
    }

    let resource_catalog = legacy_resource_catalog_for_bundle_export(catalog)?;
    let resource_group_data = export_legacy_yaml_resource_group(&resource_catalog).into_bytes();
    let resource_group_resource = match chunk_destination {
        LegacyBundleChunkDestination::LocalCdn => LegacyBundleDataResource {
            record: bundle_data_record(
                resource_group_relative_path,
                "ResourceGroup",
                &resource_group_data,
            )?,
            data: resource_group_data,
        },
        LegacyBundleChunkDestination::RemoteCdn => legacy_compressed_bundle_data_resource(
            resource_group_relative_path,
            "ResourceGroup",
            &resource_group_data,
        )?,
    };

    let mut chunks = Vec::new();
    let mut chunk_data = Vec::<u8>::new();
    let chunk_size = chunk_size as usize;
    let file_read_chunk_size = file_read_chunk_size as usize;
    let chunk_base_name = Path::new(resource_group_relative_path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("ResourceGroup");

    for resource in &resource_catalog.resources {
        if resource.location.is_empty() {
            continue;
        }

        let data = read_legacy_local_relative_data(resource_source_directory, &resource.path)?;
        if let Some(expected_checksum) = &resource.checksum {
            let checksum = md5_hex(&data);
            if &checksum != expected_checksum {
                return Err(format!(
                    "resource checksum mismatch for {}: expected {}, got {}",
                    resource.path, expected_checksum, checksum
                ));
            }
        }

        match chunk_destination {
            LegacyBundleChunkDestination::LocalCdn => {
                for block in data.chunks(file_read_chunk_size) {
                    chunk_data.extend_from_slice(block);
                    if chunk_data.len() >= chunk_size {
                        push_legacy_local_bundle_chunk(
                            &mut chunks,
                            chunk_destination_relative_path,
                            chunk_base_name,
                            std::mem::take(&mut chunk_data),
                        )?;
                    }
                }
            }
            LegacyBundleChunkDestination::RemoteCdn => chunk_data.extend_from_slice(&data),
        }
    }

    if !chunk_data.is_empty() {
        match chunk_destination {
            LegacyBundleChunkDestination::LocalCdn => {
                push_legacy_local_bundle_chunk(
                    &mut chunks,
                    chunk_destination_relative_path,
                    chunk_base_name,
                    chunk_data,
                )?;
            }
            LegacyBundleChunkDestination::RemoteCdn => {
                push_legacy_remote_cdn_bundle_chunk(
                    &mut chunks,
                    chunk_destination_relative_path,
                    chunk_base_name,
                    chunk_data,
                )?;
            }
        }
    }

    let total_uncompressed_size_bytes = chunks
        .iter()
        .map(|chunk| chunk.record.size_bytes)
        .sum::<u64>();
    let total_compressed_size_bytes = chunks
        .iter()
        .map(|chunk| chunk.record.compressed_size_bytes.unwrap_or_default())
        .sum::<u64>();

    let catalog = BundleResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("BundleGroup"),
        total_compressed_size_bytes: Some(total_compressed_size_bytes),
        total_uncompressed_size_bytes,
        resource_group_resource: resource_group_resource.record.clone(),
        chunk_size: chunk_size as u64,
        resources: chunks.iter().map(|chunk| chunk.record.clone()).collect(),
    };

    Ok(CreatedLegacyLocalBundle {
        catalog,
        resource_group_resource,
        chunks,
    })
}

pub fn unpack_legacy_local_bundle_from_cdn(
    catalog: &BundleResourceCatalog,
    chunk_source_directory: impl AsRef<Path>,
) -> Result<UnpackedLegacyLocalBundle, String> {
    let chunk_source_directory = chunk_source_directory.as_ref();
    if !chunk_source_directory.exists() {
        return Err(format!(
            "chunk source directory does not exist: {}",
            chunk_source_directory.display()
        ));
    }

    let resource_group_data =
        read_legacy_local_cdn_data(chunk_source_directory, &catalog.resource_group_resource)?;
    let mut bundled_data = Vec::new();
    for chunk in &catalog.resources {
        bundled_data.extend(read_legacy_local_cdn_data(chunk_source_directory, chunk)?);
    }

    unpack_legacy_bundle_data(catalog, resource_group_data, bundled_data)
}

pub fn unpack_legacy_remote_bundle_from_local_mirror(
    catalog: &BundleResourceCatalog,
    remote_mirror_directory: impl AsRef<Path>,
    cache_directory: impl AsRef<Path>,
) -> Result<UnpackedLegacyRemoteBundle, String> {
    let remote_mirror_directory = remote_mirror_directory.as_ref();
    if !remote_mirror_directory.exists() {
        return Err(format!(
            "remote mirror directory does not exist: {}",
            remote_mirror_directory.display()
        ));
    }
    let cache_directory = cache_directory.as_ref();
    fs::create_dir_all(cache_directory).map_err(|error| {
        format!(
            "failed to create remote cache directory {}: {error}",
            cache_directory.display()
        )
    })?;

    let mut cache_stats = LegacyRemoteCdnCacheStats::default();
    let resource_group_data = read_legacy_remote_cdn_data_from_local_mirror(
        remote_mirror_directory,
        cache_directory,
        &catalog.resource_group_resource,
        &mut cache_stats,
    )?;

    let mut bundled_data = Vec::new();
    for chunk in &catalog.resources {
        bundled_data.extend(read_legacy_remote_cdn_data_from_local_mirror(
            remote_mirror_directory,
            cache_directory,
            chunk,
            &mut cache_stats,
        )?);
    }

    Ok(UnpackedLegacyRemoteBundle {
        unpacked: unpack_legacy_bundle_data(catalog, resource_group_data, bundled_data)?,
        cache_stats,
    })
}

fn unpack_legacy_bundle_data(
    catalog: &BundleResourceCatalog,
    resource_group_data: Vec<u8>,
    bundled_data: Vec<u8>,
) -> Result<UnpackedLegacyLocalBundle, String> {
    let mut position = 0_usize;
    let mut resources = Vec::new();
    let resource_catalog = parse_legacy_yaml_resource_group(
        std::str::from_utf8(&resource_group_data)
            .map_err(|error| format!("resource group data is not valid UTF-8: {error}"))?,
    )?;
    let resource_group_resource = LegacyBundleDataResource {
        record: catalog.resource_group_resource.clone(),
        data: resource_group_data,
    };
    for resource in &resource_catalog.resources {
        if resource.location.is_empty() {
            continue;
        }

        let end = position
            .checked_add(resource.size_bytes as usize)
            .ok_or_else(|| format!("resource size overflow for {}", resource.path))?;
        if end > bundled_data.len() {
            return Err(format!(
                "bundle ended while reading {}: needed {} bytes, had {}",
                resource.path,
                resource.size_bytes,
                bundled_data.len().saturating_sub(position)
            ));
        }

        let data = bundled_data[position..end].to_vec();
        position = end;
        if let Some(expected_checksum) = &resource.checksum {
            let checksum = md5_hex(&data);
            if &checksum != expected_checksum {
                return Err(format!(
                    "unpacked checksum mismatch for {}: expected {}, got {}",
                    resource.path, expected_checksum, checksum
                ));
            }
        }

        resources.push(LegacyUnpackedBundleResource {
            path: resource.path.clone(),
            data,
        });
    }

    Ok(UnpackedLegacyLocalBundle {
        resource_group_resource,
        resource_catalog,
        resources,
    })
}

pub fn read_legacy_local_patch_data(
    catalog: &PatchResourceCatalog,
    patch_source_directory: impl AsRef<Path>,
) -> Result<LegacyPatchDataSet, String> {
    let patch_source_directory = patch_source_directory.as_ref();
    if !patch_source_directory.exists() {
        return Err(format!(
            "patch source directory does not exist: {}",
            patch_source_directory.display()
        ));
    }

    let resource_group_resource =
        read_legacy_patch_group_data(patch_source_directory, &catalog.resource_group_resource)?;
    let mut resources = Vec::new();
    for resource in &catalog.resources {
        if resource.location.is_empty() {
            continue;
        }
        resources.push(read_legacy_patch_resource_data(
            patch_source_directory,
            resource,
        )?);
    }

    Ok(LegacyPatchDataSet {
        resource_group_resource,
        resources,
    })
}

pub fn apply_legacy_binary_patch(previous: &[u8], patch_data: &[u8]) -> Result<Vec<u8>, String> {
    if patch_data.len() < LEGACY_BSDIFF_HEADER_SIZE {
        return Err(format!(
            "legacy patch data is too short: expected at least {LEGACY_BSDIFF_HEADER_SIZE} bytes, got {}",
            patch_data.len()
        ));
    }
    if &patch_data[..LEGACY_BSDIFF_HEADER.len()] != LEGACY_BSDIFF_HEADER {
        return Err(String::from("legacy patch data has an unexpected header"));
    }

    let target_len = usize::try_from(u64::from_le_bytes(
        patch_data[LEGACY_BSDIFF_HEADER.len()..LEGACY_BSDIFF_HEADER_SIZE]
            .try_into()
            .expect("slice has fixed length"),
    ))
    .map_err(|_| String::from("legacy patch target length does not fit in usize"))?;
    let mut patched = Vec::with_capacity(target_len);
    let mut stream = &patch_data[LEGACY_BSDIFF_HEADER_SIZE..];
    bsdiff::patch(previous, &mut stream, &mut patched)
        .map_err(|error| format!("failed to apply legacy bsdiff patch: {error}"))?;
    if patched.len() != target_len {
        return Err(format!(
            "legacy patch target length mismatch: expected {target_len}, got {}",
            patched.len()
        ));
    }

    Ok(patched)
}

pub fn create_legacy_binary_patch(previous: &[u8], latest: &[u8]) -> Result<Vec<u8>, String> {
    let target_len = u64::try_from(latest.len())
        .map_err(|_| String::from("legacy patch target length does not fit in u64"))?;
    let mut patch_data = Vec::with_capacity(LEGACY_BSDIFF_HEADER_SIZE);
    patch_data.extend_from_slice(LEGACY_BSDIFF_HEADER);
    patch_data.extend_from_slice(&target_len.to_le_bytes());
    bsdiff::diff(previous, latest, &mut patch_data)
        .map_err(|error| format!("failed to create legacy bsdiff patch: {error}"))?;
    Ok(patch_data)
}

pub fn create_legacy_local_patch_from_resource_groups(
    previous_catalog: &ResourceCatalog,
    next_catalog: &ResourceCatalog,
    previous_resource_directory: impl AsRef<Path>,
    next_resource_directory: impl AsRef<Path>,
    max_input_chunk_size: u64,
) -> Result<CreatedLegacyLocalPatch, String> {
    create_legacy_local_patch_from_resource_groups_with_options(
        previous_catalog,
        next_catalog,
        previous_resource_directory,
        next_resource_directory,
        max_input_chunk_size,
        "ResourceGroup.yaml",
        "Patches/Patch",
    )
}

pub fn create_legacy_local_patch_from_resource_groups_with_options(
    previous_catalog: &ResourceCatalog,
    next_catalog: &ResourceCatalog,
    previous_resource_directory: impl AsRef<Path>,
    next_resource_directory: impl AsRef<Path>,
    max_input_chunk_size: u64,
    resource_group_relative_path: &str,
    patch_file_relative_path_prefix: &str,
) -> Result<CreatedLegacyLocalPatch, String> {
    if max_input_chunk_size == 0 {
        return Err(String::from("invalid max input chunk size: 0"));
    }
    if max_input_chunk_size > usize::MAX as u64 {
        return Err(String::from("max input chunk size does not fit in usize"));
    }
    let previous_resource_directory = previous_resource_directory.as_ref();
    if !previous_resource_directory.exists() {
        return Err(format!(
            "previous resource directory does not exist: {}",
            previous_resource_directory.display()
        ));
    }
    let next_resource_directory = next_resource_directory.as_ref();
    if !next_resource_directory.exists() {
        return Err(format!(
            "next resource directory does not exist: {}",
            next_resource_directory.display()
        ));
    }

    let diff = diff_legacy_resource_catalogs(previous_catalog, next_catalog);
    let previous_by_path = previous_catalog
        .resources
        .iter()
        .map(|resource| (resource.path.clone(), resource))
        .collect::<BTreeMap<_, _>>();
    let next_by_path = next_catalog
        .resources
        .iter()
        .map(|resource| (resource.path.clone(), resource))
        .collect::<BTreeMap<_, _>>();

    let mut diff_resources = Vec::new();
    for path in &diff.additions {
        let resource = next_by_path
            .get(path)
            .ok_or_else(|| format!("target resource missing from next catalog: {path}"))?;
        diff_resources.push((*resource).clone());
    }
    let resource_group_catalog = ResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("ResourceGroup"),
        total_compressed_size_bytes: total_compressed_size(&diff_resources),
        total_uncompressed_size_bytes: diff_resources
            .iter()
            .map(|resource| resource.size_bytes)
            .sum::<u64>(),
        resources: diff_resources,
    };
    let resource_group_data =
        export_legacy_yaml_resource_group(&resource_group_catalog).into_bytes();
    let resource_group_checksum = md5_hex(&resource_group_data);
    let resource_group_path = format!("DiffResourceGroup_{resource_group_checksum}.yaml");
    let resource_group_resource = LegacyPatchDataResource {
        path: resource_group_path,
        resource_type: String::from("ResourceGroup"),
        location: legacy_location_for_path(resource_group_relative_path, &resource_group_checksum),
        checksum: resource_group_checksum,
        data: resource_group_data,
    };

    let mut resources = Vec::new();
    let mut patch_records = Vec::new();
    let max_input_chunk_size = usize::try_from(max_input_chunk_size)
        .map_err(|_| String::from("max input chunk size does not fit in usize"))?;
    let mut patch_id = 0_usize;
    for path in &diff.additions {
        let Some(previous_resource) = previous_by_path.get(path) else {
            continue;
        };
        let next_resource = next_by_path
            .get(path)
            .ok_or_else(|| format!("target resource missing from next catalog: {path}"))?;
        if previous_resource.checksum == next_resource.checksum {
            continue;
        }

        let previous =
            read_legacy_local_relative_data(previous_resource_directory, &previous_resource.path)?;
        let latest = read_legacy_local_relative_data(next_resource_directory, &next_resource.path)?;
        if let Some(expected_checksum) = &previous_resource.checksum {
            let checksum = md5_hex(&previous);
            if &checksum != expected_checksum {
                return Err(format!(
                    "previous resource checksum mismatch for {}: expected {}, got {}",
                    previous_resource.path, expected_checksum, checksum
                ));
            }
        }
        if let Some(expected_checksum) = &next_resource.checksum {
            let checksum = md5_hex(&latest);
            if &checksum != expected_checksum {
                return Err(format!(
                    "next resource checksum mismatch for {}: expected {}, got {}",
                    next_resource.path, expected_checksum, checksum
                ));
            }
        }

        let mut data_offset = 0_usize;
        let mut source_offset = 0_usize;
        let mut previous_stream_position = 0_usize;
        while data_offset < latest.len() {
            if previous_stream_position >= previous.len() && previous.len() > data_offset {
                previous_stream_position = 0;
            }
            let previous_chunk_start = previous_stream_position;
            let previous_chunk_end = previous_chunk_start
                .checked_add(max_input_chunk_size)
                .map(|end| end.min(previous.len()))
                .ok_or_else(|| format!("source offset overflow for {path}"))?;
            let previous_chunk = if previous_chunk_start < previous.len() {
                previous_stream_position = previous_chunk_end;
                &previous[previous_chunk_start..previous_chunk_end]
            } else {
                &[]
            };

            let next_end = data_offset
                .checked_add(max_input_chunk_size)
                .map(|end| end.min(latest.len()))
                .ok_or_else(|| format!("data offset overflow for {path}"))?;
            let next_chunk = &latest[data_offset..next_end];
            let patch_path = format!("{patch_file_relative_path_prefix}.{patch_id}");

            if !previous_chunk.is_empty() {
                if let Some(match_offset) =
                    legacy_find_matching_chunk(&previous, next_chunk, max_input_chunk_size)
                {
                    let match_count = 1_usize
                        .checked_add(legacy_count_matching_chunks(
                            &latest,
                            next_end,
                            &previous,
                            match_offset
                                .checked_add(max_input_chunk_size)
                                .ok_or_else(|| format!("source offset overflow for {path}"))?,
                            max_input_chunk_size,
                        ))
                        .ok_or_else(|| format!("match count overflow for {path}"))?;
                    let match_size = max_input_chunk_size
                        .checked_mul(match_count)
                        .map(|size| size.min(previous.len() - match_offset))
                        .ok_or_else(|| format!("match size overflow for {path}"))?;
                    if match_size == 0 {
                        return Err(format!("zero-length chunk match for {path}"));
                    }
                    let checksum = md5_hex(&[]);
                    patch_records.push(PatchResourceRecord {
                        path: patch_path,
                        resource_type: String::from("BinaryPatch"),
                        location: String::new(),
                        size_bytes: match_size as u64,
                        compressed_size_bytes: None,
                        checksum,
                        target_resource_relative_path: next_resource.path.clone(),
                        data_offset: data_offset as u64,
                        source_offset: match_offset as u64,
                    });
                    source_offset = match_offset
                        .checked_add(match_size)
                        .ok_or_else(|| format!("source offset overflow for {path}"))?
                        .min(previous.len());
                    previous_stream_position = source_offset;
                    data_offset = data_offset
                        .checked_add(match_size)
                        .map(|offset| offset.min(latest.len()))
                        .ok_or_else(|| format!("data offset overflow for {path}"))?;
                    patch_id += 1;
                    continue;
                }
            }

            let patch_data = create_legacy_binary_patch(previous_chunk, next_chunk)?;
            let checksum = md5_hex(&patch_data);
            let location = legacy_location_for_path(&patch_path, &checksum);
            let compressed_size = gzip_compress(&patch_data)
                .map_err(|error| error.to_string())?
                .len() as u64;
            resources.push(LegacyPatchDataResource {
                path: patch_path.clone(),
                resource_type: String::from("BinaryPatch"),
                location: location.clone(),
                checksum: checksum.clone(),
                data: patch_data.clone(),
            });
            patch_records.push(PatchResourceRecord {
                path: patch_path,
                resource_type: String::from("BinaryPatch"),
                location,
                size_bytes: patch_data.len() as u64,
                compressed_size_bytes: Some(compressed_size),
                checksum,
                target_resource_relative_path: next_resource.path.clone(),
                data_offset: data_offset as u64,
                source_offset: source_offset as u64,
            });
            let source_offset_delta = if previous_chunk.is_empty() {
                next_chunk.len()
            } else {
                previous_chunk.len()
            };
            source_offset = source_offset
                .checked_add(source_offset_delta)
                .ok_or_else(|| format!("source offset overflow for {path}"))?;
            data_offset = next_end;
            patch_id += 1;
        }
    }

    let patch_records = patch_records;
    let max_input_chunk_size = max_input_chunk_size as u64;

    let catalog = PatchResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("PatchGroup"),
        total_compressed_size_bytes: Some(
            patch_records
                .iter()
                .map(|resource| resource.compressed_size_bytes.unwrap_or_default())
                .sum::<u64>(),
        ),
        total_uncompressed_size_bytes: patch_records
            .iter()
            .map(|resource| resource.size_bytes)
            .sum::<u64>(),
        resource_group_resource: PatchResourceGroupRecord {
            path: resource_group_resource.path.clone(),
            resource_type: resource_group_resource.resource_type.clone(),
            location: resource_group_resource.location.clone(),
            size_bytes: resource_group_resource.data.len() as u64,
            compressed_size_bytes: Some(
                gzip_compress(&resource_group_resource.data)
                    .map_err(|error| error.to_string())?
                    .len() as u64,
            ),
            checksum: resource_group_resource.checksum.clone(),
        },
        max_input_chunk_size,
        removed_resource_relative_paths: if diff.subtractions.is_empty() {
            None
        } else {
            Some(diff.subtractions)
        },
        resources: patch_records,
    };

    Ok(CreatedLegacyLocalPatch {
        catalog,
        resource_group_resource,
        resources,
    })
}

fn legacy_find_matching_chunk(
    previous: &[u8],
    next_chunk: &[u8],
    max_input_chunk_size: usize,
) -> Option<usize> {
    if next_chunk.len() != max_input_chunk_size || previous.len() < max_input_chunk_size {
        return None;
    }
    previous
        .windows(max_input_chunk_size)
        .position(|candidate| candidate == next_chunk)
}

fn legacy_count_matching_chunks(
    latest: &[u8],
    mut latest_offset: usize,
    previous: &[u8],
    mut previous_offset: usize,
    max_input_chunk_size: usize,
) -> usize {
    let mut result = 0_usize;
    loop {
        if latest_offset >= latest.len() || previous_offset >= previous.len() {
            return result;
        }
        let latest_end = latest_offset
            .saturating_add(max_input_chunk_size)
            .min(latest.len());
        let previous_end = previous_offset
            .saturating_add(max_input_chunk_size)
            .min(previous.len());
        if latest[latest_offset..latest_end] != previous[previous_offset..previous_end] {
            return result;
        }
        result += 1;
        latest_offset = latest_end;
        previous_offset = previous_end;
    }
}

pub fn apply_legacy_local_patch_from_directories(
    catalog: &PatchResourceCatalog,
    previous_resource_directory: impl AsRef<Path>,
    next_resource_directory: impl AsRef<Path>,
    patch_source_directory: impl AsRef<Path>,
) -> Result<AppliedLegacyPatchSet, String> {
    let previous_resource_directory = previous_resource_directory.as_ref();
    if !previous_resource_directory.exists() {
        return Err(format!(
            "previous resource directory does not exist: {}",
            previous_resource_directory.display()
        ));
    }
    let next_resource_directory = next_resource_directory.as_ref();
    if !next_resource_directory.exists() {
        return Err(format!(
            "next resource directory does not exist: {}",
            next_resource_directory.display()
        ));
    }

    let patch_data_set = read_legacy_local_patch_data(catalog, patch_source_directory)?;
    let resource_catalog = parse_legacy_yaml_resource_group(
        std::str::from_utf8(&patch_data_set.resource_group_resource.data)
            .map_err(|error| format!("resource group data is not valid UTF-8: {error}"))?,
    )?;

    let mut patch_data_by_path = BTreeMap::<String, Vec<u8>>::new();
    for resource in &patch_data_set.resources {
        patch_data_by_path.insert(resource.path.clone(), resource.data.clone());
    }

    let mut patches_by_target = BTreeMap::<String, Vec<&PatchResourceRecord>>::new();
    for patch in &catalog.resources {
        patches_by_target
            .entry(patch.target_resource_relative_path.to_ascii_lowercase())
            .or_default()
            .push(patch);
    }
    for patches in patches_by_target.values_mut() {
        patches.sort_by(|left, right| {
            left.data_offset
                .cmp(&right.data_offset)
                .then_with(|| left.source_offset.cmp(&right.source_offset))
                .then_with(|| left.path.cmp(&right.path))
        });
    }

    let mut resources = Vec::new();
    for resource in &resource_catalog.resources {
        if resource.location.is_empty() {
            continue;
        }

        let key = resource.path.to_ascii_lowercase();
        let data = if let Some(patches) = patches_by_target.get(&key) {
            let previous = read_legacy_local_relative_data(
                previous_resource_directory,
                &patches
                    .first()
                    .expect("patch group cannot be empty")
                    .target_resource_relative_path,
            )?;
            apply_legacy_patches_to_resource(
                &previous,
                patches,
                &patch_data_by_path,
                catalog.max_input_chunk_size,
                resource.size_bytes,
                &resource.path,
            )?
        } else {
            read_legacy_local_relative_data(next_resource_directory, &resource.path)?
        };

        validate_legacy_payload_size(&resource.path, data.len(), resource.size_bytes)?;
        if let Some(expected_checksum) = &resource.checksum {
            let actual_checksum = md5_hex(&data);
            if &actual_checksum != expected_checksum {
                return Err(format!(
                    "patched checksum mismatch for {}: expected {}, got {}",
                    resource.path, expected_checksum, actual_checksum
                ));
            }
        }
        resources.push(AppliedLegacyPatchResource {
            path: resource.path.clone(),
            data,
        });
    }

    Ok(AppliedLegacyPatchSet {
        resource_group_resource: patch_data_set.resource_group_resource,
        resource_catalog,
        resources,
    })
}

pub fn create_legacy_resource_group_from_directory(
    directory: impl AsRef<Path>,
    resource_prefix: Option<&str>,
    calculate_compressions: bool,
) -> Result<ResourceCatalog, String> {
    let directory = directory.as_ref();
    if !directory.exists() {
        return Err(format!(
            "input directory does not exist: {}",
            directory.display()
        ));
    }

    let mut resources = Vec::new();
    collect_directory_resources(
        directory,
        directory,
        resource_prefix.filter(|prefix| !prefix.is_empty()),
        calculate_compressions,
        &mut resources,
    )?;

    let total_uncompressed_size_bytes = resources
        .iter()
        .map(|resource| resource.size_bytes)
        .sum::<u64>();
    let total_compressed_size_bytes = if calculate_compressions {
        Some(
            resources
                .iter()
                .map(|resource| resource.compressed_size_bytes.unwrap_or_default())
                .sum::<u64>(),
        )
    } else {
        None
    };

    Ok(ResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("ResourceGroup"),
        total_compressed_size_bytes,
        total_uncompressed_size_bytes,
        resources,
    })
}

pub fn export_legacy_local_relative_resources(
    catalog: &ResourceCatalog,
    resource_source_directory: impl AsRef<Path>,
    resource_destination_directory: impl AsRef<Path>,
) -> Result<(usize, u64), String> {
    let resource_source_directory = resource_source_directory.as_ref();
    if !resource_source_directory.exists() {
        return Err(format!(
            "resource source directory does not exist: {}",
            resource_source_directory.display()
        ));
    }
    let resource_destination_directory = resource_destination_directory.as_ref();
    fs::create_dir_all(resource_destination_directory).map_err(|error| {
        format!(
            "failed to create resource destination directory {}: {error}",
            resource_destination_directory.display()
        )
    })?;

    let mut exported_count = 0_usize;
    let mut exported_bytes = 0_u64;
    for resource in &catalog.resources {
        if resource.location.is_empty() {
            continue;
        }
        let data = read_legacy_local_relative_data(resource_source_directory, &resource.path)?;
        if let Some(expected_checksum) = &resource.checksum {
            let checksum = md5_hex(&data);
            if &checksum != expected_checksum {
                return Err(format!(
                    "resource checksum mismatch for {}: expected {}, got {}",
                    resource.path, expected_checksum, checksum
                ));
            }
        }

        let output_path = resource_destination_directory.join(resource.path.replace('\\', "/"));
        if let Some(parent) = output_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create resource export directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        fs::write(&output_path, &data).map_err(|error| {
            format!(
                "failed to write exported resource {}: {error}",
                output_path.display()
            )
        })?;
        exported_count += 1;
        exported_bytes += data.len() as u64;
    }

    Ok((exported_count, exported_bytes))
}

pub fn parse_legacy_filter_index_mapping_yaml(
    input: &str,
) -> Result<Vec<FilterIndexMapping>, String> {
    let mapping: LegacyFilterIndexMappingFile =
        serde_yaml::from_str(input).map_err(|error| error.to_string())?;
    Ok(mapping
        .filter_index_mappings
        .into_iter()
        .map(|entry| FilterIndexMapping {
            filter_file_paths: entry
                .filter_mapping
                .into_iter()
                .map(|filter| filter.filter_file)
                .collect(),
            output_index_filename: entry.output_index_filename,
        })
        .collect())
}

pub fn create_legacy_resource_group_from_filter_files(
    prefix_map_base_path: impl AsRef<Path>,
    filter_file_paths: &[impl AsRef<Path>],
    calculate_compressions: bool,
) -> Result<ResourceCatalog, String> {
    let prefix_map_base_path = prefix_map_base_path.as_ref();
    if filter_file_paths.is_empty() {
        return Err(String::from("No filter files provided."));
    }

    let filters = filter_file_paths
        .iter()
        .map(|path| {
            let text = fs::read_to_string(path.as_ref()).map_err(|error| error.to_string())?;
            parse_legacy_filter_ini(&text)
        })
        .collect::<Result<Vec<_>, _>>()?;

    create_legacy_resource_group_from_filters(
        prefix_map_base_path,
        &filters,
        calculate_compressions,
    )
}

pub fn create_legacy_resource_groups_from_filter_mapping(
    prefix_map_base_path: impl AsRef<Path>,
    filter_file_base_path: impl AsRef<Path>,
    mappings: &[FilterIndexMapping],
    calculate_compressions: bool,
) -> Result<Vec<(String, ResourceCatalog)>, String> {
    let prefix_map_base_path = prefix_map_base_path.as_ref();
    let filter_file_base_path = filter_file_base_path.as_ref();
    mappings
        .iter()
        .map(|mapping| {
            let filter_paths = mapping
                .filter_file_paths
                .iter()
                .map(|path| filter_file_base_path.join(path))
                .collect::<Vec<_>>();
            let catalog = create_legacy_resource_group_from_filter_files(
                prefix_map_base_path,
                &filter_paths,
                calculate_compressions,
            )?;
            Ok((mapping.output_index_filename.clone(), catalog))
        })
        .collect()
}

pub fn create_legacy_resource_group_from_filters(
    prefix_map_base_path: impl AsRef<Path>,
    filters: &[ResourceFilter],
    calculate_compressions: bool,
) -> Result<ResourceCatalog, String> {
    let prefix_map_base_path = prefix_map_base_path.as_ref();
    if !prefix_map_base_path.exists() {
        return Err(format!(
            "prefix map base path does not exist: {}",
            prefix_map_base_path.display()
        ));
    }
    if filters.is_empty() {
        return Err(String::from("No resource filters provided."));
    }

    let mut search_paths = Vec::<String>::new();
    for filter in filters {
        for prefix_path in filter.prefix_paths() {
            if !search_paths.iter().any(|existing| existing == prefix_path) {
                search_paths.push(prefix_path.clone());
            }
        }
    }

    let mut resources = Vec::new();
    for search_path in search_paths {
        let input_directory = if search_path == "." {
            prefix_map_base_path.to_path_buf()
        } else {
            prefix_map_base_path.join(&search_path)
        };
        if !input_directory.exists() {
            return Err(format!(
                "input directory does not exist: {}",
                input_directory.display()
            ));
        }
        collect_filtered_directory_resources(
            prefix_map_base_path,
            &input_directory,
            &input_directory,
            &search_path,
            filters,
            calculate_compressions,
            &mut resources,
        )?;
    }

    let total_uncompressed_size_bytes = resources
        .iter()
        .map(|resource| resource.size_bytes)
        .sum::<u64>();
    let total_compressed_size_bytes = if calculate_compressions {
        Some(
            resources
                .iter()
                .map(|resource| resource.compressed_size_bytes.unwrap_or_default())
                .sum::<u64>(),
        )
    } else {
        None
    };

    Ok(ResourceCatalog {
        version: String::from("0.1.0"),
        catalog_type: String::from("ResourceGroup"),
        total_compressed_size_bytes,
        total_uncompressed_size_bytes,
        resources,
    })
}

pub fn merge_legacy_resource_catalogs(
    base: &ResourceCatalog,
    merge: &ResourceCatalog,
) -> ResourceCatalog {
    let mut resources_by_path = BTreeMap::<String, ResourceRecord>::new();
    for resource in &base.resources {
        resources_by_path.insert(resource.path.clone(), resource.clone());
    }
    for resource in &merge.resources {
        resources_by_path.insert(resource.path.clone(), resource.clone());
    }

    let resources = resources_by_path.into_values().collect::<Vec<_>>();
    ResourceCatalog {
        version: base.version.clone(),
        catalog_type: base.catalog_type.clone(),
        total_compressed_size_bytes: total_compressed_size(&resources),
        total_uncompressed_size_bytes: resources
            .iter()
            .map(|resource| resource.size_bytes)
            .sum::<u64>(),
        resources,
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceCatalogDiff {
    pub additions: Vec<String>,
    pub subtractions: Vec<String>,
}

pub fn diff_legacy_resource_catalogs(
    base: &ResourceCatalog,
    target: &ResourceCatalog,
) -> ResourceCatalogDiff {
    let base_by_path = base
        .resources
        .iter()
        .map(|resource| (resource.path.clone(), resource))
        .collect::<BTreeMap<_, _>>();
    let target_by_path = target
        .resources
        .iter()
        .map(|resource| (resource.path.clone(), resource))
        .collect::<BTreeMap<_, _>>();

    let mut additions = Vec::new();
    for (path, target_resource) in &target_by_path {
        match base_by_path.get(path) {
            Some(base_resource) if base_resource.checksum == target_resource.checksum => {}
            _ => additions.push(path.clone()),
        }
    }

    let mut subtractions = Vec::new();
    for path in base_by_path.keys() {
        if !target_by_path.contains_key(path) {
            subtractions.push(path.clone());
        }
    }

    ResourceCatalogDiff {
        additions,
        subtractions,
    }
}

pub fn export_legacy_diff(diff: &ResourceCatalogDiff) -> String {
    let mut output = String::new();
    for addition in &diff.additions {
        output.push_str("+ ");
        output.push_str(addition);
        output.push('\n');
    }
    for subtraction in &diff.subtractions {
        output.push_str("- ");
        output.push_str(subtraction);
        output.push('\n');
    }
    output
}

pub fn remove_legacy_resources(
    catalog: &ResourceCatalog,
    paths_to_remove: &[String],
    error_if_missing: bool,
) -> Result<ResourceCatalog, String> {
    let mut resources = catalog.resources.clone();
    for path in paths_to_remove {
        let Some(index) = resources.iter().position(|resource| &resource.path == path) else {
            if error_if_missing {
                return Err(format!("resource not found: {path}"));
            }
            continue;
        };
        resources.remove(index);
    }

    Ok(ResourceCatalog {
        version: catalog.version.clone(),
        catalog_type: catalog.catalog_type.clone(),
        total_compressed_size_bytes: total_compressed_size(&resources),
        total_uncompressed_size_bytes: resources
            .iter()
            .map(|resource| resource.size_bytes)
            .sum::<u64>(),
        resources,
    })
}

fn legacy_resource_catalog_for_bundle_export(
    catalog: &ResourceCatalog,
) -> Result<ResourceCatalog, String> {
    let mut catalog = catalog.clone();
    let version = parse_legacy_version(&catalog.version)
        .ok_or_else(|| format!("invalid resource group version: {}", catalog.version))?;
    if version
        < (LegacyVersion {
            major: 0,
            minor: 1,
            patch: 0,
        })
    {
        catalog.version = String::from("0.1.0");
    }
    Ok(catalog)
}

fn bundle_data_record(
    relative_path: &str,
    resource_type: &str,
    data: &[u8],
) -> Result<BundleResourceRecord, String> {
    let checksum = md5_hex(data);
    Ok(BundleResourceRecord {
        path: relative_path.to_string(),
        resource_type: resource_type.to_string(),
        location: legacy_location_for_path(relative_path, &checksum),
        size_bytes: data.len() as u64,
        compressed_size_bytes: Some(
            gzip_compress(data)
                .map_err(|error| error.to_string())?
                .len() as u64,
        ),
        checksum,
    })
}

fn push_legacy_local_bundle_chunk(
    chunks: &mut Vec<LegacyBundleDataResource>,
    chunk_destination_relative_path: &str,
    chunk_base_name: &str,
    data: Vec<u8>,
) -> Result<(), String> {
    let relative_path = path_to_legacy_string(
        Path::new(chunk_destination_relative_path)
            .join(format!("{chunk_base_name}{}.chunk", chunks.len())),
    );
    chunks.push(LegacyBundleDataResource {
        record: bundle_data_record(&relative_path, "BinaryChunk", &data)?,
        data,
    });
    Ok(())
}

fn push_legacy_remote_cdn_bundle_chunk(
    chunks: &mut Vec<LegacyBundleDataResource>,
    chunk_destination_relative_path: &str,
    chunk_base_name: &str,
    data: Vec<u8>,
) -> Result<(), String> {
    let relative_path = path_to_legacy_string(
        Path::new(chunk_destination_relative_path)
            .join(format!("{chunk_base_name}{}.chunk", chunks.len())),
    );
    chunks.push(legacy_compressed_bundle_data_resource(
        &relative_path,
        "BinaryChunk",
        &data,
    )?);
    Ok(())
}

fn legacy_compressed_bundle_data_resource(
    relative_path: &str,
    resource_type: &str,
    data: &[u8],
) -> Result<LegacyBundleDataResource, String> {
    Ok(LegacyBundleDataResource {
        record: bundle_data_record(relative_path, resource_type, data)?,
        data: gzip_compress(data).map_err(|error| error.to_string())?,
    })
}

fn read_legacy_local_relative_data(
    base_path: &Path,
    relative_path: &str,
) -> Result<Vec<u8>, String> {
    let path = base_path.join(relative_path.replace('\\', "/"));
    read_case_insensitive_path(&path)
}

fn apply_legacy_patches_to_resource(
    previous: &[u8],
    patches: &[&PatchResourceRecord],
    patch_data_by_path: &BTreeMap<String, Vec<u8>>,
    max_input_chunk_size: u64,
    expected_size: u64,
    target_path: &str,
) -> Result<Vec<u8>, String> {
    if max_input_chunk_size == 0 {
        return Err(String::from("invalid max input chunk size: 0"));
    }
    let max_input_chunk_size = usize::try_from(max_input_chunk_size)
        .map_err(|_| String::from("max input chunk size does not fit in usize"))?;
    let expected_size = usize::try_from(expected_size)
        .map_err(|_| format!("expected resource size for {target_path} does not fit in usize"))?;
    let mut output = Vec::with_capacity(expected_size);
    let mut previous_position = 0_usize;

    for patch in patches {
        let data_offset = usize::try_from(patch.data_offset)
            .map_err(|_| format!("patch data offset for {} does not fit in usize", patch.path))?;
        let source_offset = usize::try_from(patch.source_offset).map_err(|_| {
            format!(
                "patch source offset for {} does not fit in usize",
                patch.path
            )
        })?;
        if data_offset > expected_size {
            return Err(format!(
                "patch data offset for {} exceeds expected size of {}",
                patch.path, target_path
            ));
        }
        if output.len() > data_offset {
            return Err(format!(
                "patch {} overlaps earlier output for {}",
                patch.path, target_path
            ));
        }

        let copy_len = data_offset - output.len();
        append_checked_range(
            previous,
            previous_position,
            copy_len,
            &mut output,
            target_path,
        )?;

        if patch.location.is_empty() {
            let copy_len = usize::try_from(patch.size_bytes)
                .map_err(|_| format!("copy patch size for {} does not fit in usize", patch.path))?;
            append_checked_range(previous, source_offset, copy_len, &mut output, target_path)?;
            previous_position = source_offset
                .checked_add(copy_len)
                .ok_or_else(|| format!("previous position overflow for {target_path}"))?;
            continue;
        }

        if source_offset > previous.len() {
            return Err(format!(
                "patch source offset for {} exceeds previous size of {}",
                patch.path, target_path
            ));
        }
        let source_end = source_offset
            .checked_add(max_input_chunk_size)
            .map(|end| end.min(previous.len()))
            .ok_or_else(|| format!("source range overflow for {}", patch.path))?;
        let patch_data = patch_data_by_path
            .get(&patch.path)
            .ok_or_else(|| format!("patch payload missing for {}", patch.path))?;
        let patched = apply_legacy_binary_patch(&previous[source_offset..source_end], patch_data)?;
        output.extend_from_slice(&patched);
        previous_position = source_end;
    }

    if output.len() < expected_size {
        let copy_len = expected_size - output.len();
        append_checked_range(
            previous,
            previous_position,
            copy_len,
            &mut output,
            target_path,
        )?;
    }
    if output.len() != expected_size {
        return Err(format!(
            "patched size mismatch for {target_path}: expected {expected_size}, got {}",
            output.len()
        ));
    }

    Ok(output)
}

fn append_checked_range(
    source: &[u8],
    offset: usize,
    len: usize,
    output: &mut Vec<u8>,
    target_path: &str,
) -> Result<(), String> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| format!("source range overflow for {target_path}"))?;
    if end > source.len() {
        return Err(format!(
            "source range for {target_path} exceeds previous data: offset {offset}, len {len}, previous size {}",
            source.len()
        ));
    }
    output.extend_from_slice(&source[offset..end]);
    Ok(())
}

fn read_legacy_local_cdn_data(
    base_path: &Path,
    resource: &BundleResourceRecord,
) -> Result<Vec<u8>, String> {
    let path = base_path.join(resource.location.replace('\\', "/"));
    let data = read_case_insensitive_path(&path)?;
    let checksum = md5_hex(&data);
    if checksum == resource.checksum {
        return Ok(data);
    }

    Err(format!(
        "resource checksum mismatch for {}: expected {}, got {}",
        resource.path, resource.checksum, checksum
    ))
}

fn read_legacy_remote_cdn_data(
    base_path: &Path,
    resource: &BundleResourceRecord,
) -> Result<Vec<u8>, String> {
    let path = base_path.join(resource.location.replace('\\', "/"));
    let compressed = read_case_insensitive_path(&path)?;
    if let Some(expected_compressed_size) = resource.compressed_size_bytes {
        if compressed.len() as u64 != expected_compressed_size {
            return Err(format!(
                "remote CDN compressed size mismatch for {}: expected {}, got {}",
                resource.path,
                expected_compressed_size,
                compressed.len()
            ));
        }
    }
    let decompressed = gzip_decompress(&compressed).map_err(|error| {
        format!(
            "remote CDN resource {} is not a valid gzip payload: {}",
            resource.path, error
        )
    })?;
    let decompressed_checksum = md5_hex(&decompressed);
    if decompressed_checksum == resource.checksum {
        return Ok(decompressed);
    }
    Err(format!(
        "remote CDN resource checksum mismatch for {}: expected {}, got {} decompressed",
        resource.path, resource.checksum, decompressed_checksum
    ))
}

fn read_legacy_remote_cdn_data_from_local_mirror(
    mirror_base_path: &Path,
    cache_base_path: &Path,
    resource: &BundleResourceRecord,
    stats: &mut LegacyRemoteCdnCacheStats,
) -> Result<Vec<u8>, String> {
    let relative_location = resource.location.replace('\\', "/");
    let cache_path = cache_base_path.join(&relative_location);

    if cache_path.exists() {
        match read_legacy_remote_cdn_data(cache_base_path, resource) {
            Ok(data) => {
                stats.cache_hits += 1;
                return Ok(data);
            }
            Err(_) => {
                remove_existing_cache_entry(&cache_path)?;
                stats.replaced_bad_cache_entries += 1;
            }
        }
    }

    let mirror_path = mirror_base_path.join(&relative_location);
    let payload = read_case_insensitive_path(&mirror_path).map_err(|error| {
        format!(
            "failed to copy remote CDN payload {} into cache: {error}",
            mirror_path.display()
        )
    })?;
    if let Some(parent) = cache_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create remote CDN cache directory {}: {error}",
                parent.display()
            )
        })?;
    }
    fs::write(&cache_path, &payload).map_err(|error| {
        format!(
            "failed to write remote CDN cache file {}: {error}",
            cache_path.display()
        )
    })?;
    stats.downloads += 1;
    stats.bytes_copied_to_cache += payload.len() as u64;

    read_legacy_remote_cdn_data(cache_base_path, resource)
}

fn remove_existing_cache_entry(path: &Path) -> Result<(), String> {
    let metadata = fs::metadata(path)
        .map_err(|error| format!("failed to inspect cache entry {}: {error}", path.display()))?;
    if metadata.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
    .map_err(|error| {
        format!(
            "failed to remove bad cache entry {}: {error}",
            path.display()
        )
    })
}

fn read_legacy_patch_group_data(
    base_path: &Path,
    resource: &PatchResourceGroupRecord,
) -> Result<LegacyPatchDataResource, String> {
    let data = read_legacy_cdn_payload(
        base_path,
        &resource.path,
        &resource.location,
        &resource.checksum,
        resource.size_bytes,
        resource.compressed_size_bytes,
    )?;
    Ok(LegacyPatchDataResource {
        path: resource.path.clone(),
        resource_type: resource.resource_type.clone(),
        location: resource.location.clone(),
        checksum: resource.checksum.clone(),
        data,
    })
}

fn read_legacy_patch_resource_data(
    base_path: &Path,
    resource: &PatchResourceRecord,
) -> Result<LegacyPatchDataResource, String> {
    let data = read_legacy_cdn_payload(
        base_path,
        &resource.path,
        &resource.location,
        &resource.checksum,
        resource.size_bytes,
        resource.compressed_size_bytes,
    )?;
    Ok(LegacyPatchDataResource {
        path: resource.path.clone(),
        resource_type: resource.resource_type.clone(),
        location: resource.location.clone(),
        checksum: resource.checksum.clone(),
        data,
    })
}

fn read_legacy_cdn_payload(
    base_path: &Path,
    relative_path: &str,
    location: &str,
    checksum: &str,
    size_bytes: u64,
    compressed_size_bytes: Option<u64>,
) -> Result<Vec<u8>, String> {
    let path = base_path.join(location.replace('\\', "/"));
    let data = read_case_insensitive_path(&path)?;
    let checksum_actual = md5_hex(&data);
    if checksum_actual == checksum {
        validate_legacy_payload_size(relative_path, data.len(), size_bytes)?;
        return Ok(data);
    }

    if compressed_size_bytes.is_some() {
        let decompressed = gzip_decompress(&data).map_err(|error| {
            format!(
                "resource checksum mismatch for {relative_path}: expected {checksum}, got {checksum_actual}; gzip decompress failed: {error}"
            )
        })?;
        let decompressed_checksum = md5_hex(&decompressed);
        if decompressed_checksum == checksum {
            validate_legacy_payload_size(relative_path, decompressed.len(), size_bytes)?;
            return Ok(decompressed);
        }
        return Err(format!(
            "resource checksum mismatch for {relative_path}: expected {checksum}, got {checksum_actual} compressed and {decompressed_checksum} decompressed"
        ));
    }

    Err(format!(
        "resource checksum mismatch for {relative_path}: expected {checksum}, got {checksum_actual}"
    ))
}

fn validate_legacy_payload_size(
    relative_path: &str,
    actual_size: usize,
    expected_size: u64,
) -> Result<(), String> {
    if actual_size as u64 == expected_size {
        return Ok(());
    }
    Err(format!(
        "resource size mismatch for {relative_path}: expected {expected_size}, got {actual_size}"
    ))
}

fn read_case_insensitive_path(path: &Path) -> Result<Vec<u8>, String> {
    let resolved = resolve_existing_case_insensitive_path(path)
        .ok_or_else(|| format!("file does not exist: {}", path.display()))?;
    fs::read(&resolved).map_err(|error| error.to_string())
}

fn resolve_existing_case_insensitive_path(path: &Path) -> Option<PathBuf> {
    if path.exists() {
        return Some(path.to_path_buf());
    }

    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {
                if resolved.as_os_str().is_empty() {
                    resolved.push(".");
                }
            }
            Component::ParentDir => resolved.push(".."),
            Component::Normal(name) => {
                let exact = resolved.join(name);
                if exact.exists() {
                    resolved = exact;
                    continue;
                }

                let name = name.to_string_lossy();
                let directory = if resolved.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    resolved.as_path()
                };
                let mut matched = None;
                for entry in fs::read_dir(directory).ok()?.flatten() {
                    if entry
                        .file_name()
                        .to_string_lossy()
                        .eq_ignore_ascii_case(&name)
                    {
                        matched = Some(entry.path());
                        break;
                    }
                }
                resolved = matched?;
            }
        }
    }

    resolved.exists().then_some(resolved)
}

fn validate_legacy_binary_operation(
    binary_operation: Option<u64>,
) -> Result<Option<u64>, LegacyResourceError> {
    if let Some(value) = binary_operation {
        if value > u64::from(u32::MAX) {
            return Err(LegacyResourceError::new(
                LegacyResourceResultType::MalformedResourceInput,
                format!("binary operation out of range: {value}"),
            ));
        }
    }
    Ok(binary_operation)
}

fn bundle_record_from_legacy(
    resource: LegacyYamlTypedResource,
) -> Result<BundleResourceRecord, LegacyResourceError> {
    Ok(BundleResourceRecord {
        path: resource.relative_path,
        resource_type: resource.resource_type,
        location: resource.location,
        size_bytes: resource.uncompressed_size,
        compressed_size_bytes: resource.compressed_size,
        checksum: resource.checksum,
    })
}

fn patch_group_record_from_legacy(resource: LegacyYamlTypedResource) -> PatchResourceGroupRecord {
    PatchResourceGroupRecord {
        path: resource.relative_path,
        resource_type: resource.resource_type,
        location: resource.location,
        size_bytes: resource.uncompressed_size,
        compressed_size_bytes: resource.compressed_size,
        checksum: resource.checksum,
    }
}

fn patch_record_from_legacy(resource: LegacyYamlPatchResource) -> PatchResourceRecord {
    PatchResourceRecord {
        path: resource.relative_path,
        resource_type: resource.resource_type,
        location: resource.location,
        size_bytes: resource.uncompressed_size,
        compressed_size_bytes: resource.compressed_size,
        checksum: resource.checksum,
        target_resource_relative_path: resource.target_resource_relative_path,
        data_offset: resource.data_offset,
        source_offset: resource.source_offset,
    }
}

fn push_legacy_bundle_record_yaml(
    output: &mut String,
    resource: &BundleResourceRecord,
    indent: &str,
) {
    output.push_str(&format!("{indent}RelativePath: {}\n", resource.path));
    output.push_str(&format!("{indent}Type: {}\n", resource.resource_type));
    output.push_str(&format!("{indent}Location: {}\n", resource.location));
    output.push_str(&format!("{indent}Checksum: {}\n", resource.checksum));
    output.push_str(&format!(
        "{indent}UncompressedSize: {}\n",
        resource.size_bytes
    ));
    if let Some(compressed_size) = resource.compressed_size_bytes {
        output.push_str(&format!("{indent}CompressedSize: {compressed_size}\n"));
    }
}

fn push_legacy_patch_group_record_yaml(
    output: &mut String,
    resource: &PatchResourceGroupRecord,
    indent: &str,
) {
    output.push_str(&format!("{indent}RelativePath: {}\n", resource.path));
    output.push_str(&format!("{indent}Type: {}\n", resource.resource_type));
    push_legacy_yaml_scalar_line(output, indent, "Location", &resource.location);
    output.push_str(&format!("{indent}Checksum: {}\n", resource.checksum));
    output.push_str(&format!(
        "{indent}UncompressedSize: {}\n",
        resource.size_bytes
    ));
    if let Some(compressed_size) = resource.compressed_size_bytes {
        output.push_str(&format!("{indent}CompressedSize: {compressed_size}\n"));
    }
}

fn push_legacy_yaml_scalar_line(output: &mut String, indent: &str, key: &str, value: &str) {
    if value.is_empty() {
        output.push_str(&format!("{indent}{key}: \"\"\n"));
    } else {
        output.push_str(&format!("{indent}{key}: {value}\n"));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LegacyVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

fn normalize_legacy_yaml_version(version: &str) -> Result<String, LegacyResourceError> {
    const SUPPORTED: LegacyVersion = LegacyVersion {
        major: 0,
        minor: 1,
        patch: 0,
    };
    let parsed = parse_legacy_version(version).ok_or_else(|| {
        LegacyResourceError::new(
            LegacyResourceResultType::MalformedResourceGroup,
            format!("invalid resource group version: {version}"),
        )
    })?;

    if parsed.major > SUPPORTED.major {
        return Err(LegacyResourceError::new(
            LegacyResourceResultType::DocumentVersionUnsupported,
            format!("unsupported resource group version: {version}"),
        ));
    }
    if parsed > SUPPORTED {
        Ok(format!(
            "{}.{}.{}",
            SUPPORTED.major, SUPPORTED.minor, SUPPORTED.patch
        ))
    } else {
        Ok(version.to_string())
    }
}

fn parse_legacy_version(version: &str) -> Option<LegacyVersion> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;
    let patch = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(LegacyVersion {
        major,
        minor,
        patch,
    })
}

fn yaml_mapping_contains_key(mapping: &serde_yaml::Mapping, key: &str) -> bool {
    mapping.contains_key(&serde_yaml::Value::String(key.to_string()))
}

fn split_legacy_resource_path(input: &str) -> (Option<String>, String) {
    if let Some((prefix, path)) = input.split_once(":/") {
        (Some(prefix.to_string()), path.to_string())
    } else {
        (None, input.to_string())
    }
}

fn total_compressed_size(resources: &[ResourceRecord]) -> Option<u64> {
    let mut total = 0_u64;
    for resource in resources {
        total += resource.compressed_size_bytes?;
    }
    Some(total)
}

fn collect_directory_resources(
    root: &Path,
    directory: &Path,
    resource_prefix: Option<&str>,
    calculate_compressions: bool,
    resources: &mut Vec<ResourceRecord>,
) -> Result<(), String> {
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if file_type.is_dir() {
            collect_directory_resources(
                root,
                &path,
                resource_prefix,
                calculate_compressions,
                resources,
            )?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let data = fs::read(&path).map_err(|error| error.to_string())?;
        let relative_path = legacy_relative_path(root, &path)?;
        let checksum = md5_hex(&data);
        let compressed_size_bytes = if calculate_compressions {
            Some(
                gzip_compress(&data)
                    .map_err(|error| error.to_string())?
                    .len() as u64,
            )
        } else {
            None
        };
        resources.push(ResourceRecord {
            location: legacy_location_for_path(&relative_path, &checksum),
            path: relative_path,
            size_bytes: data.len() as u64,
            compressed_size_bytes,
            checksum: Some(checksum),
            binary_operation: Some(legacy_binary_operation(&path)?),
            prefix: resource_prefix.map(str::to_string),
        });
    }
    Ok(())
}

fn collect_filtered_directory_resources(
    prefix_map_base_path: &Path,
    search_root: &Path,
    directory: &Path,
    search_path: &str,
    filters: &[ResourceFilter],
    calculate_compressions: bool,
    resources: &mut Vec<ResourceRecord>,
) -> Result<(), String> {
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if file_type.is_dir() {
            collect_filtered_directory_resources(
                prefix_map_base_path,
                search_root,
                &path,
                search_path,
                filters,
                calculate_compressions,
                resources,
            )?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let normalised_relative_to_base = legacy_relative_path(prefix_map_base_path, &path)?;
        let matched = filters.iter().any(|filter| {
            filter
                .prefix_paths()
                .iter()
                .any(|prefix_path| prefix_path == search_path)
                && filter.check_path(&normalised_relative_to_base)
        });
        if !matched {
            continue;
        }

        let data = fs::read(&path).map_err(|error| error.to_string())?;
        let relative_path = legacy_relative_path(search_root, &path)?;
        let checksum = md5_hex(&data);
        let compressed_size_bytes = if calculate_compressions {
            Some(
                gzip_compress(&data)
                    .map_err(|error| error.to_string())?
                    .len() as u64,
            )
        } else {
            None
        };
        resources.push(ResourceRecord {
            location: legacy_location_for_path(&relative_path, &checksum),
            path: relative_path,
            size_bytes: data.len() as u64,
            compressed_size_bytes,
            checksum: Some(checksum),
            binary_operation: Some(legacy_binary_operation(&path)?),
            prefix: None,
        });
    }
    Ok(())
}

fn legacy_relative_path(root: &Path, path: &Path) -> Result<String, String> {
    path.strip_prefix(root)
        .map_err(|error| error.to_string())
        .map(path_to_legacy_string)
}

fn path_to_legacy_string(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().replace('\\', "/")
}

fn legacy_location_for_path(relative_path: &str, checksum: &str) -> String {
    let path_checksum = legacy_fnv_hex(relative_path.as_bytes());
    format!("{}/{}_{}", &path_checksum[..2], path_checksum, checksum)
}

fn legacy_binary_operation(path: &Path) -> Result<u64, String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(u64::from(metadata.mode()))
    }
    #[cfg(not(unix))]
    {
        let readonly = metadata.permissions().readonly();
        let mut mode = 0x8000_u64;
        mode |= if readonly { 0o444 } else { 0o666 };
        Ok(mode)
    }
}

fn legacy_resource_path(resource: &ResourceRecord) -> String {
    if let Some(prefix) = &resource.prefix {
        format!("{prefix}:/{}", resource.path)
    } else {
        resource.path.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceFilter {
    sections: Vec<Vec<FilterPath>>,
    prefix_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FilterPath {
    path: String,
    path_contains_wildcard: bool,
    include_rules: Vec<String>,
    exclude_rules: Vec<String>,
    contains_local_include_exclude_rules: bool,
}

#[derive(Debug, Clone, Default)]
struct IniSection {
    values: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct PrefixMapping {
    id: String,
    paths: Vec<String>,
}

#[derive(Debug, Clone)]
struct RawResPath {
    prefix_id: String,
    path: String,
    include_rules: Vec<String>,
    exclude_rules: Vec<String>,
}

impl ResourceFilter {
    pub fn check_path(&self, path: &str) -> bool {
        for section in &self.sections {
            let mut include_or_exclude_rules_failed_for_section = false;

            for filter_path in section {
                let specific_file_match = !filter_path.path_contains_wildcard
                    && filter_path.path.len() == path.len()
                    && filter_path.path == path;

                if specific_file_match {
                    if filter_path.contains_local_include_exclude_rules {
                        continue;
                    }
                    return true;
                }

                if !filter_path.path_contains_wildcard {
                    continue;
                }

                if include_or_exclude_rules_failed_for_section {
                    continue;
                }

                if filter_path
                    .exclude_rules
                    .iter()
                    .any(|rule| path.contains(rule))
                {
                    include_or_exclude_rules_failed_for_section = true;
                    continue;
                }

                if !filter_path.include_rules.is_empty()
                    && !filter_path
                        .include_rules
                        .iter()
                        .any(|rule| path.contains(rule))
                {
                    include_or_exclude_rules_failed_for_section = true;
                    continue;
                }

                if wildcard_path_matches(&filter_path.path, path) {
                    return true;
                }
            }
        }

        false
    }

    pub fn prefix_paths(&self) -> &[String] {
        &self.prefix_paths
    }
}

pub fn parse_legacy_filter_ini(input: &str) -> Result<ResourceFilter, String> {
    let sections = parse_ini_sections(input)?;
    let default = sections
        .get("default")
        .ok_or_else(|| String::from("Required [DEFAULT] section not present in INI file."))?;
    let prefixes = parse_prefix_mappings(
        default
            .values
            .get("prefixmap")
            .map(String::as_str)
            .unwrap_or_default(),
    )?;
    let (global_include, global_exclude) = parse_include_exclude_rules(
        default
            .values
            .get("filter")
            .map(String::as_str)
            .unwrap_or_default(),
    )?;

    let mut filter_sections = Vec::new();
    for (section_name, section) in sections
        .iter()
        .filter(|(name, _)| name.as_str() != "default")
    {
        let (section_include, section_exclude) = parse_include_exclude_rules(
            section
                .values
                .get("filter")
                .map(String::as_str)
                .unwrap_or_default(),
        )?;
        let raw_respaths = parse_section_respaths(
            section
                .values
                .get("respaths")
                .map(String::as_str)
                .unwrap_or_default(),
            &prefixes,
        )?;

        let mut include_rules = merge_rules(&global_include, &section_include);
        let mut exclude_rules = merge_rules(&global_exclude, &section_exclude);
        let mut filter_paths = Vec::new();

        for raw_respath in raw_respaths {
            include_rules = merge_rules(&include_rules, &raw_respath.include_rules);
            exclude_rules = merge_rules(&exclude_rules, &raw_respath.exclude_rules);
            let prefix = prefixes
                .iter()
                .find(|prefix| prefix.id == raw_respath.prefix_id)
                .ok_or_else(|| format!("Respath referencing unknown prefix: {section_name}"))?;

            for prefix_path in &prefix.paths {
                let mut path = if prefix_path == "." {
                    raw_respath.path.clone()
                } else {
                    format!("{prefix_path}{}", raw_respath.path)
                };
                path = path.replace('\\', "/");
                if path.starts_with('/') {
                    path = path[1..].to_string();
                }
                let path_contains_wildcard = path.contains('*') || path.contains("...");
                filter_paths.push(FilterPath {
                    path,
                    path_contains_wildcard,
                    include_rules: include_rules.clone(),
                    exclude_rules: exclude_rules.clone(),
                    contains_local_include_exclude_rules: !raw_respath.include_rules.is_empty()
                        || !raw_respath.exclude_rules.is_empty(),
                });
            }
        }

        filter_sections.push(filter_paths);
    }

    Ok(ResourceFilter {
        prefix_paths: prefixes
            .iter()
            .flat_map(|prefix| prefix.paths.iter().cloned())
            .collect(),
        sections: filter_sections,
    })
}

fn parse_ini_sections(input: &str) -> Result<BTreeMap<String, IniSection>, String> {
    let mut sections = BTreeMap::<String, IniSection>::new();
    let mut current_section = String::new();
    let mut current_key: Option<String> = None;

    for raw_line in input.lines() {
        let line = raw_line.trim_end_matches('\r');
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }

        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            current_section = trimmed[1..trimmed.len() - 1].to_ascii_lowercase();
            sections.entry(current_section.clone()).or_default();
            current_key = None;
            continue;
        }

        if line.chars().next().is_some_and(char::is_whitespace) {
            if let Some(key) = &current_key {
                if let Some(section) = sections.get_mut(&current_section) {
                    let value = section.values.entry(key.clone()).or_default();
                    value.push('\n');
                    value.push_str(trimmed);
                    continue;
                }
            }
        }

        let Some((key, value)) = trimmed.split_once('=') else {
            return Err(format!("Invalid INI line: {line}"));
        };
        if current_section.is_empty() {
            return Err(String::from("INI key found before section header"));
        }
        let key = key.trim().to_ascii_lowercase();
        sections
            .entry(current_section.clone())
            .or_default()
            .values
            .insert(key.clone(), value.trim().to_string());
        current_key = Some(key);
    }

    Ok(sections)
}

fn parse_prefix_mappings(input: &str) -> Result<Vec<PrefixMapping>, String> {
    let mut prefixes = Vec::<PrefixMapping>::new();
    for token in input.split_whitespace() {
        let Some((prefix_id, paths)) = token.split_once(':') else {
            return Err(String::from("Invalid prefixmap format: missing ':'"));
        };
        if prefix_id.is_empty() || paths.is_empty() {
            return Err(String::from("Invalid prefixmap format"));
        }

        let index = prefixes
            .iter()
            .position(|prefix| prefix.id == prefix_id)
            .unwrap_or_else(|| {
                prefixes.push(PrefixMapping {
                    id: prefix_id.to_string(),
                    paths: Vec::new(),
                });
                prefixes.len() - 1
            });
        prefixes[index].paths.extend(
            paths
                .split(';')
                .filter(|path| !path.is_empty())
                .map(str::to_string),
        );
    }
    Ok(prefixes)
}

fn parse_include_exclude_rules(input: &str) -> Result<(Vec<String>, Vec<String>), String> {
    let mut include_rules = Vec::new();
    let mut exclude_rules = Vec::new();
    let bytes = input.as_bytes();
    let mut pos = 0;

    while pos < input.len() {
        while pos < input.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= input.len() {
            break;
        }

        let mut is_exclude = false;
        if bytes[pos] == b'!' {
            is_exclude = true;
            pos += 1;
            while pos < input.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
        }

        if pos >= input.len() || bytes[pos] != b'[' {
            return Err(String::from("Invalid filter format: missing '['"));
        }
        pos += 1;
        let Some(relative_end) = input[pos..].find(']') else {
            return Err(String::from("Invalid filter format: missing ']'"));
        };
        let end = pos + relative_end;
        let entries = &input[pos..end];
        for token in entries.split_whitespace() {
            if is_exclude {
                insert_unique(&mut exclude_rules, token.to_string());
            } else {
                insert_unique(&mut include_rules, token.to_string());
            }
        }
        pos = end + 1;
    }

    Ok((include_rules, exclude_rules))
}

fn parse_section_respaths(
    input: &str,
    prefixes: &[PrefixMapping],
) -> Result<Vec<RawResPath>, String> {
    let mut respaths = Vec::new();
    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let raw_prefix_path = parts.next().unwrap_or_default();
        let Some((prefix_id, path)) = raw_prefix_path.split_once(':') else {
            return Err(format!("Missing prefix in path for: {line}"));
        };
        if path.contains("../") {
            return Err(format!("Escaping paths not supported for respaths: {line}"));
        }
        if !prefixes.iter().any(|prefix| prefix.id == prefix_id) {
            return Err(format!("Respath referencing unknown prefix: {line}"));
        }
        let (include_rules, exclude_rules) =
            parse_include_exclude_rules(parts.next().unwrap_or_default())?;
        respaths.push(RawResPath {
            prefix_id: prefix_id.to_string(),
            path: path.to_string(),
            include_rules,
            exclude_rules,
        });
    }
    Ok(respaths)
}

fn merge_rules(left: &[String], right: &[String]) -> Vec<String> {
    let mut result = left.to_vec();
    for rule in right {
        insert_unique(&mut result, rule.clone());
    }
    result
}

fn insert_unique(target: &mut Vec<String>, value: String) {
    if !target.iter().any(|existing| existing == &value) {
        target.push(value);
        target.sort();
    }
}

fn wildcard_path_matches(pattern: &str, path: &str) -> bool {
    wildcard_path_matches_inner(
        pattern.to_ascii_lowercase().as_bytes(),
        path.to_ascii_lowercase().as_bytes(),
    )
}

fn wildcard_path_matches_inner(pattern: &[u8], path: &[u8]) -> bool {
    if pattern.is_empty() {
        return path.is_empty();
    }

    if pattern.starts_with(b"...") {
        for consumed in 0..=path.len() {
            if wildcard_path_matches_inner(&pattern[3..], &path[consumed..]) {
                return true;
            }
        }
        return false;
    }

    if pattern[0] == b'*' {
        for consumed in 0..=path.len() {
            if consumed > 0 && path[consumed - 1] == b'/' {
                break;
            }
            if wildcard_path_matches_inner(&pattern[1..], &path[consumed..]) {
                return true;
            }
        }
        return false;
    }

    !path.is_empty()
        && pattern[0] == path[0]
        && wildcard_path_matches_inner(&pattern[1..], &path[1..])
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyYamlResourceGroup {
    version: String,
    #[serde(rename = "Type")]
    catalog_type: String,
    number_of_resources: u64,
    #[serde(default)]
    total_resources_size_compressed: Option<u64>,
    #[serde(rename = "TotalResourcesSizeUnCompressed")]
    total_resources_size_uncompressed: u64,
    resources: Option<Vec<LegacyYamlResource>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyYamlBundleResourceGroup {
    version: String,
    #[serde(rename = "Type")]
    catalog_type: String,
    number_of_resources: u64,
    #[serde(default)]
    total_resources_size_compressed: Option<u64>,
    #[serde(rename = "TotalResourcesSizeUnCompressed")]
    total_resources_size_uncompressed: u64,
    resource_group_resource: LegacyYamlTypedResource,
    chunk_size: u64,
    resources: Option<Vec<LegacyYamlTypedResource>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyYamlPatchResourceGroup {
    version: String,
    #[serde(rename = "Type")]
    catalog_type: String,
    number_of_resources: u64,
    #[serde(default)]
    total_resources_size_compressed: Option<u64>,
    #[serde(rename = "TotalResourcesSizeUnCompressed")]
    total_resources_size_uncompressed: u64,
    resource_group_resource: LegacyYamlTypedResource,
    max_input_chunk_size: u64,
    #[serde(default)]
    removed_resource_relative_paths: Option<Vec<String>>,
    resources: Option<Vec<LegacyYamlPatchResource>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyYamlResource {
    relative_path: String,
    location: String,
    checksum: String,
    uncompressed_size: u64,
    compressed_size: Option<u64>,
    #[serde(default)]
    binary_operation: Option<u64>,
    #[serde(default)]
    prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyYamlTypedResource {
    relative_path: String,
    #[serde(rename = "Type")]
    resource_type: String,
    location: String,
    checksum: String,
    uncompressed_size: u64,
    compressed_size: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyYamlPatchResource {
    relative_path: String,
    #[serde(rename = "Type")]
    resource_type: String,
    location: String,
    checksum: String,
    uncompressed_size: u64,
    compressed_size: Option<u64>,
    target_resource_relative_path: String,
    data_offset: u64,
    source_offset: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyFilterIndexMappingFile {
    filter_index_mappings: Vec<LegacyFilterIndexMappingEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyFilterIndexMappingEntry {
    filter_mapping: Vec<LegacyFilterFileEntry>,
    output_index_filename: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyFilterFileEntry {
    filter_file: String,
}

pub fn md5_hex(data: &[u8]) -> String {
    format!("{:x}", md5::compute(data))
}

pub fn md5_hex_stream_chunks<I, T>(chunks: I) -> String
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let mut context = md5::Context::new();
    for chunk in chunks {
        context.consume(chunk.as_ref());
    }
    format!("{:x}", context.compute())
}

pub fn legacy_fnv_hex(input: &[u8]) -> String {
    let mut hash = 14_695_981_039_346_656_037_u64;
    let prime = 1_099_511_628_211_u64;

    for byte in input {
        hash = hash.wrapping_mul(prime);
        hash ^= u64::from(*byte);
    }

    format!("{hash:016x}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollingChecksum {
    pub alpha: u32,
    pub beta: u32,
    pub checksum: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMatch {
    pub source_offset: usize,
    pub destination_offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone)]
pub struct LegacyChunkIndex {
    file_to_index: PathBuf,
    chunk_size: usize,
    index_folder: PathBuf,
    checksum_filter: BTreeSet<u32>,
    index_files: Vec<PathBuf>,
    current_index_file: usize,
}

const LEGACY_CHUNK_INDEX_BLOCK_SIZE: usize = std::mem::size_of::<u32>() * 2;
const LEGACY_CHUNK_INDEX_TARGET_FILE_SIZE: usize = 1024 * 1024 * 512;
const LEGACY_CHUNK_INDEX_BLOCKS_PER_FILE: usize =
    LEGACY_CHUNK_INDEX_TARGET_FILE_SIZE / LEGACY_CHUNK_INDEX_BLOCK_SIZE;

pub fn rolling_adler_checksum(input: &[u8], start: usize, end: usize) -> RollingChecksum {
    const MODULO: u32 = 2 << 15;

    let window = &input[start..end];
    let alpha = window
        .iter()
        .fold(0_u32, |sum, byte| sum + u32::from(*byte))
        % MODULO;
    let beta = window.iter().enumerate().fold(0_u32, |sum, (index, byte)| {
        sum + ((end - start - index) as u32) * u32::from(*byte)
    }) % MODULO;

    RollingChecksum {
        alpha,
        beta,
        checksum: alpha + beta * MODULO,
    }
}

impl LegacyChunkIndex {
    pub fn new(
        file_to_index: impl AsRef<Path>,
        chunk_size: usize,
        index_folder: impl AsRef<Path>,
    ) -> Self {
        Self {
            file_to_index: file_to_index.as_ref().to_path_buf(),
            chunk_size,
            index_folder: index_folder.as_ref().to_path_buf(),
            checksum_filter: BTreeSet::new(),
            index_files: Vec::new(),
            current_index_file: 0,
        }
    }

    pub fn index_files(&self) -> &[PathBuf] {
        &self.index_files
    }

    pub fn generate_checksum_filter(
        &mut self,
        target_file: impl AsRef<Path>,
    ) -> std::io::Result<()> {
        if self.chunk_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "legacy chunk index chunk size must be non-zero",
            ));
        }

        self.checksum_filter.clear();
        let target = fs::read(target_file)?;
        for chunk in target.chunks(self.chunk_size) {
            if chunk.is_empty() {
                continue;
            }
            self.checksum_filter
                .insert(rolling_adler_checksum(chunk, 0, chunk.len()).checksum);
        }
        Ok(())
    }

    pub fn generate(&mut self) -> std::io::Result<()> {
        if self.chunk_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "legacy chunk index chunk size must be non-zero",
            ));
        }

        self.index_files.clear();
        self.current_index_file = 0;
        fs::create_dir_all(&self.index_folder)?;
        let data = fs::read(&self.file_to_index)?;
        let mut records = Vec::new();

        if data.len() >= self.chunk_size {
            for offset in 0..=data.len() - self.chunk_size {
                let checksum =
                    rolling_adler_checksum(&data, offset, offset + self.chunk_size).checksum;
                if !self.checksum_filter.is_empty() && !self.checksum_filter.contains(&checksum) {
                    continue;
                }
                let relative_offset = offset
                    .checked_sub(self.current_index_file * LEGACY_CHUNK_INDEX_BLOCKS_PER_FILE)
                    .unwrap_or(offset);
                let relative_offset = u32::try_from(relative_offset).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "legacy chunk index offset exceeds u32",
                    )
                })?;
                records.push((checksum, relative_offset));
                if records.len() >= LEGACY_CHUNK_INDEX_BLOCKS_PER_FILE {
                    self.flush(&mut records)?;
                }
            }
        }

        self.flush(&mut records)
    }

    pub fn find_chunk_offsets(&self, checksum: u32) -> std::io::Result<Vec<usize>> {
        let mut offsets = Vec::new();
        let mut base_offset = 0_usize;
        for path in &self.index_files {
            find_legacy_chunk_offsets_in_index_file(checksum, path, base_offset, &mut offsets)?;
            base_offset += LEGACY_CHUNK_INDEX_BLOCKS_PER_FILE;
        }
        Ok(offsets)
    }

    pub fn find_matching_chunk(&self, chunk: &[u8]) -> std::io::Result<Option<usize>> {
        if chunk.is_empty() {
            return Ok(None);
        }
        let checksum = rolling_adler_checksum(chunk, 0, chunk.len()).checksum;
        let source = fs::read(&self.file_to_index)?;
        for offset in self.find_chunk_offsets(checksum)? {
            let end = offset.saturating_add(chunk.len());
            if end <= source.len() && &source[offset..end] == chunk {
                return Ok(Some(offset));
            }
        }
        Ok(None)
    }

    fn flush(&mut self, records: &mut Vec<(u32, u32)>) -> std::io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        records.sort_unstable();
        let path = self.generate_index_path();
        self.current_index_file += 1;
        let mut output = Vec::with_capacity(records.len() * LEGACY_CHUNK_INDEX_BLOCK_SIZE);
        for (checksum, offset) in records.drain(..) {
            output.extend_from_slice(&checksum.to_le_bytes());
            output.extend_from_slice(&offset.to_le_bytes());
        }
        fs::write(&path, output)?;
        self.index_files.push(path);
        Ok(())
    }

    fn generate_index_path(&self) -> PathBuf {
        let filename = self
            .file_to_index
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("chunk");
        self.index_folder
            .join(format!("{filename}{}.index", self.current_index_file))
    }
}

fn find_legacy_chunk_offsets_in_index_file(
    checksum: u32,
    path: impl AsRef<Path>,
    base_offset: usize,
    offsets: &mut Vec<usize>,
) -> std::io::Result<()> {
    let bytes = fs::read(path)?;
    for record in bytes.chunks_exact(LEGACY_CHUNK_INDEX_BLOCK_SIZE) {
        let current = u32::from_le_bytes(record[0..4].try_into().expect("record checksum bytes"));
        if current == checksum {
            let relative =
                u32::from_le_bytes(record[4..8].try_into().expect("record offset bytes"));
            offsets.push(base_offset + relative as usize);
        }
    }
    Ok(())
}

pub fn gzip_compress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    gzip_compress_with_zlib(data).map(normalize_legacy_gzip_header)
}

pub fn gzip_compress_stream_chunks<I, T>(chunks: I) -> std::io::Result<Vec<u8>>
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    for chunk in chunks {
        encoder.write_all(chunk.as_ref())?;
    }
    encoder.finish().map(normalize_legacy_gzip_header)
}

pub fn gzip_decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    Ok(output)
}

pub fn legacy_file_stream_read_chunks(
    path: impl AsRef<Path>,
    chunk_size: usize,
) -> std::io::Result<Vec<Vec<u8>>> {
    if chunk_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "legacy file stream chunk size must be non-zero",
        ));
    }

    let mut file = fs::File::open(path)?;
    let mut chunks = Vec::new();
    loop {
        let mut chunk = vec![0_u8; chunk_size];
        let read = file.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        chunk.truncate(read);
        chunks.push(chunk);
    }
    Ok(chunks)
}

pub fn legacy_file_stream_write_chunks<I, T>(
    path: impl AsRef<Path>,
    chunks: I,
) -> std::io::Result<u64>
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }

    let mut file = fs::File::create(path)?;
    let mut written = 0_u64;
    for chunk in chunks {
        let chunk = chunk.as_ref();
        file.write_all(chunk)?;
        written += chunk.len() as u64;
    }
    file.flush()?;
    Ok(written)
}

pub fn find_matching_chunks(source: &[u8], destination: &[u8]) -> Vec<ChunkMatch> {
    let window = 4_usize;
    if destination.len() < window || source.len() < window {
        return Vec::new();
    }

    let source_start_checksum = rolling_adler_checksum(source, 0, window);
    let mut result = Vec::new();
    let mut start = 0_usize;
    let mut end = window;
    while end < destination.len() {
        let destination_checksum = rolling_adler_checksum(destination, start, end);
        let mut current_length = 0_usize;
        let mut max_length = 0_usize;
        let mut source_start = 0_usize;
        let mut source_end = window;
        let mut source_match_start = source_start;
        let mut best_match = ChunkMatch {
            source_offset: 0,
            destination_offset: start,
            length: 0,
        };

        while source_end <= source.len() && end + current_length <= destination.len() {
            let rolling_destination_checksum = if current_length == 0 {
                destination_checksum
            } else {
                rolling_adler_checksum(destination, start + current_length, end + current_length)
            };
            let rolling_source_checksum = if source_start == 0 {
                source_start_checksum
            } else {
                rolling_adler_checksum(source, source_start, source_end)
            };

            if rolling_destination_checksum.checksum == rolling_source_checksum.checksum
                && destination[start + current_length..end + current_length]
                    == source[source_start..source_end]
            {
                if current_length == 0 {
                    source_match_start = source_start;
                }
                current_length += 1;
                if current_length > max_length {
                    max_length = current_length;
                    best_match = ChunkMatch {
                        source_offset: source_match_start,
                        destination_offset: start,
                        length: current_length + window - 1,
                    };
                }
            } else {
                current_length = 0;
            }

            source_start += 1;
            source_end += 1;
        }

        if max_length > 0 {
            let subsumed = result.iter().rev().any(|previous: &ChunkMatch| {
                let delta = best_match.destination_offset - previous.destination_offset;
                delta <= best_match.length && previous.length >= delta + best_match.length
            });
            if !subsumed {
                result.push(best_match);
            }
        }

        start += 1;
        end += 1;
    }

    result
}

pub fn find_matching_chunk_in_file(
    chunk: &[u8],
    file_path: impl AsRef<Path>,
) -> std::io::Result<Option<usize>> {
    if chunk.is_empty() {
        return Ok(None);
    }
    let data = fs::read(file_path)?;
    Ok(data
        .windows(chunk.len())
        .position(|candidate| candidate == chunk))
}

pub fn count_matching_chunks(
    file_a: impl AsRef<Path>,
    offset_a: usize,
    file_b: impl AsRef<Path>,
    offset_b: usize,
    chunk_size: usize,
) -> std::io::Result<usize> {
    if chunk_size == 0 {
        return Ok(0);
    }
    let data_a = fs::read(file_a)?;
    let data_b = fs::read(file_b)?;
    let mut offset_a = offset_a;
    let mut offset_b = offset_b;
    let mut result = 0_usize;

    loop {
        if offset_a >= data_a.len() || offset_b >= data_b.len() {
            return Ok(result);
        }
        let end_a = offset_a.saturating_add(chunk_size).min(data_a.len());
        let end_b = offset_b.saturating_add(chunk_size).min(data_b.len());
        if end_a - offset_a != end_b - offset_b
            || data_a[offset_a..end_a] != data_b[offset_b..end_b]
        {
            return Ok(result);
        }
        result += 1;
        offset_a = end_a;
        offset_b = end_b;
    }
}

fn normalize_legacy_gzip_header(mut data: Vec<u8>) -> Vec<u8> {
    if data.len() >= 10 && data[0] == 0x1f && data[1] == 0x8b && data[2] == 0x08 {
        data[9] = 0x0a;
    }
    data
}

#[cfg(unix)]
fn gzip_compress_with_zlib(data: &[u8]) -> std::io::Result<Vec<u8>> {
    const CHUNK: usize = 16_384;
    const Z_OK: c_int = 0;
    const Z_STREAM_END: c_int = 1;
    const Z_NO_FLUSH: c_int = 0;
    const Z_FINISH: c_int = 4;
    const Z_DEFLATED: c_int = 8;
    const Z_BEST_COMPRESSION: c_int = 9;
    const Z_DEFAULT_STRATEGY: c_int = 0;
    const MAX_WBITS: c_int = 15;
    const GZIP_ENCODING: c_int = 16;

    if data.len() > c_uint::MAX as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "gzip input is larger than zlib uInt",
        ));
    }

    let mut stream = ZStream::default();
    let init = unsafe {
        deflateInit2_(
            &mut stream,
            Z_BEST_COMPRESSION,
            Z_DEFLATED,
            MAX_WBITS | GZIP_ENCODING,
            8,
            Z_DEFAULT_STRATEGY,
            zlibVersion(),
            std::mem::size_of::<ZStream>() as c_int,
        )
    };
    if init != Z_OK {
        return Err(std::io::Error::other(format!(
            "zlib deflateInit2 failed: {init}"
        )));
    }

    stream.next_in = data.as_ptr() as *mut u8;
    stream.avail_in = data.len() as c_uint;

    let mut output = Vec::new();
    let mut out = [0_u8; CHUNK];
    loop {
        stream.next_out = out.as_mut_ptr();
        stream.avail_out = CHUNK as c_uint;
        let flush = if stream.avail_in <= CHUNK as c_uint {
            Z_FINISH
        } else {
            Z_NO_FLUSH
        };
        let ret = unsafe { deflate(&mut stream, flush) };
        let produced = stream.total_out as usize - output.len();
        output.extend_from_slice(&out[..produced]);

        if ret == Z_STREAM_END {
            break;
        }
        if ret != Z_OK {
            unsafe {
                deflateEnd(&mut stream);
            }
            return Err(std::io::Error::other(format!("zlib deflate failed: {ret}")));
        }
    }

    let end = unsafe { deflateEnd(&mut stream) };
    if end != Z_OK {
        return Err(std::io::Error::other(format!(
            "zlib deflateEnd failed: {end}"
        )));
    }

    Ok(output)
}

#[cfg(not(unix))]
fn gzip_compress_with_zlib(data: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Write;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    encoder.write_all(data)?;
    encoder.finish()
}

#[cfg(unix)]
#[repr(C)]
struct ZStream {
    next_in: *mut u8,
    avail_in: c_uint,
    total_in: c_ulong,
    next_out: *mut u8,
    avail_out: c_uint,
    total_out: c_ulong,
    msg: *mut c_char,
    state: *mut c_void,
    zalloc: Option<unsafe extern "C" fn(*mut c_void, c_uint, c_uint) -> *mut c_void>,
    zfree: Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>,
    opaque: *mut c_void,
    data_type: c_int,
    adler: c_ulong,
    reserved: c_ulong,
}

#[cfg(unix)]
impl Default for ZStream {
    fn default() -> Self {
        Self {
            next_in: std::ptr::null_mut(),
            avail_in: 0,
            total_in: 0,
            next_out: std::ptr::null_mut(),
            avail_out: 0,
            total_out: 0,
            msg: std::ptr::null_mut(),
            state: std::ptr::null_mut(),
            zalloc: None,
            zfree: None,
            opaque: std::ptr::null_mut(),
            data_type: 0,
            adler: 0,
            reserved: 0,
        }
    }
}

#[cfg(unix)]
#[link(name = "z")]
unsafe extern "C" {
    fn zlibVersion() -> *const c_char;
    fn deflateInit2_(
        stream: *mut ZStream,
        level: c_int,
        method: c_int,
        window_bits: c_int,
        mem_level: c_int,
        strategy: c_int,
        version: *const c_char,
        stream_size: c_int,
    ) -> c_int;
    fn deflate(stream: *mut ZStream, flush: c_int) -> c_int;
    fn deflateEnd(stream: *mut ZStream) -> c_int;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn resource_catalog_arrow_ipc_roundtrips_without_legacy_text() {
        let catalog = ResourceCatalog {
            version: String::from("0.1.0"),
            catalog_type: String::from("ResourceGroup"),
            total_compressed_size_bytes: Some(18),
            total_uncompressed_size_bytes: 42,
            resources: vec![
                ResourceRecord {
                    path: String::from("a.bin"),
                    location: String::from("chunks/a"),
                    size_bytes: 17,
                    compressed_size_bytes: Some(9),
                    checksum: Some(String::from("abc")),
                    binary_operation: Some(1),
                    prefix: Some(String::from("res")),
                },
                ResourceRecord {
                    path: String::from("b.bin"),
                    location: String::from("chunks/b"),
                    size_bytes: 25,
                    compressed_size_bytes: None,
                    checksum: None,
                    binary_operation: None,
                    prefix: None,
                },
            ],
        };

        let bytes =
            resource_catalog_to_arrow_ipc_bytes(&catalog).expect("catalog writes to Arrow IPC");
        assert!(!bytes.is_empty());
        let parsed =
            resource_catalog_from_arrow_ipc_bytes(&bytes).expect("catalog reads from Arrow IPC");

        assert_eq!(parsed, catalog);
    }

    #[test]
    fn resource_catalog_parquet_roundtrips_legacy_fixture_catalog() {
        let text = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupLinux.yaml",
        ))
        .expect("fixture exists");
        let catalog = parse_legacy_yaml_resource_group(&text).expect("fixture parses");

        let bytes = resource_catalog_to_parquet_bytes(&catalog).expect("catalog writes Parquet");
        assert!(!bytes.is_empty());
        let parsed = resource_catalog_from_parquet_bytes(&bytes).expect("catalog reads Parquet");

        assert_eq!(parsed, catalog);
    }

    #[test]
    fn md5_matches_legacy_string_fixture() {
        assert_eq!(md5_hex(b"Dummy"), "bcf036b6f33e182d4705f4f5b1af13ac");
    }

    #[test]
    fn md5_matches_legacy_file_fixture() {
        let path = test_data_path("resourcesOnBranch/introMovie.txt");
        let data = fs::read(path).expect("fixture exists");
        assert_eq!(md5_hex(&data), "e9fadf6f2d386a0a0786bc863f20fa34");
    }

    #[test]
    fn md5_stream_chunks_match_legacy_file_fixture() {
        let chunks =
            legacy_file_stream_read_chunks(test_data_path("resourcesOnBranch/introMovie.txt"), 50)
                .expect("fixture stream reads");
        assert!(chunks.len() > 1);
        assert_eq!(
            md5_hex_stream_chunks(chunks.iter()),
            "e9fadf6f2d386a0a0786bc863f20fa34"
        );
    }

    #[test]
    fn legacy_fnv_matches_resource_path_fixture() {
        assert_eq!(legacy_fnv_hex(b"res:/intromovie.txt"), "a9d1721dd5cc6d54");
    }

    #[test]
    fn gzip_decompresses_legacy_some_data_fixture() {
        let compressed = [
            0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x0a, 0x0b, 0xce, 0xcf, 0x4d,
            0x75, 0x49, 0x2c, 0x49, 0x04, 0x00, 0xb8, 0x70, 0x48, 0x0a, 0x08, 0x00, 0x00, 0x00,
        ];
        let decompressed = gzip_decompress(&compressed).expect("legacy gzip decompresses");
        assert_eq!(decompressed, b"SomeData");
    }

    #[test]
    fn gzip_compressed_data_has_gzip_header_and_roundtrips() {
        let compressed = gzip_compress(b"SomeData").expect("gzip compresses");
        assert_eq!(
            compressed,
            [
                0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x0a, 0x0b, 0xce, 0xcf, 0x4d,
                0x75, 0x49, 0x2c, 0x49, 0x04, 0x00, 0xb8, 0x70, 0x48, 0x0a, 0x08, 0x00, 0x00, 0x00,
            ]
        );
        assert_eq!(
            gzip_decompress(&compressed).expect("gzip decompresses"),
            b"SomeData"
        );
    }

    #[test]
    fn gzip_stream_chunks_match_legacy_header_and_roundtrip() {
        let compressed = gzip_compress_stream_chunks([b"Some".as_slice(), b"Data".as_slice()])
            .expect("gzip stream compresses");
        assert_eq!(
            compressed,
            [
                0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x0a, 0x0b, 0xce, 0xcf, 0x4d,
                0x75, 0x49, 0x2c, 0x49, 0x04, 0x00, 0xb8, 0x70, 0x48, 0x0a, 0x08, 0x00, 0x00, 0x00,
            ]
        );
        assert_eq!(
            gzip_decompress(&compressed).expect("gzip stream decompresses"),
            b"SomeData"
        );
    }

    #[test]
    fn legacy_file_stream_out_writes_fixture_bytes() {
        let output_root = fresh_test_output_dir("file-stream-out");
        let output_path = output_root.join("FileDataStreamOut.txt");
        let written = legacy_file_stream_write_chunks(
            &output_path,
            [
                b"Test1\n".as_slice(),
                b"Test2\n".as_slice(),
                b"Test3\n".as_slice(),
            ],
        )
        .expect("stream writes");
        let expected =
            fs::read(test_data_path("FileStream/FileDataStreamOut.txt")).expect("fixture reads");

        assert_eq!(written, expected.len() as u64);
        assert_eq!(fs::read(&output_path).expect("output reads"), expected);

        fs::remove_dir_all(output_root).ok();
    }

    #[test]
    fn legacy_file_stream_in_reads_expected_chunks() {
        let chunks =
            legacy_file_stream_read_chunks(test_data_path("FileStream/FileDataStreamOut.txt"), 6)
                .expect("stream reads");
        assert_eq!(
            chunks,
            vec![
                b"Test1\n".to_vec(),
                b"Test2\n".to_vec(),
                b"Test3\n".to_vec()
            ]
        );
    }

    #[test]
    fn compressed_file_stream_out_roundtrips_fixture_bytes() {
        let chunks =
            legacy_file_stream_read_chunks(test_data_path("FileStream/FileDataStreamOut.txt"), 6)
                .expect("fixture stream reads");
        let compressed = gzip_compress_stream_chunks(chunks.iter()).expect("stream compresses");
        let decompressed = gzip_decompress(&compressed).expect("stream decompresses");
        let expected =
            fs::read(test_data_path("FileStream/FileDataStreamOut.txt")).expect("fixture reads");

        assert_eq!(decompressed, expected);
    }

    #[test]
    fn gzip_streams_match_legacy_chunked_md5_path() {
        let chunks =
            legacy_file_stream_read_chunks(test_data_path("resourcesOnBranch/introMovie.txt"), 50)
                .expect("fixture stream reads");
        let original_checksum = md5_hex_stream_chunks(chunks.iter());
        let compressed = gzip_compress_stream_chunks(chunks.iter()).expect("stream compresses");
        let uncompressed = gzip_decompress(&compressed).expect("stream decompresses");

        assert_eq!(original_checksum, "e9fadf6f2d386a0a0786bc863f20fa34");
        assert_eq!(md5_hex(&uncompressed), original_checksum);
    }

    #[test]
    fn rolling_adler_matches_legacy_formula() {
        assert_eq!(
            rolling_adler_checksum(b"abcd", 0, 4),
            RollingChecksum {
                alpha: 394,
                beta: 980,
                checksum: 64_225_674,
            }
        );
    }

    #[test]
    fn find_matching_chunks_matches_entire_equal_input_like_legacy() {
        assert_eq!(
            find_matching_chunks(b"0123456789", b"0123456789"),
            vec![ChunkMatch {
                source_offset: 0,
                destination_offset: 0,
                length: 10,
            }]
        );
    }

    #[test]
    fn find_matching_chunks_handles_shorter_destination_like_legacy() {
        assert_eq!(
            find_matching_chunks(b"0123456789", b"01234"),
            vec![ChunkMatch {
                source_offset: 0,
                destination_offset: 0,
                length: 5,
            }]
        );
    }

    #[test]
    fn find_matching_chunks_handles_shorter_source_like_legacy() {
        assert_eq!(
            find_matching_chunks(b"01234", b"0123456789"),
            vec![ChunkMatch {
                source_offset: 0,
                destination_offset: 0,
                length: 5,
            }]
        );
    }

    #[test]
    fn find_matching_chunks_finds_embedded_match_like_legacy() {
        assert_eq!(
            find_matching_chunks(b"abc3456ij", b"0123456789"),
            vec![ChunkMatch {
                source_offset: 3,
                destination_offset: 3,
                length: 4,
            }]
        );
    }

    #[test]
    fn find_matching_chunk_in_file_matches_legacy_offsets() {
        let path = test_data_path("resourcesOnBranch/introMovie.txt");
        let data = fs::read(&path).expect("fixture reads");
        let early = b"introseq.blue";
        let final_chunk = &data[data.len() - 20..];

        assert_eq!(
            find_matching_chunk_in_file(b"Once upon a time, in a galaxy far, far away...", &path)
                .expect("search succeeds"),
            None
        );
        assert_eq!(
            find_matching_chunk_in_file(b"TIME", &path).expect("search succeeds"),
            Some(0)
        );
        assert_eq!(
            find_matching_chunk_in_file(early, &path).expect("search succeeds"),
            data.windows(early.len())
                .position(|candidate| candidate == early)
        );
        assert_eq!(
            find_matching_chunk_in_file(final_chunk, &path).expect("search succeeds"),
            Some(data.len() - 20)
        );
    }

    #[test]
    fn legacy_chunk_index_generates_and_finds_matching_chunks() {
        let path = test_data_path("resourcesOnBranch/introMovie.txt");
        let data = fs::read(&path).expect("fixture reads");
        let output_root = fresh_test_output_dir("chunk-index-generate");
        let index_folder = output_root.join("GenerateChunkIndex/Indexes");

        let not_in_file = b"Once upon a time, in a galaxy far, far away...";
        let mut not_in_file_index = LegacyChunkIndex::new(&path, not_in_file.len(), &index_folder);
        not_in_file_index.generate().expect("index generates");
        assert_eq!(
            not_in_file_index
                .find_matching_chunk(not_in_file)
                .expect("index searches"),
            None
        );

        let mut start_index = LegacyChunkIndex::new(&path, 4, &index_folder);
        start_index.generate().expect("index generates");
        assert_eq!(
            start_index
                .find_matching_chunk(b"TIME")
                .expect("index searches"),
            Some(0)
        );
        assert_eq!(start_index.index_files().len(), 1);
        assert!(start_index.index_files()[0].exists());

        let early = b"introseq.blue";
        let mut early_index = LegacyChunkIndex::new(&path, early.len(), &index_folder);
        early_index.generate().expect("index generates");
        assert_eq!(
            early_index
                .find_matching_chunk(early)
                .expect("index searches"),
            data.windows(early.len())
                .position(|candidate| candidate == early)
        );

        let final_chunk = &data[data.len() - 20..];
        let mut final_index = LegacyChunkIndex::new(&path, final_chunk.len(), &index_folder);
        final_index.generate().expect("index generates");
        assert_eq!(
            final_index
                .find_matching_chunk(final_chunk)
                .expect("index searches"),
            Some(data.len() - 20)
        );

        fs::remove_dir_all(output_root).ok();
    }

    #[test]
    fn legacy_chunk_index_checksum_filter_limits_index_to_target_chunks() {
        let path = test_data_path("resourcesOnBranch/introMovie.txt");
        let data = fs::read(&path).expect("fixture reads");
        let output_root = fresh_test_output_dir("chunk-index-filter");
        let index_folder = output_root.join("GenerateChunkIndex/Indexes");

        let not_in_file = b"Once upon a time, in a galaxy far, far away...";
        let mut not_in_file_index = LegacyChunkIndex::new(&path, not_in_file.len(), &index_folder);
        not_in_file_index
            .generate_checksum_filter(&path)
            .expect("filter generates");
        not_in_file_index.generate().expect("index generates");
        assert_eq!(
            not_in_file_index
                .find_matching_chunk(not_in_file)
                .expect("index searches"),
            None
        );

        let mut start_index = LegacyChunkIndex::new(&path, 4, &index_folder);
        start_index
            .generate_checksum_filter(&path)
            .expect("filter generates");
        start_index.generate().expect("index generates");
        assert_eq!(
            start_index
                .find_matching_chunk(b"TIME")
                .expect("index searches"),
            Some(0)
        );

        let early = &data[100..110];
        let mut early_index = LegacyChunkIndex::new(&path, early.len(), &index_folder);
        early_index
            .generate_checksum_filter(&path)
            .expect("filter generates");
        early_index.generate().expect("index generates");
        let early_offset = early_index
            .find_matching_chunk(early)
            .expect("index searches")
            .expect("chunk found");
        assert_eq!(&data[early_offset..early_offset + early.len()], early);

        let final_chunk = &data[data.len() - 31..data.len() - 11];
        let mut final_index = LegacyChunkIndex::new(&path, 20, &index_folder);
        final_index
            .generate_checksum_filter(&path)
            .expect("filter generates");
        final_index.generate().expect("index generates");
        assert_eq!(
            final_index
                .find_matching_chunk(final_chunk)
                .expect("index searches"),
            Some(data.len() - 31)
        );

        fs::remove_dir_all(output_root).ok();
    }

    #[test]
    fn count_matching_chunks_matches_legacy_patch_fixture_offsets() {
        let previous = test_data_path("Patch/PreviousBuildResources/introMoviePrefixed.txt");
        let next = test_data_path("Patch/NextBuildResources/introMoviePrefixed.txt");
        const CHUNK_SIZE: usize = 500;
        const PREFIX_SIZE: usize = 308;

        assert_eq!(
            count_matching_chunks(&previous, 0, &next, 0, CHUNK_SIZE).expect("count succeeds"),
            0
        );
        assert_eq!(
            count_matching_chunks(&previous, 0, &next, PREFIX_SIZE, CHUNK_SIZE)
                .expect("count succeeds"),
            19
        );
        assert_eq!(
            count_matching_chunks(
                &previous,
                CHUNK_SIZE,
                &next,
                CHUNK_SIZE + PREFIX_SIZE,
                CHUNK_SIZE,
            )
            .expect("count succeeds"),
            18
        );
    }

    #[test]
    fn parses_legacy_yaml_resource_group_fixture() {
        let text = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupLinux.yaml",
        ))
        .expect("fixture exists");
        let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");
        assert_eq!(catalog.version, "0.1.0");
        assert_eq!(catalog.catalog_type, "ResourceGroup");
        assert_eq!(catalog.len(), 3);
        assert_eq!(catalog.total_uncompressed_size_bytes, 39);
        assert_eq!(catalog.total_compressed_size_bytes, Some(99));
        assert_eq!(catalog.resources[0].path, "FileB.txt");
        assert_eq!(catalog.resources[0].binary_operation, Some(33204));
    }

    #[test]
    fn parses_legacy_empty_yaml_resource_group_fixture() {
        let text = fs::read_to_string(test_data_path("ResourceGroups/EmptyResourceGroup.yaml"))
            .expect("fixture exists");
        let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");
        assert!(catalog.is_empty());
        assert_eq!(catalog.total_uncompressed_size_bytes, 0);
        assert_eq!(catalog.total_compressed_size_bytes, Some(0));
    }

    #[test]
    fn parses_legacy_higher_minor_yaml_as_supported_version_like_cpp() {
        let text = fs::read_to_string(test_data_path("ResourceGroups/HigherMinorVersion.yaml"))
            .expect("fixture exists");
        let catalog = parse_legacy_yaml_resource_group_compat(&text).expect("yaml parses");
        assert_eq!(catalog.version, "0.1.0");
        assert!(catalog.is_empty());
    }

    #[test]
    fn rejects_legacy_higher_major_yaml_like_cpp() {
        let text = fs::read_to_string(test_data_path("ResourceGroups/HigherMajorVersion.yaml"))
            .expect("fixture exists");
        let error = parse_legacy_yaml_resource_group_compat(&text).expect_err("version rejects");
        assert_eq!(
            error.result_type,
            LegacyResourceResultType::DocumentVersionUnsupported
        );
    }

    #[test]
    fn rejects_legacy_yaml_missing_required_group_parameters_like_cpp() {
        let text = fs::read_to_string(test_data_path("ResourceGroups/EmptyResourceGroup.yaml"))
            .expect("fixture exists");
        for tag in [
            "Version",
            "Type",
            "NumberOfResources",
            "TotalResourcesSizeUnCompressed",
            "Resources",
        ] {
            let malformed = text
                .lines()
                .filter(|line| !line.starts_with(&format!("{tag}:")))
                .collect::<Vec<_>>()
                .join("\n");
            let error = parse_legacy_yaml_resource_group_compat(&malformed)
                .expect_err("missing required tag rejects");
            assert_eq!(
                error.result_type,
                LegacyResourceResultType::MalformedResourceGroup
            );
        }
    }

    #[test]
    fn rejects_invalid_yaml_with_legacy_parse_result() {
        let error = parse_legacy_yaml_resource_group_compat("Version: [")
            .expect_err("invalid yaml rejects");
        assert_eq!(
            error.result_type,
            LegacyResourceResultType::FailedToParseYaml
        );
    }

    #[test]
    fn parses_legacy_csv_resource_group_fixture() {
        let text = fs::read_to_string(test_data_path("CreateResourceFiles/ResourceGroupLinux.csv"))
            .expect("fixture exists");
        let catalog = parse_legacy_csv_resource_group(&text).expect("csv parses");
        assert_eq!(catalog.len(), 3);
        assert_eq!(catalog.total_uncompressed_size_bytes, 39);
        assert_eq!(catalog.total_compressed_size_bytes, Some(99));
        assert_eq!(catalog.resources[0].path, "FileA.txt");
        assert_eq!(catalog.resources[0].binary_operation, Some(33204));
    }

    #[test]
    fn parses_legacy_empty_csv_resource_group_fixture_like_cpp() {
        let text = fs::read_to_string(test_data_path("Indicies/resFileIndex_v0_0_0_EMPTY.txt"))
            .expect("fixture exists");
        let catalog = parse_legacy_csv_resource_group_compat(&text).expect("empty csv parses");
        assert!(catalog.is_empty());
        assert_eq!(catalog.total_uncompressed_size_bytes, 0);
        assert_eq!(catalog.total_compressed_size_bytes, Some(0));
    }

    #[test]
    fn rejects_legacy_nonsense_csv_like_cpp() {
        let text = fs::read_to_string(test_data_path("Indicies/resFileIndex_v0_0_0_NONESENSE.txt"))
            .expect("fixture exists");
        let error = parse_legacy_csv_resource_group_compat(&text).expect_err("csv rejects");
        assert_eq!(
            error.result_type,
            LegacyResourceResultType::MalformedResourceInput
        );
    }

    #[test]
    fn rejects_legacy_csv_invalid_size_field_like_cpp() {
        let text = fs::read_to_string(test_data_path("Indicies/resFileIndex_v0_0_0_INVALID.txt"))
            .expect("fixture exists");
        let error = parse_legacy_csv_resource_group_compat(&text).expect_err("csv rejects");
        assert_eq!(
            error.result_type,
            LegacyResourceResultType::MalformedResourceInput
        );
    }

    #[test]
    fn rejects_legacy_csv_out_of_bounds_binary_operation_like_cpp() {
        let text = fs::read_to_string(test_data_path(
            "Indicies/resFileIndex_v0_0_0-OutOfBoundsBinaryOp.txt",
        ))
        .expect("fixture exists");
        let error = parse_legacy_csv_resource_group_compat(&text).expect_err("csv rejects");
        assert_eq!(
            error.result_type,
            LegacyResourceResultType::MalformedResourceInput
        );
    }

    #[test]
    fn exports_legacy_yaml_resource_group_fixture_byte_for_byte() {
        let text = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupLinux.yaml",
        ))
        .expect("fixture exists");
        let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");
        assert_eq!(export_legacy_yaml_resource_group(&catalog), text);
    }

    #[test]
    fn exports_legacy_skip_compression_yaml_fixture_byte_for_byte() {
        let text = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupSkipCompressionLinux.yaml",
        ))
        .expect("fixture exists");
        let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");
        assert_eq!(catalog.total_compressed_size_bytes, None);
        assert_eq!(catalog.resources[0].compressed_size_bytes, None);
        assert_eq!(export_legacy_yaml_resource_group(&catalog), text);
    }

    #[test]
    fn exports_legacy_platform_create_group_yaml_fixtures_byte_for_byte() {
        for (fixture, expected_paths, expected_binary_operation) in [
            (
                "CreateResourceFiles/ResourceGroupLinux.yaml",
                ["FileB.txt", "FileC.txt", "FileA.txt"],
                33204,
            ),
            (
                "CreateResourceFiles/ResourceGroupMacOS.yaml",
                ["FileB.txt", "FileC.txt", "FileA.txt"],
                33188,
            ),
            (
                "CreateResourceFiles/ResourceGroupWindows.yaml",
                ["FileA.txt", "FileB.txt", "FileC.txt"],
                33206,
            ),
        ] {
            let text = fs::read_to_string(test_data_path(fixture)).expect("fixture exists");
            let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");
            let actual_paths = catalog
                .resources
                .iter()
                .map(|resource| resource.path.as_str())
                .collect::<Vec<_>>();
            let actual_binary_operations = catalog
                .resources
                .iter()
                .map(|resource| resource.binary_operation)
                .collect::<Vec<_>>();

            assert_eq!(actual_paths, expected_paths, "{fixture}");
            assert_eq!(
                actual_binary_operations,
                vec![Some(expected_binary_operation); expected_paths.len()],
                "{fixture}"
            );
            assert_eq!(
                export_legacy_yaml_resource_group(&catalog),
                text,
                "{fixture}"
            );
        }
    }

    #[test]
    fn exports_legacy_platform_skip_compression_yaml_fixtures_byte_for_byte() {
        for (fixture, expected_binary_operation) in [
            (
                "CreateResourceFiles/ResourceGroupSkipCompressionLinux.yaml",
                33204,
            ),
            (
                "CreateResourceFiles/ResourceGroupSkipCompressionMacOS.yaml",
                33188,
            ),
            (
                "CreateResourceFiles/ResourceGroupSkipCompressionWindows.yaml",
                33206,
            ),
        ] {
            let text = fs::read_to_string(test_data_path(fixture)).expect("fixture exists");
            let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");

            assert_eq!(catalog.total_compressed_size_bytes, None, "{fixture}");
            assert!(catalog
                .resources
                .iter()
                .all(|resource| resource.compressed_size_bytes.is_none()));
            assert!(catalog
                .resources
                .iter()
                .all(|resource| { resource.binary_operation == Some(expected_binary_operation) }));
            assert_eq!(
                export_legacy_yaml_resource_group(&catalog),
                text,
                "{fixture}"
            );
        }
    }

    #[test]
    fn exports_legacy_platform_create_group_csv_fixtures_byte_for_byte() {
        for (fixture, expected_first_path, expected_binary_operation) in [
            (
                "CreateResourceFiles/ResourceGroupLinux.csv",
                "FileA.txt",
                33204,
            ),
            (
                "CreateResourceFiles/ResourceGroupMacOS.csv",
                "FileA.txt",
                33188,
            ),
            (
                "CreateResourceFiles/ResourceGroupWindows.csv",
                "FileA.txt",
                33206,
            ),
            (
                "CreateResourceFiles/ResourceGroupLinuxPrefixed.csv",
                "FileA.txt",
                33204,
            ),
            (
                "CreateResourceFiles/ResourceGroupMacOSPrefixed.csv",
                "FileA.txt",
                33188,
            ),
            (
                "CreateResourceFiles/ResourceGroupWindowsPrefixed.csv",
                "FileA.txt",
                33206,
            ),
        ] {
            let text = fs::read_to_string(test_data_path(fixture)).expect("fixture exists");
            let catalog = parse_legacy_csv_resource_group(&text).expect("csv parses");

            assert_eq!(catalog.resources[0].path, expected_first_path, "{fixture}");
            assert!(catalog
                .resources
                .iter()
                .all(|resource| { resource.binary_operation == Some(expected_binary_operation) }));
            assert_eq!(
                export_legacy_csv_resource_group(&catalog),
                text,
                "{fixture}"
            );
        }
    }

    #[test]
    fn exports_legacy_empty_yaml_resource_group_fixture_byte_for_byte() {
        let text = fs::read_to_string(test_data_path("ResourceGroups/EmptyResourceGroup.yaml"))
            .expect("fixture exists");
        let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");
        assert_eq!(export_legacy_yaml_resource_group(&catalog), text);
    }

    #[test]
    fn exports_legacy_csv_resource_group_fixture_byte_for_byte() {
        let text = fs::read_to_string(test_data_path("CreateResourceFiles/ResourceGroupLinux.csv"))
            .expect("fixture exists");
        let catalog = parse_legacy_csv_resource_group(&text).expect("csv parses");
        assert_eq!(export_legacy_csv_resource_group(&catalog), text);
    }

    #[test]
    fn exports_legacy_indicies_v0_resource_csv_as_v1_yaml_byte_for_byte() {
        assert_legacy_indicies_v0_csv_as_v1_yaml(
            "Indicies/resFileIndex_v0_0_0.txt",
            "Indicies/ResourceGroup_v0_1_0.yaml",
        );
    }

    #[test]
    fn exports_legacy_indicies_v0_binary_csv_as_v1_yaml_byte_for_byte() {
        assert_legacy_indicies_v0_csv_as_v1_yaml(
            "Indicies/binaryFileIndex_v0_0_0.txt",
            "Indicies/BinaryResourceGroup_v0_1_0.yaml",
        );
    }

    #[test]
    fn exports_legacy_indicies_yaml_fixtures_byte_for_byte() {
        for fixture in [
            "Indicies/ResourceGroup_v0_1_0.yaml",
            "Indicies/BinaryResourceGroup_v0_1_0.yaml",
        ] {
            let text = fs::read_to_string(test_data_path(fixture)).expect("fixture exists");
            let catalog = parse_legacy_yaml_resource_group(&text).expect("yaml parses");
            assert_eq!(export_legacy_yaml_resource_group(&catalog), text);
        }
    }

    #[test]
    fn parses_and_exports_legacy_create_bundle_yaml_byte_for_byte() {
        assert_bundle_yaml_fixture(
            "CreateBundle/BundleResourceGroup.yaml",
            3,
            "CreateBundleOut",
        );
    }

    #[test]
    fn parses_and_exports_legacy_create_bundle_remote_cdn_yaml_byte_for_byte() {
        assert_bundle_yaml_fixture(
            "CreateBundle/BundleResourceGroupRemoteCDN.yaml",
            1,
            "CreateBundleOutRemoteCDN",
        );
    }

    #[test]
    fn creates_legacy_local_bundle_fixture_bytes() {
        let source = parse_legacy_csv_fixture("Bundle/resFileIndexShort.txt");
        let bundle = create_legacy_local_bundle_from_resource_group(
            &source,
            test_data_path("Bundle/Res"),
            "ResourceGroup.yaml",
            "CreateBundleOut",
            1000,
            10_000_000,
        )
        .expect("local bundle is created");

        let expected_manifest =
            fs::read_to_string(test_data_path("CreateBundle/BundleResourceGroup.yaml"))
                .expect("fixture exists");
        assert_eq!(
            export_legacy_yaml_bundle_resource_group(&bundle.catalog),
            expected_manifest
        );

        assert_bundle_data_matches_fixture(
            &bundle.resource_group_resource,
            "CreateBundle/CreateBundleOut",
        );
        assert_eq!(bundle.chunks.len(), 3);
        for chunk in &bundle.chunks {
            assert_bundle_data_matches_fixture(chunk, "CreateBundle/CreateBundleOut");
        }
    }

    #[test]
    fn resource_chunking_many_files_into_many_uncompressed_chunks_reconstructs() {
        let (source, resources) = bundle_stream_test_resource_catalog();
        let bundle = create_legacy_local_bundle_from_resource_group(
            &source,
            test_data_path("Bundle/TestResources"),
            "ResourceGroup.yaml",
            "ResourceChunking",
            1000,
            1000,
        )
        .expect("local bundle stream is created");

        assert!(
            bundle.chunks.len() > 1,
            "uncompressed stream should split into multiple chunks"
        );
        assert!(bundle
            .chunks
            .iter()
            .all(|chunk| chunk.record.resource_type == "BinaryChunk"));
        assert_reconstructed_bundle_stream_matches_resources(
            &bundle
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>(),
            &resources,
        );
    }

    #[test]
    fn resource_chunking_many_files_into_single_compressed_chunk_reconstructs() {
        let (source, resources) = bundle_stream_test_resource_catalog();
        let bundle = create_legacy_remote_cdn_bundle_from_resource_group(
            &source,
            test_data_path("Bundle/TestResources"),
            "ResourceGroup.yaml",
            "ResourceChunking",
            1000,
            1000,
        )
        .expect("remote CDN bundle stream is created");

        assert_eq!(bundle.chunks.len(), 1);
        assert_eq!(bundle.chunks[0].record.resource_type, "BinaryChunk");
        assert!(bundle.chunks[0].record.compressed_size_bytes.is_some());
        let decompressed =
            gzip_decompress(&bundle.chunks[0].data).expect("bundle chunk decompresses");
        assert_reconstructed_bundle_stream_matches_resources(&decompressed, &resources);
    }

    #[test]
    fn creates_legacy_remote_cdn_bundle_fixture_bytes() {
        let source = parse_legacy_csv_fixture("Bundle/resFileIndexShort.txt");
        let bundle = create_legacy_remote_cdn_bundle_from_resource_group(
            &source,
            test_data_path("Bundle/Res"),
            "ResourceGroup.yaml",
            "CreateBundleOutRemoteCDN",
            1000,
            10_000_000,
        )
        .expect("remote CDN bundle is created");

        let expected_manifest = fs::read_to_string(test_data_path(
            "CreateBundle/BundleResourceGroupRemoteCDN.yaml",
        ))
        .expect("fixture exists");
        assert_eq!(
            export_legacy_yaml_bundle_resource_group(&bundle.catalog),
            expected_manifest
        );

        assert_bundle_data_matches_fixture(
            &bundle.resource_group_resource,
            "CreateBundle/CreateBundleOutRemoteCDN",
        );
        assert_eq!(bundle.chunks.len(), 1);
        assert_bundle_data_matches_fixture(
            &bundle.chunks[0],
            "CreateBundle/CreateBundleOutRemoteCDN",
        );
    }

    #[test]
    fn parses_and_exports_legacy_unpack_bundle_yaml_byte_for_byte() {
        assert_bundle_yaml_fixture("Bundle/BundleResourceGroup.yaml", 42, "CreateBundleOut");
    }

    #[test]
    fn unpacks_legacy_local_bundle_fixture_bytes() {
        let text = fs::read_to_string(test_data_path("Bundle/BundleResourceGroup.yaml"))
            .expect("fixture exists");
        let catalog = parse_legacy_yaml_bundle_resource_group(&text).expect("bundle parses");
        let unpacked = unpack_legacy_local_bundle_from_cdn(
            &catalog,
            test_data_path("Bundle/LocalRemoteChunks"),
        )
        .expect("bundle unpacks");

        assert_eq!(unpacked.resources.len(), 3);
        let exported_group = export_legacy_yaml_resource_group(&unpacked.resource_catalog);
        assert_eq!(
            exported_group.as_bytes(),
            unpacked.resource_group_resource.data.as_slice()
        );

        let expected_root = test_data_path("Bundle/Res");
        for resource in &unpacked.resources {
            let expected_path =
                resolve_existing_case_insensitive_path(&expected_root.join(&resource.path))
                    .expect("fixture resource exists");
            let expected = fs::read(expected_path).expect("fixture resource reads");
            assert_eq!(resource.data, expected, "resource {}", resource.path);
        }
    }

    #[test]
    fn unpacks_legacy_compressed_remote_cdn_bundle_fixture_bytes() {
        let catalog = parse_bundle_fixture("CreateBundle/BundleResourceGroupRemoteCDN.yaml");
        let cache_root = fresh_test_output_dir("remote-cdn-success-cache");
        let unpacked = unpack_legacy_remote_bundle_from_local_mirror(
            &catalog,
            test_data_path("CreateBundle/CreateBundleOutRemoteCDN"),
            &cache_root,
        )
        .expect("compressed remote CDN bundle unpacks")
        .unpacked;

        assert_eq!(unpacked.resources.len(), 3);
        let exported_group = export_legacy_yaml_resource_group(&unpacked.resource_catalog);
        assert_eq!(
            exported_group.as_bytes(),
            unpacked.resource_group_resource.data.as_slice()
        );

        let expected_root = test_data_path("Bundle/Res");
        for resource in &unpacked.resources {
            let expected_path =
                resolve_existing_case_insensitive_path(&expected_root.join(&resource.path))
                    .expect("fixture resource exists");
            let expected = fs::read(expected_path).expect("fixture resource reads");
            assert_eq!(resource.data, expected, "resource {}", resource.path);
        }
    }

    #[test]
    fn remote_cdn_unpack_rejects_uncompressed_local_chunks() {
        let catalog = parse_bundle_fixture("Bundle/BundleResourceGroup.yaml");
        let cache_root = fresh_test_output_dir("remote-cdn-local-source-rejection-cache");

        let error = unpack_legacy_remote_bundle_from_local_mirror(
            &catalog,
            test_data_path("Bundle/LocalRemoteChunks"),
            &cache_root,
        )
        .expect_err("remote CDN unpack rejects uncompressed local chunks");

        assert!(
            error.contains("remote CDN compressed size mismatch")
                || error.contains("not a valid gzip payload"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn local_cdn_unpack_rejects_compressed_remote_chunks() {
        let catalog = parse_bundle_fixture("CreateBundle/BundleResourceGroupRemoteCDN.yaml");

        let error = unpack_legacy_local_bundle_from_cdn(
            &catalog,
            test_data_path("CreateBundle/CreateBundleOutRemoteCDN"),
        )
        .expect_err("local CDN unpack rejects compressed remote chunks");

        assert!(
            error.contains("resource checksum mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn remote_cdn_bundle_unpack_uses_local_mirror_and_cache_without_network() {
        let catalog = parse_bundle_fixture("CreateBundle/BundleResourceGroupRemoteCDN.yaml");
        let cache_root = fresh_test_output_dir("remote-cdn-cache");

        let first = unpack_legacy_remote_bundle_from_local_mirror(
            &catalog,
            test_data_path("CreateBundle/CreateBundleOutRemoteCDN"),
            &cache_root,
        )
        .expect("remote CDN mirror bundle unpacks");
        assert_eq!(first.cache_stats.downloads, 2);
        assert_eq!(first.cache_stats.cache_hits, 0);
        assert_eq!(first.cache_stats.replaced_bad_cache_entries, 0);
        assert!(first.cache_stats.bytes_copied_to_cache > 0);
        assert_unpacked_bundle_matches_expected_resources(&first.unpacked);

        let second = unpack_legacy_remote_bundle_from_local_mirror(
            &catalog,
            test_data_path("CreateBundle/CreateBundleOutRemoteCDN"),
            &cache_root,
        )
        .expect("remote CDN mirror bundle unpacks from cache");
        assert_eq!(second.cache_stats.downloads, 0);
        assert_eq!(second.cache_stats.cache_hits, 2);
        assert_eq!(second.cache_stats.replaced_bad_cache_entries, 0);
        assert_eq!(second.cache_stats.bytes_copied_to_cache, 0);
        assert_unpacked_bundle_matches_expected_resources(&second.unpacked);

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn remote_cdn_bundle_unpack_replaces_bad_cached_payload() {
        let catalog = parse_bundle_fixture("CreateBundle/BundleResourceGroupRemoteCDN.yaml");
        let cache_root = fresh_test_output_dir("remote-cdn-bad-cache");
        let bad_cache_path = cache_root.join(catalog.resources[0].location.replace('\\', "/"));
        fs::create_dir_all(bad_cache_path.parent().expect("cache file has parent"))
            .expect("cache dir is created");
        fs::write(&bad_cache_path, b"bad cache payload").expect("bad cache payload is written");

        let unpacked = unpack_legacy_remote_bundle_from_local_mirror(
            &catalog,
            test_data_path("CreateBundle/CreateBundleOutRemoteCDN"),
            &cache_root,
        )
        .expect("remote CDN mirror bundle replaces bad cache");
        assert_eq!(unpacked.cache_stats.downloads, 2);
        assert_eq!(unpacked.cache_stats.cache_hits, 0);
        assert_eq!(unpacked.cache_stats.replaced_bad_cache_entries, 1);
        assert_unpacked_bundle_matches_expected_resources(&unpacked.unpacked);

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn remote_cdn_bundle_unpack_reports_checksum_mismatch() {
        let catalog = parse_bundle_fixture("CreateBundle/BundleResourceGroupRemoteCDN.yaml");
        let mirror_root = fresh_test_output_dir("remote-cdn-corrupt-mirror");
        copy_directory_recursive(
            &test_data_path("CreateBundle/CreateBundleOutRemoteCDN"),
            &mirror_root,
        );
        let corrupt_chunk_path = mirror_root.join(catalog.resources[0].location.replace('\\', "/"));
        fs::write(&corrupt_chunk_path, b"corrupt remote payload")
            .expect("corrupt remote payload is written");

        let cache_root = fresh_test_output_dir("remote-cdn-corrupt-cache");
        let error =
            unpack_legacy_remote_bundle_from_local_mirror(&catalog, &mirror_root, &cache_root)
                .expect_err("corrupt mirror payload is rejected");
        assert!(
            error.contains("remote CDN compressed size mismatch")
                || error.contains("not a valid gzip payload")
                || error.contains("resource checksum mismatch"),
            "unexpected error: {error}"
        );

        fs::remove_dir_all(mirror_root).ok();
        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn parses_and_exports_legacy_create_patch_yaml_byte_for_byte() {
        assert_patch_yaml_fixture("Patch/PatchResourceGroup.yaml", 2, 50_000_000, Some(1));
    }

    #[test]
    fn reads_legacy_local_patch_fixture_bytes() {
        let text = fs::read_to_string(test_data_path("Patch/PatchResourceGroup.yaml"))
            .expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        let data_set =
            read_legacy_local_patch_data(&catalog, test_data_path("Patch/LocalCDNPatches"))
                .expect("patch data reads");

        assert_eq!(
            data_set.resource_group_resource.resource_type,
            "ResourceGroup"
        );
        assert_eq!(
            data_set.resource_group_resource.data.len() as u64,
            catalog.resource_group_resource.size_bytes
        );
        assert_eq!(data_set.resources.len(), 2);
        for resource in &data_set.resources {
            let record = catalog
                .resources
                .iter()
                .find(|record| record.path == resource.path)
                .expect("resource came from catalog");
            assert_eq!(resource.data.len() as u64, record.size_bytes);
            assert_eq!(md5_hex(&resource.data), record.checksum);
        }
    }

    #[test]
    fn rejects_legacy_local_patch_payload_checksum_mismatch() {
        let root = fresh_test_output_dir("corrupt-local-patch-payload");
        let patch_root = root.join("LocalCDNPatches");
        copy_directory_recursive(&test_data_path("Patch/LocalCDNPatches"), &patch_root);
        let text = fs::read_to_string(test_data_path("Patch/PatchResourceGroup.yaml"))
            .expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        let corrupt_location = catalog
            .resources
            .iter()
            .find(|resource| !resource.location.is_empty())
            .expect("fixture has patch payload")
            .location
            .replace('\\', "/");
        fs::write(patch_root.join(corrupt_location), b"corrupt patch payload")
            .expect("patch payload is corrupted");

        let error =
            read_legacy_local_patch_data(&catalog, &patch_root).expect_err("corrupt patch fails");
        assert!(
            error.contains("resource checksum mismatch"),
            "unexpected error: {error}"
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_legacy_binary_patch_corruption_variants() {
        let error = apply_legacy_binary_patch(b"old", b"short")
            .expect_err("short patch payload is rejected");
        assert!(error.contains("too short"), "{error}");

        let mut invalid_header = Vec::from(&b"NOT-ENDSLEY-PATCH"[..]);
        invalid_header.extend_from_slice(&0_u64.to_le_bytes());
        let error = apply_legacy_binary_patch(b"old", &invalid_header)
            .expect_err("invalid patch header is rejected");
        assert!(error.contains("unexpected header"), "{error}");

        let mut truncated_stream = Vec::from(LEGACY_BSDIFF_HEADER.as_slice());
        truncated_stream.extend_from_slice(&4_u64.to_le_bytes());
        let error = apply_legacy_binary_patch(b"old", &truncated_stream)
            .expect_err("truncated bsdiff stream is rejected");
        assert!(error.contains("target length mismatch"), "{error}");

        let previous = b"old-data";
        let latest = b"new-data-and-more";
        let mut wrong_target_len =
            create_legacy_binary_patch(previous, latest).expect("valid legacy patch is created");
        wrong_target_len[LEGACY_BSDIFF_HEADER.len()..LEGACY_BSDIFF_HEADER_SIZE]
            .copy_from_slice(&(latest.len() as u64 + 1).to_le_bytes());
        let error = apply_legacy_binary_patch(previous, &wrong_target_len)
            .expect_err("declared target length mismatch is rejected");
        assert!(error.contains("target length mismatch"), "{error}");
    }

    #[test]
    fn applies_legacy_local_patch_fixture_bytes() {
        let text = fs::read_to_string(test_data_path("Patch/PatchResourceGroup.yaml"))
            .expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        let applied = apply_legacy_local_patch_from_directories(
            &catalog,
            test_data_path("Patch/PreviousBuildResources"),
            test_data_path("Patch/NextBuildResources"),
            test_data_path("Patch/LocalCDNPatches"),
        )
        .expect("patch applies");

        assert_eq!(applied.resources.len(), 3);
        assert_eq!(
            export_legacy_yaml_resource_group(&applied.resource_catalog).as_bytes(),
            applied.resource_group_resource.data.as_slice()
        );
        assert_applied_patch_resources_match_next_build(&applied, "Patch/NextBuildResources");
    }

    #[test]
    fn creates_legacy_local_patch_payloads_byte_for_byte() {
        assert_legacy_local_patch_payloads_byte_for_byte(
            "Patch/PatchResourceGroup.yaml",
            "Patch/PreviousBuildResources",
            "Patch/NextBuildResources",
            "Patch/LocalCDNPatches",
            2,
            0,
        );
    }

    #[test]
    fn creates_legacy_local_patch_manifest_and_payloads_byte_for_byte() {
        let previous_text =
            fs::read_to_string(test_data_path("Patch/resFileIndexShort_build_previous.txt"))
                .expect("previous resource group fixture exists");
        let next_text =
            fs::read_to_string(test_data_path("Patch/resFileIndexShort_build_next.txt"))
                .expect("next resource group fixture exists");
        let previous_catalog =
            parse_legacy_csv_resource_group(&previous_text).expect("previous catalog parses");
        let next_catalog =
            parse_legacy_csv_resource_group(&next_text).expect("next catalog parses");
        let created = create_legacy_local_patch_from_resource_groups(
            &previous_catalog,
            &next_catalog,
            test_data_path("Patch/PreviousBuildResources"),
            test_data_path("Patch/NextBuildResources"),
            50_000_000,
        )
        .expect("patch is created");

        let expected_manifest = fs::read_to_string(test_data_path("Patch/PatchResourceGroup.yaml"))
            .expect("expected patch manifest exists");
        assert_eq!(
            export_legacy_yaml_patch_resource_group(&created.catalog),
            expected_manifest
        );

        let expected_patch_root = test_data_path("Patch/LocalCDNPatches");
        let expected_group = fs::read(
            expected_patch_root.join(created.resource_group_resource.location.replace('\\', "/")),
        )
        .expect("expected resource group payload exists");
        assert_eq!(created.resource_group_resource.data, expected_group);
        for resource in &created.resources {
            let expected = fs::read(expected_patch_root.join(resource.location.replace('\\', "/")))
                .expect("expected patch payload exists");
            assert_eq!(resource.data, expected, "patch payload {}", resource.path);
        }
    }

    #[test]
    fn creates_legacy_no_change_local_patch_with_empty_patch_list() {
        let previous_text =
            fs::read_to_string(test_data_path("Patch/resFileIndexShort_build_previous.txt"))
                .expect("previous resource group fixture exists");
        let previous_catalog =
            parse_legacy_csv_resource_group(&previous_text).expect("previous catalog parses");
        let created = create_legacy_local_patch_from_resource_groups(
            &previous_catalog,
            &previous_catalog,
            test_data_path("Patch/PreviousBuildResources"),
            test_data_path("Patch/PreviousBuildResources"),
            50_000_000,
        )
        .expect("no-change patch is created");

        let expected_resource_group = ResourceCatalog {
            version: String::from("0.1.0"),
            catalog_type: String::from("ResourceGroup"),
            total_compressed_size_bytes: Some(0),
            total_uncompressed_size_bytes: 0,
            resources: Vec::new(),
        };
        assert_eq!(created.catalog.resources, Vec::new());
        assert_eq!(created.catalog.removed_resource_relative_paths, None);
        assert_eq!(created.catalog.total_compressed_size_bytes, Some(0));
        assert_eq!(created.catalog.total_uncompressed_size_bytes, 0);
        assert_eq!(created.resources, Vec::new());
        assert_eq!(
            created.resource_group_resource.data,
            export_legacy_yaml_resource_group(&expected_resource_group).into_bytes()
        );
        assert_eq!(
            created.catalog.resource_group_resource.size_bytes,
            created.resource_group_resource.data.len() as u64
        );
        assert_eq!(
            created.catalog.resource_group_resource.checksum,
            md5_hex(&created.resource_group_resource.data)
        );
    }

    #[test]
    fn parses_and_exports_legacy_create_patch_chunked_yaml_byte_for_byte() {
        let catalog = assert_patch_yaml_fixture(
            "PatchWithInputChunk/PatchResourceGroup_previousBuild_latestBuild.yaml",
            12,
            500,
            Some(1),
        );
        assert!(catalog
            .resources
            .iter()
            .any(|resource| resource.compressed_size_bytes.is_none()));
        assert!(catalog
            .resources
            .iter()
            .any(|resource| resource.location.is_empty()));
    }

    #[test]
    fn reads_legacy_chunked_local_patch_fixture_bytes() {
        let text = fs::read_to_string(test_data_path(
            "PatchWithInputChunk/PatchResourceGroup_previousBuild_latestBuild.yaml",
        ))
        .expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        let data_set = read_legacy_local_patch_data(
            &catalog,
            test_data_path("PatchWithInputChunk/LocalCDNPatches"),
        )
        .expect("patch data reads");

        assert_eq!(
            data_set.resource_group_resource.resource_type,
            "ResourceGroup"
        );
        let non_empty_locations = catalog
            .resources
            .iter()
            .filter(|resource| !resource.location.is_empty())
            .count();
        assert_eq!(data_set.resources.len(), non_empty_locations);
        for resource in &data_set.resources {
            let record = catalog
                .resources
                .iter()
                .find(|record| record.path == resource.path)
                .expect("resource came from catalog");
            assert_eq!(resource.data.len() as u64, record.size_bytes);
            assert_eq!(md5_hex(&resource.data), record.checksum);
        }
    }

    #[test]
    fn applies_legacy_chunked_local_patch_fixture_bytes() {
        let text = fs::read_to_string(test_data_path(
            "PatchWithInputChunk/PatchResourceGroup_previousBuild_latestBuild.yaml",
        ))
        .expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        let applied = apply_legacy_local_patch_from_directories(
            &catalog,
            test_data_path("PatchWithInputChunk/PreviousBuildResources"),
            test_data_path("PatchWithInputChunk/NextBuildResources"),
            test_data_path("PatchWithInputChunk/LocalCDNPatches"),
        )
        .expect("chunked patch applies");

        assert_eq!(applied.resources.len(), 4);
        assert_eq!(
            export_legacy_yaml_resource_group(&applied.resource_catalog).as_bytes(),
            applied.resource_group_resource.data.as_slice()
        );
        assert_applied_patch_resources_match_next_build(
            &applied,
            "PatchWithInputChunk/NextBuildResources",
        );
    }

    #[test]
    fn creates_legacy_chunked_local_patch_payloads_byte_for_byte() {
        assert_legacy_local_patch_payloads_byte_for_byte(
            "PatchWithInputChunk/PatchResourceGroup_previousBuild_latestBuild.yaml",
            "PatchWithInputChunk/PreviousBuildResources",
            "PatchWithInputChunk/NextBuildResources",
            "PatchWithInputChunk/LocalCDNPatches",
            6,
            6,
        );
    }

    #[test]
    fn creates_and_applies_copy_only_local_patch_without_binary_payloads() {
        let root = fresh_test_output_dir("copy-only-patch");
        let previous_root = root.join("previous");
        let next_root = root.join("next");
        let patch_root = root.join("patches");
        fs::create_dir_all(&previous_root).expect("previous dir is created");
        fs::create_dir_all(&next_root).expect("next dir is created");

        let previous = b"AAAABBBBCCCCDDDD".to_vec();
        let latest = b"BBBBAAAADDDDCCCC".to_vec();
        fs::write(previous_root.join("copy.dat"), &previous).expect("previous file is written");
        fs::write(next_root.join("copy.dat"), &latest).expect("next file is written");

        let previous_catalog = single_resource_catalog("copy.dat", &previous);
        let next_catalog = single_resource_catalog("copy.dat", &latest);
        let created = create_legacy_local_patch_from_resource_groups(
            &previous_catalog,
            &next_catalog,
            &previous_root,
            &next_root,
            4,
        )
        .expect("copy-only patch is created");

        assert_eq!(created.resources.len(), 0);
        assert_eq!(created.catalog.resources.len(), 4);
        assert!(created.catalog.resources.iter().all(|record| {
            record.location.is_empty()
                && record.compressed_size_bytes.is_none()
                && record.checksum == md5_hex(&[])
        }));
        assert_eq!(created.catalog.total_compressed_size_bytes, Some(0));
        assert_eq!(
            created.catalog.total_uncompressed_size_bytes,
            latest.len() as u64
        );

        write_test_patch_payload(&patch_root, &created.resource_group_resource);
        let applied = apply_legacy_local_patch_from_directories(
            &created.catalog,
            &previous_root,
            &next_root,
            &patch_root,
        )
        .expect("copy-only patch applies");
        assert_eq!(applied.resources.len(), 1);
        assert_eq!(applied.resources[0].path, "copy.dat");
        assert_eq!(applied.resources[0].data, latest);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_malformed_legacy_patch_apply_manifests() {
        fn copy_only_patch_record(
            path: &str,
            data_offset: u64,
            source_offset: u64,
            size_bytes: u64,
        ) -> PatchResourceRecord {
            PatchResourceRecord {
                path: String::from(path),
                resource_type: String::from("BinaryPatch"),
                location: String::new(),
                size_bytes,
                compressed_size_bytes: None,
                checksum: md5_hex(&[]),
                target_resource_relative_path: String::from("copy.dat"),
                data_offset,
                source_offset,
            }
        }

        let root = fresh_test_output_dir("malformed-patch-apply-manifests");
        let previous_root = root.join("previous");
        let next_root = root.join("next");
        let patch_root = root.join("patches");
        fs::create_dir_all(&previous_root).expect("previous dir is created");
        fs::create_dir_all(&next_root).expect("next dir is created");
        fs::create_dir_all(&patch_root).expect("patch dir is created");

        let previous = b"AAAABBBBCCCC".to_vec();
        let latest = b"BBBBAAAA".to_vec();
        fs::write(previous_root.join("copy.dat"), &previous).expect("previous file is written");
        fs::write(next_root.join("copy.dat"), &latest).expect("next file is written");

        let resource_group_data =
            export_legacy_yaml_resource_group(&single_resource_catalog("copy.dat", &latest))
                .into_bytes();
        let resource_group_location = String::from("ResourceGroup.yaml");
        fs::write(
            patch_root.join(&resource_group_location),
            &resource_group_data,
        )
        .expect("patch resource group payload is written");
        let resource_group_resource = PatchResourceGroupRecord {
            path: String::from("ResourceGroup.yaml"),
            resource_type: String::from("ResourceGroup"),
            location: resource_group_location,
            size_bytes: resource_group_data.len() as u64,
            compressed_size_bytes: None,
            checksum: md5_hex(&resource_group_data),
        };

        let cases = [
            (
                "zero max input chunk size",
                0,
                vec![copy_only_patch_record("Patches/Patch.0", 0, 0, 4)],
                "invalid max input chunk size: 0",
            ),
            (
                "data offset exceeds target size",
                4,
                vec![copy_only_patch_record("Patches/Patch.0", 99, 0, 4)],
                "patch data offset for Patches/Patch.0 exceeds expected size of copy.dat",
            ),
            (
                "copy source range exceeds previous data",
                4,
                vec![copy_only_patch_record("Patches/Patch.0", 0, 10, 4)],
                "source range for copy.dat exceeds previous data",
            ),
            (
                "overlapping copy ranges",
                4,
                vec![
                    copy_only_patch_record("Patches/Patch.0", 0, 0, 4),
                    copy_only_patch_record("Patches/Patch.1", 2, 4, 2),
                ],
                "patch Patches/Patch.1 overlaps earlier output for copy.dat",
            ),
        ];

        for (name, max_input_chunk_size, resources, expected_error) in cases {
            let catalog = PatchResourceCatalog {
                version: String::from("0.1.0"),
                catalog_type: String::from("PatchGroup"),
                total_compressed_size_bytes: Some(0),
                total_uncompressed_size_bytes: resources
                    .iter()
                    .map(|resource| resource.size_bytes)
                    .sum(),
                resource_group_resource: resource_group_resource.clone(),
                max_input_chunk_size,
                removed_resource_relative_paths: None,
                resources,
            };

            let error = apply_legacy_local_patch_from_directories(
                &catalog,
                &previous_root,
                &next_root,
                &patch_root,
            )
            .expect_err(name);
            assert!(
                error.contains(expected_error),
                "{name}: expected {expected_error:?}, got {error:?}"
            );
        }

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn parses_and_exports_legacy_old_patch_yaml_byte_for_byte() {
        assert_patch_yaml_fixture(
            "Patch/Old/PatchResourceGroup_previousBuild_latestBuild.yaml",
            2,
            100_000_000,
            None,
        );
    }

    #[test]
    fn applies_legacy_old_local_patch_fixture_bytes() {
        let text = fs::read_to_string(test_data_path(
            "Patch/Old/PatchResourceGroup_previousBuild_latestBuild.yaml",
        ))
        .expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        assert_eq!(catalog.removed_resource_relative_paths, None);
        let applied = apply_legacy_local_patch_from_directories(
            &catalog,
            test_data_path("Patch/PreviousBuildResources"),
            test_data_path("Patch/NextBuildResources"),
            test_data_path("Patch/Old/LocalCDNPatches"),
        )
        .expect("old-layout patch applies");

        assert_eq!(applied.resources.len(), 3);
        assert_eq!(
            export_legacy_yaml_resource_group(&applied.resource_catalog).as_bytes(),
            applied.resource_group_resource.data.as_slice()
        );
        assert_applied_patch_resources_match_next_build(&applied, "Patch/NextBuildResources");
    }

    #[test]
    fn creates_legacy_old_local_patch_payloads_byte_for_byte() {
        assert_legacy_local_patch_payloads_byte_for_byte(
            "Patch/Old/PatchResourceGroup_previousBuild_latestBuild.yaml",
            "Patch/PreviousBuildResources",
            "Patch/NextBuildResources",
            "Patch/Old/LocalCDNPatches",
            2,
            0,
        );
    }

    #[test]
    fn parses_legacy_filter_index_mapping_fixture() {
        let text = fs::read_to_string(test_data_path("FilterFiles/resFilterIndexMapping.yaml"))
            .expect("fixture exists");
        let mappings = parse_legacy_filter_index_mapping_yaml(&text).expect("mapping parses");
        assert_eq!(mappings.len(), 1);
        assert_eq!(
            mappings[0].filter_file_paths,
            vec![String::from("filterToIncludeAllAtBaseDirectory.ini")]
        );
        assert_eq!(mappings[0].output_index_filename, "ResourceGroup.yaml");
    }

    #[test]
    fn creates_legacy_resource_group_from_filter_file_byte_for_byte() {
        let expected = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupLinux.yaml",
        ))
        .expect("fixture exists");
        let filter_file = test_data_path("FilterFiles/filterToIncludeAllAtBaseDirectory.ini");
        let catalog = create_legacy_resource_group_from_filter_files(
            test_data_path("CreateResourceFiles/ResourceFiles"),
            &[filter_file],
            true,
        )
        .expect("filtered ResourceGroup is created");
        assert_eq!(export_legacy_yaml_resource_group(&catalog), expected);
    }

    #[test]
    fn exports_legacy_local_relative_resources_from_filter_catalog() {
        let source_root = test_data_path("CreateResourceFiles/ResourceFiles");
        let filter_file = test_data_path("FilterFiles/filterToIncludeAllAtBaseDirectory.ini");
        let catalog =
            create_legacy_resource_group_from_filter_files(&source_root, &[filter_file], true)
                .expect("filtered ResourceGroup is created");
        let output_root = fresh_test_output_dir("export-filter-catalog");
        let (count, bytes) =
            export_legacy_local_relative_resources(&catalog, &source_root, &output_root)
                .expect("resources export");

        assert_eq!(count, 3);
        assert!(bytes > 0);
        assert_directory_subset(&output_root, &source_root);
        fs::remove_dir_all(output_root).ok();
    }

    #[test]
    fn creates_legacy_resource_groups_from_filter_mapping_byte_for_byte() {
        let expected = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupLinux.yaml",
        ))
        .expect("fixture exists");
        let mapping_text =
            fs::read_to_string(test_data_path("FilterFiles/resFilterIndexMapping.yaml"))
                .expect("fixture exists");
        let mappings =
            parse_legacy_filter_index_mapping_yaml(&mapping_text).expect("mapping parses");
        let groups = create_legacy_resource_groups_from_filter_mapping(
            test_data_path("CreateResourceFiles/ResourceFiles"),
            test_data_path("FilterFiles"),
            &mappings,
            true,
        )
        .expect("filtered ResourceGroups are created");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, "ResourceGroup.yaml");
        assert_eq!(export_legacy_yaml_resource_group(&groups[0].1), expected);
    }

    #[test]
    fn creates_legacy_resource_group_from_directory_yaml_byte_for_byte() {
        let expected = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupLinux.yaml",
        ))
        .expect("fixture exists");
        let catalog = create_legacy_resource_group_from_directory(
            test_data_path("CreateResourceFiles/ResourceFiles"),
            None,
            true,
        )
        .expect("directory catalog is created");
        assert_eq!(export_legacy_yaml_resource_group(&catalog), expected);
    }

    #[test]
    fn exports_legacy_local_relative_resources_from_directory_catalog() {
        let source_root = test_data_path("CreateResourceFiles/ResourceFiles");
        let catalog = create_legacy_resource_group_from_directory(&source_root, None, true)
            .expect("directory catalog is created");
        let output_root = fresh_test_output_dir("export-directory-catalog");
        let (count, bytes) =
            export_legacy_local_relative_resources(&catalog, &source_root, &output_root)
                .expect("resources export");

        assert_eq!(count, 3);
        assert!(bytes > 0);
        assert_directory_subset(&output_root, &source_root);
        fs::remove_dir_all(output_root).ok();
    }

    #[test]
    fn creates_legacy_resource_group_from_directory_skip_compression_yaml_byte_for_byte() {
        let expected = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupSkipCompressionLinux.yaml",
        ))
        .expect("fixture exists");
        let catalog = create_legacy_resource_group_from_directory(
            test_data_path("CreateResourceFiles/ResourceFiles"),
            None,
            false,
        )
        .expect("directory catalog is created");
        assert_eq!(export_legacy_yaml_resource_group(&catalog), expected);
    }

    #[test]
    fn creates_legacy_resource_group_from_directory_csv_byte_for_byte() {
        let expected =
            fs::read_to_string(test_data_path("CreateResourceFiles/ResourceGroupLinux.csv"))
                .expect("fixture exists");
        let catalog = create_legacy_resource_group_from_directory(
            test_data_path("CreateResourceFiles/ResourceFiles"),
            None,
            true,
        )
        .expect("directory catalog is created");
        assert_eq!(export_legacy_csv_resource_group(&catalog), expected);
    }

    #[test]
    fn creates_legacy_resource_group_from_directory_prefixed_csv_byte_for_byte() {
        let expected = fs::read_to_string(test_data_path(
            "CreateResourceFiles/ResourceGroupLinuxPrefixed.csv",
        ))
        .expect("fixture exists");
        let catalog = create_legacy_resource_group_from_directory(
            test_data_path("CreateResourceFiles/ResourceFiles"),
            Some("test"),
            true,
        )
        .expect("directory catalog is created");
        assert_eq!(export_legacy_csv_resource_group(&catalog), expected);
    }

    #[test]
    fn merges_legacy_yaml_additive_resource_groups_byte_for_byte() {
        let base = parse_legacy_yaml_fixture("MergeGroups/YamlAdditive/BaseResourceGroup.yaml");
        let merge = parse_legacy_yaml_fixture("MergeGroups/YamlAdditive/MergeResourceGroup.yaml");
        let expected = fs::read_to_string(test_data_path(
            "MergeGroups/YamlAdditive/ExpectedMergedResourceGroup.yaml",
        ))
        .expect("fixture exists");
        let merged = merge_legacy_resource_catalogs(&base, &merge);
        assert_eq!(export_legacy_yaml_resource_group(&merged), expected);
    }

    #[test]
    fn merges_legacy_yaml_identical_resource_groups_byte_for_byte() {
        let base = parse_legacy_yaml_fixture("MergeGroups/YamlIdentical/BaseResourceGroup.yaml");
        let merge = parse_legacy_yaml_fixture("MergeGroups/YamlIdentical/MergeResourceGroup.yaml");
        let expected = fs::read_to_string(test_data_path(
            "MergeGroups/YamlIdentical/ExpectedMergedResourceGroup.yaml",
        ))
        .expect("fixture exists");
        let merged = merge_legacy_resource_catalogs(&base, &merge);
        assert_eq!(export_legacy_yaml_resource_group(&merged), expected);
    }

    #[test]
    fn merges_legacy_csv_additive_resource_groups_byte_for_byte() {
        let base = parse_legacy_csv_fixture("MergeGroups/CSVAdditive/BaseResourceGroup.txt");
        let merge = parse_legacy_csv_fixture("MergeGroups/CSVAdditive/MergeResourceGroup.txt");
        let expected = fs::read_to_string(test_data_path(
            "MergeGroups/CSVAdditive/ExpectedMergedResourceGroup.txt",
        ))
        .expect("fixture exists");
        let merged = merge_legacy_resource_catalogs(&base, &merge);
        assert_eq!(export_legacy_csv_resource_group(&merged), expected);
    }

    #[test]
    fn merges_legacy_csv_intersect_resource_groups_byte_for_byte() {
        let base = parse_legacy_csv_fixture("MergeGroups/CSVWithIntersect/BaseResourceGroup.txt");
        let merge = parse_legacy_csv_fixture("MergeGroups/CSVWithIntersect/MergeResourceGroup.txt");
        let expected = fs::read_to_string(test_data_path(
            "MergeGroups/CSVWithIntersect/ExpectedMergedResourceGroup.txt",
        ))
        .expect("fixture exists");
        let merged = merge_legacy_resource_catalogs(&base, &merge);
        assert_eq!(export_legacy_csv_resource_group(&merged), expected);
    }

    #[test]
    fn diffs_legacy_csv_additions_byte_for_byte() {
        assert_diff_fixture(
            "DiffGroups/resFileIndex.txt",
            "DiffGroups/resFileIndexWithAdditions.txt",
            "DiffGroups/ExpectedDiffWithAdditions.txt",
        );
    }

    #[test]
    fn diffs_legacy_csv_changes_byte_for_byte() {
        assert_diff_fixture(
            "DiffGroups/resFileIndex.txt",
            "DiffGroups/resFileIndexWithChanges.txt",
            "DiffGroups/ExpectedDiffWithChanges.txt",
        );
    }

    #[test]
    fn diffs_legacy_csv_subtractions_byte_for_byte() {
        assert_diff_fixture(
            "DiffGroups/resFileIndex.txt",
            "DiffGroups/resFileIndexWithSubtractions.txt",
            "DiffGroups/ExpectedDiffWithSubtractions.txt",
        );
    }

    #[test]
    fn removes_legacy_yaml_resources_byte_for_byte() {
        let catalog = parse_legacy_yaml_fixture("RemoveResource/BaseResourceGroup.yaml");
        let expected = fs::read_to_string(test_data_path(
            "RemoveResource/ResourceGroupAfterRemove.yaml",
        ))
        .expect("fixture exists");
        let removed = remove_legacy_resources(&catalog, &[String::from("B.txt")], true)
            .expect("resource is removed");
        assert_eq!(export_legacy_yaml_resource_group(&removed), expected);
    }

    #[test]
    fn remove_legacy_resources_can_ignore_missing_paths() {
        let catalog = parse_legacy_yaml_fixture("RemoveResource/BaseResourceGroup.yaml");
        let removed = remove_legacy_resources(&catalog, &[String::from("C.txt")], false)
            .expect("missing path is ignored");
        assert_eq!(removed, catalog);
    }

    #[test]
    fn remove_legacy_resources_reports_missing_paths_when_requested() {
        let catalog = parse_legacy_yaml_fixture("RemoveResource/BaseResourceGroup.yaml");
        let error = remove_legacy_resources(&catalog, &[String::from("C.txt")], true)
            .expect_err("missing path is an error");
        assert!(error.contains("C.txt"));
    }

    #[test]
    fn parses_legacy_filter_prefixes_and_rules() {
        let ini = "[DEFAULT]\n\
                   prefixmap = prefix1:.\n\
                   [testSection]\n\
                   filter = [.type1] ![.type2]\n\
                   respaths = prefix1:/* [.type3] ![.type4]";
        let filter = parse_legacy_filter_ini(ini).expect("filter parses");
        assert_eq!(filter.prefix_paths(), &[String::from(".")]);
        assert!(filter.check_path("File.type1.type3"));
        assert!(!filter.check_path("File.type2.type3"));
        assert!(!filter.check_path("File.type1.type4"));
    }

    #[test]
    fn legacy_filter_wildcards_match_like_cpp() {
        assert_filter(
            "[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nrespaths = prefix1:/*",
            [("File", true), ("Subfolder/File", false)],
        );
        assert_filter(
            "[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nrespaths = prefix1:/...",
            [
                ("File", true),
                ("Subfolder1/File", true),
                ("Subfolder2/File", true),
            ],
        );
        assert_filter(
            "[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nrespaths = prefix1:/File",
            [("File", true), ("NonMatching", false)],
        );
    }

    #[test]
    fn legacy_filter_generated_wildcard_path_matrix_matches_cpp_rules() {
        let single_segment = parse_legacy_filter_ini(
            "[DEFAULT]\nprefixmap = prefix1:.\n[test]\nrespaths = prefix1:/*",
        )
        .expect("single segment filter parses");
        for path in ["File", "file.type", "UPPER", "mixed.Case"] {
            assert!(single_segment.check_path(path), "{path} should match /*");
        }
        for path in ["Subfolder/File", "Subfolder/Nested/File"] {
            assert!(
                !single_segment.check_path(path),
                "{path} should not match /*"
            );
        }

        let direct_asset = parse_legacy_filter_ini(
            "[DEFAULT]\nprefixmap = prefix1:.\n[test]\nrespaths = prefix1:/Assets/*.dds",
        )
        .expect("direct asset filter parses");
        for path in ["Assets/ship.dds", "Assets/SHIP.DDS", "assets/mixed.DdS"] {
            assert!(
                direct_asset.check_path(path),
                "{path} should match direct asset wildcard"
            );
        }
        for path in ["Assets/ship.png", "Assets/Sub/ship.dds", "Other/ship.dds"] {
            assert!(
                !direct_asset.check_path(path),
                "{path} should not match direct asset wildcard"
            );
        }

        let recursive_asset = parse_legacy_filter_ini(
            "[DEFAULT]\nprefixmap = prefix1:.\n[test]\nrespaths = prefix1:/Assets/...",
        )
        .expect("recursive asset filter parses");
        for path in [
            "Assets/",
            "Assets/ship.dds",
            "Assets/Sub/ship.dds",
            "assets/Sub/Deep/ship.dds",
        ] {
            assert!(
                recursive_asset.check_path(path),
                "{path} should match recursive ellipsis"
            );
        }
        for path in ["Other/ship.dds", "Asset/ship.dds"] {
            assert!(
                !recursive_asset.check_path(path),
                "{path} should not match recursive ellipsis"
            );
        }
    }

    #[test]
    fn legacy_filter_generated_rule_property_matrix_matches_cpp_rules() {
        let section_recovery = parse_legacy_filter_ini(
            "[DEFAULT]\n\
             prefixmap = prefix1:Root\n\
             [alpha]\n\
             filter = ![ Reject ]\n\
             respaths = prefix1:/...\n\
             [beta]\n\
             respaths = prefix1:/...",
        )
        .expect("section recovery filter parses");
        for (path, expected) in [
            ("Root/Allowed.bin", true),
            ("Root/Reject.bin", true),
            ("Other/Reject.bin", false),
        ] {
            assert_eq!(
                section_recovery.check_path(path),
                expected,
                "section-local include/exclude failure should not poison later sections for {path}"
            );
        }

        let exact_after_failed_wildcard = parse_legacy_filter_ini(
            "[DEFAULT]\n\
             prefixmap = prefix1:Root prefix2:Other\n\
             [test]\n\
             filter = ![ Reject ]\n\
             respaths = prefix1:/...\n           prefix2:/RejectExact.txt",
        )
        .expect("exact-after-failed-wildcard filter parses");
        for (path, expected) in [
            ("Root/Reject.bin", false),
            ("Other/RejectExact.txt", true),
            ("Other/RejectOther.txt", false),
        ] {
            assert_eq!(
                exact_after_failed_wildcard.check_path(path),
                expected,
                "exact file matches should keep the legacy C++ rules for {path}"
            );
        }

        for (label, ini_filter, local_rules, cases) in [
            (
                "global include and exclude",
                "[ Keep ] ![ Drop ]",
                "",
                [
                    ("Assets/Keep.bin", true),
                    ("Assets/DropKeep.bin", false),
                    ("Assets/Other.bin", false),
                    ("Assets/Sub/Keep.bin", false),
                ],
            ),
            (
                "local include and exclude",
                "",
                "[ Keep ] ![ Drop ]",
                [
                    ("Assets/Keep.bin", true),
                    ("Assets/DropKeep.bin", false),
                    ("Assets/Other.bin", false),
                    ("Assets/Sub/Keep.bin", false),
                ],
            ),
            (
                "wildcard include with recursive ellipsis",
                "[ Keep ]",
                "",
                [
                    ("Assets/Keep.bin", true),
                    ("Assets/Sub/Keep.bin", true),
                    ("Assets/Sub/Other.bin", false),
                    ("Other/Keep.bin", false),
                ],
            ),
        ] {
            let respath = if label == "wildcard include with recursive ellipsis" {
                "prefix1:/Assets/..."
            } else {
                "prefix1:/Assets/*.bin"
            };
            let filter = parse_legacy_filter_ini(&format!(
                "[DEFAULT]\n\
                 prefixmap = prefix1:.\n\
                 [test]\n\
                 filter = {ini_filter}\n\
                 respaths = {respath} {local_rules}",
            ))
            .expect("generated rule filter parses");

            for (path, expected) in cases {
                assert_eq!(
                    filter.check_path(path),
                    expected,
                    "{label}: generated rule matrix case {path}"
                );
            }
        }
    }

    #[test]
    fn legacy_filter_include_exclude_rules_match_like_cpp() {
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nfilter = [ .type1 ]\nrespaths = prefix1:/*", [
            ("File.type1", true),
            ("File.type2", false),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nfilter = ![ .type1 ]\nrespaths = prefix1:/*", [
            ("File.type2", true),
            ("File.type1", false),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nfilter = [ .type1 ]![ .type1 ]\nrespaths = prefix1:/*", [
            ("File.type1", false),
            ("File.type2", false),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nfilter = ![ .type1 ][ .type1 ]\nrespaths = prefix1:/*", [
            ("File.type1", false),
            ("File.type2", false),
        ]);
    }

    #[test]
    fn legacy_filter_respath_local_rules_match_like_cpp() {
        assert_filter(
            "[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nrespaths = prefix1:/* ![ File ]",
            [("File", false), ("Another", true)],
        );
        assert_filter(
            "[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nrespaths = prefix1:/* [ File ]",
            [("File", true), ("Another", false)],
        );
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nfilter = [ File ]\nrespaths = prefix1:/* ![ File ]", [
            ("File", false),
            ("Another", false),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nfilter = ![ File ]\nrespaths = prefix1:/* [ File ]", [
            ("File", false),
            ("Another", false),
        ]);
    }

    #[test]
    fn legacy_filter_multi_prefix_rules_match_like_cpp() {
        assert_filter("[DEFAULT]\nprefixmap = prefix1:Path1 prefix2:Path2\n[testSection]\nrespaths = prefix1:/*\n           prefix2:/*", [
            ("Path1/File", true),
            ("Path2/File", true),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:Path1 prefix2:Path2\n[testSection]\nfilter = ![ File ]\nrespaths = prefix1:/*\n           prefix2:/*", [
            ("Path1/File", false),
            ("Path2/File", false),
            ("Path1/Another", true),
            ("Path2/Another", true),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:Path1 prefix2:Path2\n[testSection]\nfilter = [ File ]\nrespaths = prefix1:/*\n           prefix2:/*", [
            ("Path1/File", true),
            ("Path2/File", true),
            ("Path1/Another", false),
            ("Path2/Another", false),
        ]);
    }

    #[test]
    fn legacy_filter_accumulates_local_rules_across_later_paths() {
        assert_filter("[DEFAULT]\nprefixmap = prefix1:Path1 prefix2:Path2\n[testSection]\nfilter = [ File ]\nrespaths = prefix1:/* ![ File ]\n           prefix2:/*", [
            ("Path1/File", false),
            ("Path2/File", false),
            ("Path1/Another", false),
            ("Path2/Another", false),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:Path1 prefix2:Path2\n[testSection]\nfilter = [ .type1 ]\nrespaths = prefix1:/* [ .type2 ]\n           prefix2:/*", [
            ("Path1/File.type1", true),
            ("Path2/File.type2", true),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:Path1 prefix2:Path2\n[testSection]\nfilter = [ .type1 ]\nrespaths = prefix1:/*\n           prefix1:/* ![ .type1 ]\n           prefix2:/*", [
            ("Path1/File.type1", true),
            ("Path2/File.type1", false),
        ]);
    }

    #[test]
    fn legacy_filter_specific_file_local_rule_quirks_match_cpp() {
        assert_filter(
            "[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nrespaths = prefix1:/File [ File ]",
            [("File", false)],
        );
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nrespaths = prefix1:/File ![ NONMatch ]\n           prefix1:/File2", [
            ("File", false),
            ("File2", true),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\n[testSection]\nfilter = ![ File ]\nrespaths = prefix1:/File", [
            ("File", true),
        ]);
        assert_filter("[DEFAULT]\nprefixmap = prefix1:.\nfilter = [ .type1 ]\n[testSection]\nrespaths = prefix1:/*", [
            ("File.type1", true),
            ("File.type2", false),
        ]);
    }

    #[test]
    fn parses_legacy_v0_csv_prefix_paths() {
        let text = fs::read_to_string(test_data_path("Indicies/resFileIndex_v0_0_0.txt"))
            .expect("fixture exists");
        let catalog = parse_legacy_csv_resource_group(&text).expect("csv parses");
        assert_eq!(catalog.resources[0].prefix.as_deref(), Some("res"));
        assert_eq!(catalog.resources[0].path, "intromovie.txt");
        assert_eq!(
            catalog.resources[0].location,
            "a9/a9d1721dd5cc6d54_e6bbb2df307e5a9527159a4c971034b5"
        );
    }

    fn assert_legacy_indicies_v0_csv_as_v1_yaml(csv_fixture: &str, yaml_fixture: &str) {
        let csv_text = fs::read_to_string(test_data_path(csv_fixture)).expect("csv fixture exists");
        let expected_yaml =
            fs::read_to_string(test_data_path(yaml_fixture)).expect("yaml fixture exists");
        let mut catalog = parse_legacy_csv_resource_group(&csv_text).expect("csv parses");
        catalog.version = String::from("0.1.0");
        assert_eq!(export_legacy_yaml_resource_group(&catalog), expected_yaml);
    }

    fn test_data_path(relative: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../carbonengine/resources/tests/testData")
            .join(relative)
    }

    fn fresh_test_output_dir(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "carbon-resources-core-{name}-{}",
            std::process::id()
        ));
        fs::remove_dir_all(&path).ok();
        path
    }

    fn assert_directory_subset(subset_root: &Path, superset_root: &Path) {
        fn visit(subset_root: &Path, current: &Path, superset_root: &Path) {
            for entry in fs::read_dir(current).expect("directory reads") {
                let entry = entry.expect("directory entry reads");
                let path = entry.path();
                let file_type = entry.file_type().expect("file type reads");
                if file_type.is_dir() {
                    visit(subset_root, &path, superset_root);
                } else if file_type.is_file() {
                    let relative = path.strip_prefix(subset_root).expect("relative path");
                    let expected =
                        fs::read(superset_root.join(relative)).expect("superset file reads");
                    let actual = fs::read(&path).expect("subset file reads");
                    assert_eq!(actual, expected, "exported resource {:?}", relative);
                }
            }
        }

        visit(subset_root, subset_root, superset_root);
    }

    fn parse_legacy_yaml_fixture(relative: &str) -> ResourceCatalog {
        let text = fs::read_to_string(test_data_path(relative)).expect("fixture exists");
        parse_legacy_yaml_resource_group(&text).expect("yaml parses")
    }

    fn parse_legacy_csv_fixture(relative: &str) -> ResourceCatalog {
        let text = fs::read_to_string(test_data_path(relative)).expect("fixture exists");
        parse_legacy_csv_resource_group(&text).expect("csv parses")
    }

    fn parse_bundle_fixture(relative: &str) -> BundleResourceCatalog {
        let text = fs::read_to_string(test_data_path(relative)).expect("fixture exists");
        parse_legacy_yaml_bundle_resource_group(&text).expect("bundle parses")
    }

    fn bundle_stream_test_resource_catalog() -> (ResourceCatalog, Vec<(String, Vec<u8>)>) {
        let root = test_data_path("Bundle/TestResources");
        let mut records = Vec::new();
        let mut resources = Vec::new();
        for relative_path in ["One.png", "Two.png", "Three.png"] {
            let path = root.join(relative_path);
            let data = fs::read(&path).expect("bundle stream fixture reads");
            let checksum = md5_hex(&data);
            let compressed_size_bytes =
                gzip_compress(&data).expect("fixture compresses").len() as u64;
            let binary_operation =
                legacy_binary_operation(&path).expect("fixture binary operation reads");
            records.push(ResourceRecord {
                location: legacy_location_for_path(relative_path, &checksum),
                path: relative_path.to_string(),
                size_bytes: data.len() as u64,
                compressed_size_bytes: Some(compressed_size_bytes),
                checksum: Some(checksum),
                binary_operation: Some(binary_operation),
                prefix: None,
            });
            resources.push((relative_path.to_string(), data));
        }

        let total_uncompressed_size_bytes =
            records.iter().map(|record| record.size_bytes).sum::<u64>();
        let total_compressed_size_bytes = records
            .iter()
            .map(|record| record.compressed_size_bytes.unwrap_or_default())
            .sum::<u64>();
        (
            ResourceCatalog {
                version: String::from("0.1.0"),
                catalog_type: String::from("ResourceGroup"),
                total_compressed_size_bytes: Some(total_compressed_size_bytes),
                total_uncompressed_size_bytes,
                resources: records,
            },
            resources,
        )
    }

    fn assert_reconstructed_bundle_stream_matches_resources(
        stream_data: &[u8],
        resources: &[(String, Vec<u8>)],
    ) {
        let expected_total = resources.iter().map(|(_, data)| data.len()).sum::<usize>();
        assert_eq!(stream_data.len(), expected_total);

        let mut offset = 0;
        for (relative_path, expected) in resources {
            let end = offset + expected.len();
            let actual = &stream_data[offset..end];
            assert_eq!(
                md5_hex(actual),
                md5_hex(expected),
                "reconstructed resource {relative_path}"
            );
            assert_eq!(actual, expected.as_slice(), "resource {relative_path}");
            offset = end;
        }
    }

    fn assert_diff_fixture(base: &str, target: &str, expected: &str) {
        let base = parse_legacy_csv_fixture(base);
        let target = parse_legacy_csv_fixture(target);
        let expected = fs::read_to_string(test_data_path(expected)).expect("fixture exists");
        let diff = diff_legacy_resource_catalogs(&base, &target);
        assert_eq!(export_legacy_diff(&diff), expected);
    }

    fn assert_bundle_data_matches_fixture(resource: &LegacyBundleDataResource, root: &str) {
        let expected = fs::read(test_data_path(&format!(
            "{}/{}",
            root, resource.record.location
        )))
        .expect("fixture exists");
        assert_eq!(resource.data, expected, "resource {}", resource.record.path);
    }

    fn assert_applied_patch_resources_match_next_build(
        applied: &AppliedLegacyPatchSet,
        next_build_root: &str,
    ) {
        let expected_root = test_data_path(next_build_root);
        for resource in &applied.resources {
            let expected_path =
                resolve_existing_case_insensitive_path(&expected_root.join(&resource.path))
                    .expect("next build fixture exists");
            let expected = fs::read(expected_path).expect("next build fixture reads");
            assert_eq!(resource.data, expected, "resource {}", resource.path);
        }
    }

    fn assert_unpacked_bundle_matches_expected_resources(unpacked: &UnpackedLegacyLocalBundle) {
        assert_eq!(unpacked.resources.len(), 3);
        let exported_group = export_legacy_yaml_resource_group(&unpacked.resource_catalog);
        assert_eq!(
            exported_group.as_bytes(),
            unpacked.resource_group_resource.data.as_slice()
        );

        let expected_root = test_data_path("Bundle/Res");
        for resource in &unpacked.resources {
            let expected_path =
                resolve_existing_case_insensitive_path(&expected_root.join(&resource.path))
                    .expect("fixture resource exists");
            let expected = fs::read(expected_path).expect("fixture resource reads");
            assert_eq!(resource.data, expected, "resource {}", resource.path);
        }
    }

    fn copy_directory_recursive(source: &Path, destination: &Path) {
        for entry in fs::read_dir(source).expect("source directory reads") {
            let entry = entry.expect("source directory entry reads");
            let path = entry.path();
            let destination_path = destination.join(entry.file_name());
            let file_type = entry.file_type().expect("source entry type reads");
            if file_type.is_dir() {
                fs::create_dir_all(&destination_path).expect("destination dir is created");
                copy_directory_recursive(&path, &destination_path);
            } else if file_type.is_file() {
                if let Some(parent) = destination_path.parent() {
                    fs::create_dir_all(parent).expect("destination parent is created");
                }
                fs::copy(&path, &destination_path).expect("file is copied");
            }
        }
    }

    fn single_resource_catalog(path: &str, data: &[u8]) -> ResourceCatalog {
        ResourceCatalog {
            version: String::from("0.1.0"),
            catalog_type: String::from("ResourceGroup"),
            total_compressed_size_bytes: None,
            total_uncompressed_size_bytes: data.len() as u64,
            resources: vec![ResourceRecord {
                path: String::from(path),
                location: String::from(path),
                size_bytes: data.len() as u64,
                compressed_size_bytes: None,
                checksum: Some(md5_hex(data)),
                binary_operation: None,
                prefix: None,
            }],
        }
    }

    fn write_test_patch_payload(root: &Path, resource: &LegacyPatchDataResource) {
        let path = root.join(resource.location.replace('\\', "/"));
        fs::create_dir_all(path.parent().expect("payload path has parent"))
            .expect("payload parent is created");
        fs::write(path, &resource.data).expect("payload is written");
    }

    fn assert_bundle_yaml_fixture(relative: &str, expected_chunks: usize, expected_prefix: &str) {
        let text = fs::read_to_string(test_data_path(relative)).expect("fixture exists");
        let catalog = parse_legacy_yaml_bundle_resource_group(&text).expect("bundle parses");
        assert_eq!(catalog.catalog_type, "BundleGroup");
        assert_eq!(
            catalog.resource_group_resource.resource_type,
            "ResourceGroup"
        );
        assert_eq!(catalog.chunk_size, 1000);
        assert_eq!(catalog.len(), expected_chunks);
        assert!(catalog
            .resources
            .iter()
            .all(|resource| resource.resource_type == "BinaryChunk"));
        assert!(catalog
            .resources
            .iter()
            .all(|resource| resource.path.starts_with(expected_prefix)));
        assert_eq!(export_legacy_yaml_bundle_resource_group(&catalog), text);
    }

    fn assert_patch_yaml_fixture(
        relative: &str,
        expected_patches: usize,
        expected_max_input_chunk_size: u64,
        expected_removed_count: Option<usize>,
    ) -> PatchResourceCatalog {
        let text = fs::read_to_string(test_data_path(relative)).expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        assert_eq!(catalog.catalog_type, "PatchGroup");
        assert_eq!(
            catalog.resource_group_resource.resource_type,
            "ResourceGroup"
        );
        assert_eq!(catalog.max_input_chunk_size, expected_max_input_chunk_size);
        assert_eq!(
            catalog
                .removed_resource_relative_paths
                .as_ref()
                .map(Vec::len),
            expected_removed_count
        );
        assert_eq!(catalog.len(), expected_patches);
        assert!(catalog
            .resources
            .iter()
            .all(|resource| resource.resource_type == "BinaryPatch"));
        assert_eq!(export_legacy_yaml_patch_resource_group(&catalog), text);
        catalog
    }

    fn assert_legacy_local_patch_payloads_byte_for_byte(
        patch_group_relative: &str,
        previous_root: &str,
        latest_root: &str,
        patch_root: &str,
        expected_generated_records: usize,
        expected_copy_only_records: usize,
    ) {
        let text =
            fs::read_to_string(test_data_path(patch_group_relative)).expect("fixture exists");
        let catalog = parse_legacy_yaml_patch_resource_group(&text).expect("patch parses");
        let max_input_chunk_size = usize::try_from(catalog.max_input_chunk_size)
            .expect("fixture max input chunk fits in usize");
        let previous_root = test_data_path(previous_root);
        let latest_root = test_data_path(latest_root);
        let patch_root = test_data_path(patch_root);
        let mut generated_records = 0_usize;
        let mut copy_only_records = 0_usize;

        for record in &catalog.resources {
            let previous = read_legacy_local_relative_data(
                &previous_root,
                &record.target_resource_relative_path,
            )
            .expect("previous resource reads");
            let latest = read_legacy_local_relative_data(
                &latest_root,
                &record.target_resource_relative_path,
            )
            .expect("latest resource reads");
            let source_offset =
                usize::try_from(record.source_offset).expect("source offset fits in usize");
            let data_offset =
                usize::try_from(record.data_offset).expect("data offset fits in usize");
            assert!(
                source_offset <= previous.len(),
                "source offset exceeds previous data for {}",
                record.path
            );
            assert!(
                data_offset <= latest.len(),
                "data offset exceeds latest data for {}",
                record.path
            );

            if record.location.is_empty() {
                copy_only_records += 1;
                let copy_len =
                    usize::try_from(record.size_bytes).expect("copy length fits in usize");
                let source_end = source_offset
                    .checked_add(copy_len)
                    .expect("copy source range does not overflow");
                let data_end = data_offset
                    .checked_add(copy_len)
                    .expect("copy target range does not overflow");
                assert!(
                    source_end <= previous.len(),
                    "copy source range exceeds previous data for {}",
                    record.path
                );
                assert!(
                    data_end <= latest.len(),
                    "copy target range exceeds latest data for {}",
                    record.path
                );
                assert_eq!(
                    &previous[source_offset..source_end],
                    &latest[data_offset..data_end],
                    "copy-only range {} should match latest bytes",
                    record.path
                );
                assert_eq!(record.checksum, md5_hex(&[]));
                assert_eq!(record.compressed_size_bytes, None);
                continue;
            }

            generated_records += 1;
            let source_end = source_offset
                .saturating_add(max_input_chunk_size)
                .min(previous.len());
            let data_end = data_offset
                .saturating_add(max_input_chunk_size)
                .min(latest.len());
            let generated = create_legacy_binary_patch(
                &previous[source_offset..source_end],
                &latest[data_offset..data_end],
            )
            .expect("legacy patch is generated");
            let expected =
                read_legacy_patch_resource_data(&patch_root, record).expect("patch payload reads");

            assert_eq!(generated, expected.data, "patch payload {}", record.path);
            assert_eq!(generated.len() as u64, record.size_bytes);
            assert_eq!(md5_hex(&generated), record.checksum);

            let reapplied =
                apply_legacy_binary_patch(&previous[source_offset..source_end], &generated)
                    .expect("generated patch applies");
            assert_eq!(
                reapplied,
                latest[data_offset..data_end],
                "generated patch {} should reproduce latest chunk",
                record.path
            );
        }

        assert_eq!(generated_records, expected_generated_records);
        assert_eq!(copy_only_records, expected_copy_only_records);
    }

    fn assert_filter<const N: usize>(ini: &str, cases: [(&str, bool); N]) {
        let filter = parse_legacy_filter_ini(ini).expect("filter parses");
        for (path, expected) in cases {
            assert_eq!(filter.check_path(path), expected, "path {path}");
        }
    }
}
