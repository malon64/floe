use arrow::record_batch::RecordBatch;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::DataFileFormat;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use uuid::Uuid;

use crate::io::write::metrics;
use crate::{config, FloeResult};

use super::map_iceberg_err;

pub(super) fn iceberg_small_file_threshold_bytes(entity: &config::EntityConfig) -> u64 {
    metrics::default_small_file_threshold_bytes(
        entity
            .sink
            .accepted
            .options
            .as_ref()
            .and_then(|options| options.max_size_per_file),
    )
}

pub(super) async fn write_data_files(
    table: &iceberg::table::Table,
    batch: RecordBatch,
) -> FloeResult<Vec<iceberg::spec::DataFile>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(map_iceberg_err("iceberg location generator failed"))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "floe".to_string(),
        Some(Uuid::now_v7().to_string()),
        DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        Default::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let partition_spec = table.metadata().default_partition_spec().clone();
    if partition_spec.is_unpartitioned() {
        let mut writer = data_file_writer_builder
            .build(None)
            .await
            .map_err(map_iceberg_err("iceberg data writer build failed"))?;
        writer
            .write(batch)
            .await
            .map_err(map_iceberg_err("iceberg data write failed"))?;
        return writer
            .close()
            .await
            .map_err(map_iceberg_err("iceberg data writer close failed"));
    }

    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        partition_spec,
    )
    .map_err(map_iceberg_err("iceberg partition splitter init failed"))?;
    let partitioned_batches = splitter
        .split(&batch)
        .map_err(map_iceberg_err("iceberg partition split failed"))?;

    let mut writer = FanoutWriter::new(data_file_writer_builder);
    for (partition_key, partition_batch) in partitioned_batches {
        writer
            .write(partition_key, partition_batch)
            .await
            .map_err(map_iceberg_err("iceberg partitioned data write failed"))?;
    }
    writer.close().await.map_err(map_iceberg_err(
        "iceberg partitioned data writer close failed",
    ))
}
