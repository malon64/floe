use floe_core::io::write::parts::{
    clear_local_part_files, list_local_part_files, next_local_part_filename, PartNameAllocator,
};
use floe_core::FloeResult;

#[test]
fn append_allocation_uses_next_available_part_index() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let output_dir = temp_dir.path().join("accepted");
    std::fs::create_dir_all(&output_dir)?;
    std::fs::write(output_dir.join("part-00000.parquet"), b"p0")?;
    std::fs::write(output_dir.join("part-00003.parquet"), b"p3")?;
    std::fs::write(output_dir.join("part-00003.csv"), b"csv")?;
    std::fs::write(output_dir.join("README.txt"), b"keep")?;

    let parts = list_local_part_files(&output_dir, "parquet")?;
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].file_name, "part-00000.parquet");
    assert_eq!(parts[1].file_name, "part-00003.parquet");

    assert_eq!(
        next_local_part_filename(&output_dir, "parquet")?,
        "part-00004.parquet"
    );

    let mut allocator = PartNameAllocator::from_local_path(&output_dir, "parquet")?;
    let first_allocated = allocator.allocate_next();
    assert_eq!(first_allocated, "part-00004.parquet");
    std::fs::write(output_dir.join(&first_allocated), b"p4")?;
    assert_eq!(allocator.allocate_next(), "part-00005.parquet");

    Ok(())
}

#[test]
fn overwrite_cleanup_deletes_existing_part_files() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let output_dir = temp_dir.path().join("accepted");
    std::fs::create_dir_all(&output_dir)?;
    std::fs::write(output_dir.join("part-00000.parquet"), b"p0")?;
    std::fs::write(output_dir.join("part-00001.parquet"), b"p1")?;

    let removed = clear_local_part_files(&output_dir, "parquet")?;
    assert_eq!(removed, 2);
    assert!(!output_dir.join("part-00000.parquet").exists());
    assert!(!output_dir.join("part-00001.parquet").exists());
    assert!(list_local_part_files(&output_dir, "parquet")?.is_empty());

    Ok(())
}

#[test]
fn overwrite_cleanup_preserves_non_part_files() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let output_dir = temp_dir.path().join("accepted");
    std::fs::create_dir_all(output_dir.join("_delta_log"))?;

    std::fs::write(output_dir.join("part-00000.parquet"), b"p0")?;
    std::fs::write(output_dir.join("part-00001.parquet"), b"p1")?;
    std::fs::write(output_dir.join("part-00001.csv"), b"csv")?;
    std::fs::write(output_dir.join("part-abcde.parquet"), b"invalid-part-name")?;
    std::fs::write(output_dir.join("manifest.parquet"), b"manifest")?;
    std::fs::write(output_dir.join("_delta_log/00000.json"), b"log")?;

    let removed = clear_local_part_files(&output_dir, "parquet")?;
    assert_eq!(removed, 2);

    assert!(!output_dir.join("part-00000.parquet").exists());
    assert!(!output_dir.join("part-00001.parquet").exists());
    assert!(output_dir.join("part-00001.csv").exists());
    assert!(output_dir.join("part-abcde.parquet").exists());
    assert!(output_dir.join("manifest.parquet").exists());
    assert!(output_dir.join("_delta_log/00000.json").exists());

    Ok(())
}
