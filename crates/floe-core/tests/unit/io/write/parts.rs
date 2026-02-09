use floe_core::io::write::parts::{
    append_part_filename, clear_local_part_files, is_part_filename, list_local_part_files,
    next_local_part_filename, PartNameAllocator,
};
use floe_core::FloeResult;
use uuid::Uuid;

#[test]
fn sequential_allocation_uses_next_available_part_index() -> FloeResult<()> {
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
fn append_allocation_generates_unique_part_names() -> FloeResult<()> {
    let mut allocator = PartNameAllocator::unique("parquet");
    let first = allocator.allocate_next();
    let second = allocator.allocate_next();
    assert_ne!(first, second);
    assert!(is_part_filename(&first, "parquet"));
    assert!(is_part_filename(&second, "parquet"));

    let first_id = first
        .strip_prefix("part-")
        .and_then(|name| name.strip_suffix(".parquet"))
        .expect("uuid suffix");
    let second_id = second
        .strip_prefix("part-")
        .and_then(|name| name.strip_suffix(".parquet"))
        .expect("uuid suffix");
    assert!(Uuid::parse_str(first_id).is_ok());
    assert!(Uuid::parse_str(second_id).is_ok());

    Ok(())
}

#[test]
fn overwrite_cleanup_deletes_existing_part_files() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let output_dir = temp_dir.path().join("accepted");
    std::fs::create_dir_all(&output_dir)?;
    std::fs::write(output_dir.join("part-00000.parquet"), b"p0")?;
    std::fs::write(output_dir.join("part-00001.parquet"), b"p1")?;
    std::fs::write(output_dir.join(append_part_filename("parquet")), b"uuid")?;

    let removed = clear_local_part_files(&output_dir, "parquet")?;
    assert_eq!(removed, 3);
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
    std::fs::write(
        output_dir.join("part-123e4567-e89b-12d3-a456-426614174000.parquet"),
        b"uuid",
    )?;
    std::fs::write(output_dir.join("part-00001.csv"), b"csv")?;
    std::fs::write(output_dir.join("part-.parquet"), b"invalid-part-name")?;
    std::fs::write(output_dir.join("manifest.parquet"), b"manifest")?;
    std::fs::write(output_dir.join("_delta_log/00000.json"), b"log")?;

    let removed = clear_local_part_files(&output_dir, "parquet")?;
    assert_eq!(removed, 3);

    assert!(!output_dir.join("part-00000.parquet").exists());
    assert!(!output_dir.join("part-00001.parquet").exists());
    assert!(!output_dir
        .join("part-123e4567-e89b-12d3-a456-426614174000.parquet")
        .exists());
    assert!(output_dir.join("part-00001.csv").exists());
    assert!(output_dir.join("part-.parquet").exists());
    assert!(output_dir.join("manifest.parquet").exists());
    assert!(output_dir.join("_delta_log/00000.json").exists());

    Ok(())
}
