/// Determines how an output file should be positioned relative to the target base.
#[derive(Debug, Clone, Copy)]
pub enum OutputPlacement {
    /// Write into the target path/key (file or directory).
    Output,
    /// Write into the target path/key treating it as a directory.
    Directory,
    /// Write alongside the target (sibling of a file target).
    Sibling,
}
