/// Shared benchmark helpers.
///
/// All benchmark files import from this library:
///   use fvfs_bench::{setup, rt};
pub mod setup;

pub use setup::{BenchSetup, make_setup, rt};
