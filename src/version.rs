/// EmberDB application version.
/// Derived from Cargo.toml.
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

/// TCP binary protocol version.
pub const PROTOCOL_VERSION: u8 = 2;

/// Persistent snapshot format version.
pub const SNAPSHOT_VERSION: u32 = 1;

/// Persistent write-ahead log format version.
pub const WAL_VERSION: u8 = 1;
