pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod codec;
pub mod command;
pub mod config;
pub mod connection;
pub mod persistence;
pub mod protocol;
pub mod store;
pub mod version;
pub mod wal;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
