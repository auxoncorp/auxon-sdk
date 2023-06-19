#![deny(warnings, clippy::all)]
pub mod child_connection;
pub mod parent_connection;
pub use modality_mutation_plane;

#[cfg(test)]
mod tests {
    // TODO - test basic child connection
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
