//! `Dataset` is an alias for `HashMap` that keeps vectors of basic data
//! (provided as `Dataunit`) by keys, so it represents a common dataset
//! having columns (the keys) and rows.

use std::collections::HashMap;

use tokio::io::{ErrorKind, Result as TokioResult};

use crate::datatype::Dataunit;


/// `Dataset` is an alias for the HashMap of strings as keys vectors of 
/// Dataunit as values. Since Dataunit is an enum over integers, float and 
/// strings, they are the supported datatypes for the dataset.
pub type Dataset = HashMap<String, Vec<Dataunit>>;


/// Get size of the dataset. It works correctly for valid datasets because the 
/// function returns the length of the first vector. Otherwise it returns error.
pub fn get_dataset_size(ds: &Dataset) -> TokioResult<usize> {
    let mut size: Option<usize> = None;
    for v in ds.values() {
        if size.is_none() {
            size = Some(v.len());
        }
        if size != Some(v.len()) {
            return Err(ErrorKind::InvalidData.into());
        }
    }
    Ok(size.unwrap_or(0))
}
