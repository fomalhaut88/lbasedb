use std::collections::HashMap;

use tokio::io::{ErrorKind, Result as TokioResult};

use crate::datatype::Dataunit;


/// `Dataset` is an alias for the vector of vectors enumerated by strins (keys).
/// The main advantage is the ability to keep inner vectors of different types
/// and converting the whole dataset into a HashMap and back.
pub type Dataset = HashMap<String, Vec<Dataunit>>;

// /// Alias for dataset represented as a HashMap.
// pub type DatasetAsMap = HashMap<String, Vec<Dataunit>>;


/// Get size of the dataset. It works correctly for valid datasets because the 
/// function returns the length of the first vector.
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


// /// Check if the dataset is valid that means all the vectors have the same size.
// pub fn is_valid(ds: &Dataset) -> bool {
//     // TODO: Consider that the types correspond to allowed datatypes.
//     get_size(ds).map(
//         |size| ds.iter().all(|(_, v)| v.len() == size)
//     ).unwrap_or(true)
// }


// /// Get keys of the dataset as iterator.
// pub fn get_keys(ds: &Dataset) -> impl Iterator<Item = String> + use<'_> {
//     ds.iter().map(|(k, _)| k.clone())
// }


// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test() {
//         // Define a dataset
//         let ds: Dataset = vec![
//             ("empty".to_string(), vec![]),
//             ("integers".to_string(), vec![Dataunit::I(5), Dataunit::I(6)]),
//             ("floats".to_string(), vec![Dataunit::F(0.25)]),
//             ("buffers".to_string(), vec![Dataunit::S("+uwgVQA=".to_string())]),
//         ];

//         // Build a map
//         let map: DatasetAsMap = ds.into_iter().collect();

//         // Extract vector of values
//         let integers = map["integers"].iter()
//             .map(|e| if let Dataunit::I(x) = e { *x  as i32 } else { 0 })
//             .collect::<Vec<i32>>();
//         assert_eq!(integers, vec![5, 6]);

//         // Convert the map back to dataset
//         let _ds: Dataset = map.into_iter().collect();
//     }
// }
