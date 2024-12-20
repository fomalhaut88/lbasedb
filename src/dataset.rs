use std::any::Any;
use std::collections::HashMap;


/// `Dataset` is an alias for the vector of vectors enumerated by strins (keys).
/// The main advantage is the ability to keep inner vectors of different types
/// and converting the whole dataset into a HashMap and back.
pub type Dataset<'a> = Vec<(&'a str, Vec<&'a dyn Any>)>;

/// Alias for dataset represented as a HashMap.
pub type DatasetAsMap<'a> = HashMap<&'a str, Vec<&'a dyn Any>>;


/// Get size of the dataset. It works correctly for valid datasets because the 
/// function returns the length of the first vector.
pub fn get_size(ds: &Dataset) -> Option<usize> {
    ds.get(0).map(|(_, v)| v.len())
}


/// Check if the dataset is valid that means all the vectors have the same size.
pub fn is_valid(ds: &Dataset) -> bool {
    // TODO: Consider that the types correspond to allowed datatypes.
    get_size(ds).map(
        |size| ds.iter().all(|(_, v)| v.len() == size)
    ).unwrap_or(true)
}


/// Get keys of the dataset as iterator.
pub fn get_keys<'a>(ds: &'a Dataset) -> impl Iterator<Item = &'a str> {
    ds.iter().map(|(k, _)| *k)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        // Define a dataset
        let ds: Dataset = vec![
            ("empty", vec![]),
            ("integers", vec![&5i32, &6i32]),
            ("floats", vec![&0.25f64]),
            // ("buffers", vec![vec![1u8, 2u8], vec![2u8, 3u8]].as_ref()),
        ];

        // Build a map
        let map: DatasetAsMap = ds.into_iter().collect();

        // Extract vector of values
        let integers = map["integers"].iter()
            .map(|e| *e.downcast_ref::<i32>().unwrap())
            .collect::<Vec<i32>>();
        assert_eq!(integers, vec![5, 6]);

        // Convert the map back to dataset
        let _ds: Dataset = map.into_iter().collect();
    }
}
