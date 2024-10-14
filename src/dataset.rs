use std::any::Any;
use std::collections::HashMap;


/// `Dataset` is an alias for the vector of vectors enumerated by strins (keys).
/// The main advantage is the ability to keep inner vectors of different types
/// and converting the whole dataset into a HashMap and back.
pub type Dataset<'a> = Vec<(&'a str, Vec<&'a dyn Any>)>;

/// Alias for dataset represented as a HashMap.
pub type DatasetAsMap<'a> = HashMap<&'a str, Vec<&'a dyn Any>>;


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
