// #![warn(missing_docs)]
#![feature(test)]
// #![feature(async_closure)]

// use std::fs::exists;
// use std::hash::Hash;
use std::collections::HashMap;

use tokio::io::Result as TokioResult;
// use tokio::task::JoinSet;
use tokio::io::ErrorKind;
// use tokio::fs::{create_dir_all, create_dir, remove_dir_all, rename};
use tokio::fs::{create_dir_all, remove_dir_all, rename};

pub mod utils;
pub mod seq;
pub mod col;
pub mod list;
pub mod datatype;
pub mod dataset;
// pub mod mgr;

pub use crate::utils::*;
pub use crate::seq::*;
pub use crate::col::*;
pub use crate::list::*;
pub use crate::datatype::*;
pub use crate::dataset::*;
// pub use crate::mgr::*;


const MAX_NAME_SIZE: usize = 256;

type NameType = [u8; MAX_NAME_SIZE];


// use std::path::Path;


// pub struct Connection {
//     pub feed_mgr: FeedMgr,
// }


// impl Connection {
//     pub async fn new(path: &str) -> TokioResult<Self> {
//         let mut feed_mgr = FeedMgr::new(path).await?;
//         Ok(Self { feed_mgr })
//     }

//     pub fn path(&self) -> &str {
//         self.feed_mgr.path()
//     }

//     pub async fn push(&mut self, feed: &str) -> TokioResult<usize> {
//         // self.feed_mgr.col_mgr(feed);
//         Ok(0)
//     }
// }


// fn map_rename_key<K, V>(map: &mut HashMap<K, V>, 
//                         key_old: &K, key_new: K) -> TokioResult<()>
//         where K: Eq + Hash {
//     if !map.contains_key(key_old) {
//         Err(ErrorKind::NotFound.into())
//     } else if map.contains_key(&key_new) {
//         Err(ErrorKind::AlreadyExists.into())
//     } else {
//         let entity = map.remove(key_old).unwrap();
//         map.insert(key_new, entity);
//         Ok(())
//     }
// }


#[derive(Clone, Debug)]
pub struct FeedItem {
    name: NameType,
    size: usize,
}


impl ListKeyTrait<String> for FeedItem {
    fn key(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }
}


impl FeedItem {
    pub fn new(name: &str) -> Self {
        Self {
            name: str_to_bytes::<MAX_NAME_SIZE>(name),
            size: 0,
        }
    }

    pub fn rename(&mut self, name: &str) {
        self.name = str_to_bytes::<MAX_NAME_SIZE>(name);
    }
}


#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ColItem {
    name: NameType,
    datatype: Datatype,
}


impl ListKeyTrait<String> for ColItem {
    fn key(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }
}


impl ColItem {
    pub fn new(name: &str, datatype: &str) -> Self {
        Self {
            name: str_to_bytes::<MAX_NAME_SIZE>(name),
            datatype: datatype.parse().unwrap(),
        }
    }

    pub fn rename(&mut self, name: &str) {
        self.name = str_to_bytes::<MAX_NAME_SIZE>(name);
    }
}


/// Connection object that manages all the entities. Since it interacts with 
/// the file system and supports asynchronous interface, there is no need 
/// to use it in a multi threading way.
pub struct Connection {
    // Path to the directory where the data and settings are stored
    path: String,

    // Feed list object to manage the feeds options
    feed_list: List<FeedItem, String>,

    // Feed mapping feed key -> feed
    feed_map: HashMap<String, FeedItem>,

    // Col list objects that is a mapping feed key -> the list
    col_list_mapping: HashMap<String, List<ColItem, String>>,

    // Col mapping as double map feed key -> col key -> col
    col_map_mapping: HashMap<String, HashMap<String, ColItem>>,

    // Seq mapping as double map feed key -> col key -> seq
    seq_mapping: HashMap<String, HashMap<String, Seq>>,
}


impl Connection {
    pub async fn new(path: &str) -> TokioResult<Self> {
        // Ensure the directory
        create_dir_all(path).await?;

        // List of feeds
        let feed_list = List::<FeedItem, String>::new(
            Self::_get_feed_list_path(path)
        ).await?;

        // Create instance
        let mut instance = Self {
            path: path.to_string(),
            feed_list,
            feed_map: HashMap::new(),
            col_list_mapping: HashMap::new(),
            col_map_mapping: HashMap::new(),
            seq_mapping: HashMap::new(),
        };

        // Open all feeds
        let feed_map = instance.feed_list.map().await?;
        for (feed_name, feed_item) in feed_map.into_iter() {
            instance._feed_open(&feed_name, feed_item).await?;
        }

        Ok(instance)

        // // Mapping feed_name -> feed
        // let feed_map = feed_list.map().await?;

        // // Mapping feed_name -> col_list
        // let mut col_list_mapping = HashMap::new();

        // // Mapping feed_name -> col_name -> col
        // let mut col_map_mapping = HashMap::new();

        // // Mapping feed_name -> col_name -> seq
        // let mut seq_mapping = HashMap::new();

        // // Loop for feed names
        // for feed_name in feed_map.keys() {
        //     // List of columns
        //     let mut col_list = List::<ColItem, String>::new(
        //         Self::_get_col_list_path(path, feed_name)
        //     ).await?;

        //     // Mapping col_name -> col
        //     let col_map = col_list.map().await?;

        //     // Mapping col_name -> seq
        //     seq_mapping.insert(feed_name.clone(), HashMap::new());

        //     // Loop for cols
        //     for (col_name, col_rec) in col_map.iter() {
        //         // Create a seq
        //         let seq = Seq::new(
        //             Self::_get_seq_path(path, feed_name, col_name),
        //             col_rec.datatype.size()
        //         ).await?;

        //         // Add the seq to the mapping
        //         seq_mapping.get_mut(feed_name).unwrap().insert(col_name.clone(), seq);
        //     }

        //     // Update mappings
        //     col_list_mapping.insert(feed_name.clone(), col_list);
        //     col_map_mapping.insert(feed_name.clone(), col_map);
        // }

        // Ok(Self {
        //     path: path.to_string(),
        //     feed_list,
        //     feed_map,
        //     col_list_mapping,
        //     col_map_mapping,
        //     seq_mapping,
        // })
    }

    pub fn path(&self) -> String {
        self.path.clone()
    }

    pub fn feed_list(&self) -> Vec<String> {
        self.feed_map.keys().cloned().collect()
    }

    pub fn feed_exists(&self, feed_name: &str) -> bool {
        self.feed_map.contains_key(feed_name)
    }

    pub async fn feed_add(&mut self, feed_name: &str) -> TokioResult<()> {
        // Check whether it exists
        if self.feed_exists(feed_name) {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Create directory for the feed
            let feed_path = path_concat!(self.path.clone(), feed_name);
            create_dir_all(feed_path).await?;

            // Insert a new record into the list
            let feed_item = FeedItem::new(feed_name);
            self.feed_list.add(&feed_item).await?;

            // Open the feed
            self._feed_open(feed_name, feed_item).await?;

            // // Ensure col list
            // let col_list = List::<ColItem, String>::new(
            //     Self::_get_col_list_path(&self.path, feed_name)
            // ).await?;

            // // Update mappings
            // self.feed_map.insert(feed_name.to_string(), feed_item);
            // self.col_list_mapping.insert(feed_name.to_string(), col_list);
            // self.col_map_mapping.insert(feed_name.to_string(), HashMap::new());
            // self.seq_mapping.insert(feed_name.to_string(), HashMap::new());

            Ok(())
        }
    }

    pub async fn feed_remove(&mut self, feed_name: &str) -> TokioResult<()> {
        // Check whether it exists
        if !self.feed_exists(feed_name) {
            Err(ErrorKind::NotFound.into())
        } else {
            // // Update mappings
            // self.feed_map.remove(feed_name);
            // self.col_list_mapping.remove(feed_name);
            // self.col_map_mapping.remove(feed_name);
            // self.seq_mapping.remove(feed_name);

            // Close the feed
            self._feed_close(feed_name);

            // Remove from the list
            self.feed_list.remove(&feed_name.to_string()).await?;

            // Remove the directory
            let feed_path = path_concat!(self.path.clone(), feed_name);
            remove_dir_all(feed_path).await?;

            Ok(())
        }
    }

    pub async fn feed_rename(&mut self, name: &str, name_new: &str) -> TokioResult<()> {
        if !self.feed_exists(name) {
            Err(ErrorKind::NotFound.into())
        } else if self.feed_exists(name_new) {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // // Close all seq files by removing them from seq_mapping
            // self.seq_mapping.remove(name);

            // // Close col list file by removing it from col_list_mapping
            // self.col_list_mapping.remove(name);
            // self.col_map_mapping.remove(name);

            // Close the feed
            let mut feed_item = self._feed_close(name);

            // Update feed list
            // let mut feed_item = self.feed_map.remove(name).unwrap();
            feed_item.rename(name_new);
            self.feed_list.modify(&name.to_string(), &feed_item).await?;
            // self.feed_map.insert(name_new.to_string(), feed_item);

            // Rename the directory
            let feed_path = path_concat!(self.path.clone(), name);
            let feed_path_new = path_concat!(self.path.clone(), name_new);
            rename(feed_path, feed_path_new).await?;

            // Open the feed
            self._feed_open(name_new, feed_item).await?;

            // // Open col list file
            // let col_list_path = Self::_get_col_list_path(&self.path, name_new);
            // let mut col_list = List::<ColItem, String>::new(col_list_path).await?;
            // let col_map = col_list.map().await?;

            // // Open all seq files
            // for (col_name, col_item) in col_map.iter() {
            //     // Create a seq
            //     let seq_path = Self::_get_seq_path(&self.path, name_new, col_name);
            //     let seq = Seq::new(seq_path, col_item.datatype.size()).await?;

            //     // Add the seq to the mapping
            //     self.seq_mapping.get_mut(name_new).unwrap().insert(col_name.clone(), seq);
            // }

            // // Update mappings
            // self.col_list_mapping.insert(name_new.to_string(), col_list);
            // self.col_map_mapping.insert(name_new.to_string(), col_map);

            Ok(())
        }
    }

    pub fn col_list(&self, feed_name: &str) -> TokioResult<Vec<String>> {
        self.col_map_mapping.get(feed_name)
            .map(|item| item.keys().cloned().collect())
            .ok_or(ErrorKind::NotFound.into())
    }

    pub fn col_exists(&self, feed_name: &str, col_name: &str) -> bool {
        self.col_map_mapping[feed_name].contains_key(col_name)
    }

    pub async fn col_rename(&mut self, feed_name: &str, name: &str, name_new: &str) -> TokioResult<()> {
        if !self.feed_exists(feed_name) {
            Err(ErrorKind::NotFound.into())
        } else if !self.col_exists(feed_name, name) {
            Err(ErrorKind::NotFound.into())
        } else if self.col_exists(feed_name, name_new) {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // // Close seq file by removing it from seq_mapping
            // self.seq_mapping.get_mut(feed_name).unwrap().remove(name);
            // let mut col_item = self.col_map_mapping.get_mut(feed_name).unwrap().remove(name).unwrap();

            // Close the col
            let mut col_item = self._col_close(feed_name, name);

            // Update col list
            col_item.rename(name_new);
            self.col_list_mapping.get_mut(feed_name).unwrap().modify(&name.to_string(), &col_item).await?;

            // Rename the seq file
            let seq_path = Self::_get_seq_path(&self.path, feed_name, name);
            let seq_path_new = Self::_get_seq_path(&self.path, feed_name, name_new);
            rename(seq_path, seq_path_new.clone()).await?;

            // Open the col
            self._col_open(feed_name, name_new, col_item).await?;

            // // Open seq file by by path
            // let seq = Seq::new(seq_path_new, col_item.datatype.size()).await?;
            // self.col_map_mapping.get_mut(feed_name).unwrap().insert(name_new.to_string(), col_item);
            // self.seq_mapping.get_mut(feed_name).unwrap().insert(name_new.to_string(), seq);

            Ok(())
        }
    }

    pub async fn col_add(&mut self, feed_name: &str, col_name: &str, datatype: &str) -> TokioResult<()> {
        if !self.feed_exists(feed_name) {
            Err(ErrorKind::NotFound.into())
        } else if self.col_exists(feed_name, col_name) {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Create col item
            let col_item = ColItem::new(col_name, datatype);

            // Add col item in the list
            self.col_list_mapping.get_mut(feed_name).unwrap().add(&col_item).await?;

            // Open the col
            self._col_open(feed_name, col_name, col_item).await?;

            // Resize the seq
            let size = self.feed_map[feed_name].size;
            let seq = &self.seq_mapping.get_mut(feed_name).unwrap()[col_name];
            seq.resize(size).await?;

            // // Create a seq for the col and set the necessary size
            // let seq_path = Self::_get_seq_path(&self.path, feed_name, col_name);
            // let seq = Seq::new(seq_path, col_item.datatype.size()).await?;
            // seq.resize(size).await?;

            // // Add col item in the list
            // self.col_list_mapping.get_mut(feed_name).unwrap().add(&col_item).await?;

            // // Update the mappings
            // self.col_map_mapping.get_mut(feed_name).unwrap().insert(col_name.to_string(), col_item);
            // self.seq_mapping.get_mut(feed_name).unwrap().insert(col_name.to_string(), seq);

            Ok(())
        }
    }

    pub async fn col_remove(&mut self, feed_name: &str, col_name: &str) -> TokioResult<()> {
        if !self.feed_exists(feed_name) {
            Err(ErrorKind::NotFound.into())
        } else if !self.col_exists(feed_name, col_name) {
            Err(ErrorKind::NotFound.into())
        } else {
            // // Update the mappings
            // self.col_map_mapping.get_mut(feed_name).unwrap().remove(col_name);
            // self.seq_mapping.get_mut(feed_name).unwrap().remove(col_name);

            // Close the col
            self._col_close(feed_name, col_name);

            // Remove col item from the list
            self.col_list_mapping.get_mut(feed_name).unwrap().remove(&col_name.to_string()).await?;

            // Remove seq file
            let seq_path = Self::_get_seq_path(&self.path, feed_name, col_name);
            tokio::fs::remove_file(seq_path).await?;

            Ok(())
        }
    }

    pub fn size_get(&self, feed_name: &str) -> TokioResult<usize> {
        self.feed_map.get(feed_name)
            .map(|item| item.size)
            .ok_or(ErrorKind::NotFound.into())
    }

    pub async fn size_set(&mut self, feed_name: &str, size: usize) -> TokioResult<usize> {
        // Resize all seq
        for seq in self.seq_mapping[feed_name].values() {
            seq.resize(size).await?;
        }

        // TODO: Do it in parallel
        // let mut js = tokio::task::JoinSet::new();
        // for seq in self.seq_mapping[feed_name].values() {
        //     js.spawn(seq.resize(size));
        // }
        // js.join_all().await;

        // Change the size
        let feed_item = self.feed_map.get_mut(feed_name).unwrap();
        let old_size = feed_item.size;
        feed_item.size = size;
        self.feed_list.modify(&feed_name.to_string(), feed_item).await?;

        // Return
        Ok(old_size)
    }

    pub async fn data_get(&mut self, feed_name: &str, ix: usize, size: usize, cols: &[String]) -> TokioResult<Dataset> {
        let mut ds = HashMap::new();
        for col_name in cols.iter() {
            // Get datatype from col item
            let datatype = self.col_map_mapping[feed_name][col_name]
                .datatype.clone();

            // Get bytes from the seq file
            let block = self.raw_get(
                feed_name, col_name, ix, size * datatype.size()
            ).await?;

            // Convert bytes to a dataset series
            let series = block.chunks(datatype.size())
                .map(|chunk| datatype.from_bytes2(chunk))
                .collect::<Vec<Dataunit>>();

            // Insert series into the dataset
            ds.insert(col_name.clone(), series);
        }
        Ok(ds)
    }

    pub async fn data_push(&mut self, feed_name: &str, ds: &Dataset) -> TokioResult<()> {
        // Get the dataset size
        let size = get_dataset_size(ds)?;

        // If the dataset is not empty
        if size > 0 {
            // Get the current feed size into ix
            let ix = self.feed_map.get_mut(feed_name).unwrap().size;

            // Update the size of all cols
            self.size_set(feed_name, ix + size).await?;

            // Insert the data from the dataset
            self.data_patch(feed_name, ix, ds).await?;
        }

        Ok(())
    }

    pub async fn data_save(&mut self, feed_name: &str, ix: usize, ds: &Dataset) -> TokioResult<()> {
        let cols = self.col_map_mapping[feed_name]
            .keys().cloned().collect::<Vec<String>>();
        self._data_update(feed_name, ix, ds, &cols).await?;
        Ok(())
    }

    pub async fn data_patch(&mut self, feed_name: &str, ix: usize, ds: &Dataset) -> TokioResult<()> {
        let cols = ds.keys().cloned().collect::<Vec<String>>();
        self._data_update(feed_name, ix, ds, &cols).await?;
        Ok(())
    }

    pub async fn raw_get(&mut self, feed_name: &str, col_name: &str, ix: usize, size: usize) -> TokioResult<Vec<u8>> {
        // Get seq object
        let seq = self.seq_mapping.get_mut(feed_name).unwrap()
            .get_mut(col_name).unwrap();

        // Get bytes from the seq file into a buffer
        let mut block = vec![0u8; size];
        seq.get(ix, &mut block).await?;

        Ok(block)
    }

    pub async fn raw_set(&mut self, feed_name: &str, col_name: &str, ix: usize, block: &[u8]) -> TokioResult<()> {
        // Get seq object
        let seq = self.seq_mapping.get_mut(feed_name).unwrap()
            .get_mut(col_name).unwrap();

        // Update the seq file with the block
        seq.update(ix, block).await?;  

        Ok(())
    }

    async fn _data_update(&mut self, feed_name: &str, ix: usize, ds: &Dataset, cols: &[String]) -> TokioResult<()> {
        // Get dataset size, it also check where the dataset is valid: 
        // all series have the same size
        let size = get_dataset_size(ds)?;

        // If the dataset is not empty
        if size > 0 {
            // Iterate the colunms
            // TODO: Do it in parallel
            for col_name in cols.iter() {
                // Update the seq file
                self._seq_update(feed_name, col_name, ix, size, ds.get(col_name)).await?;
            }
        }

        Ok(())
    }

    async fn _seq_update(&mut self, feed_name: &str, col_name: &str, ix: usize, size: usize, series: Option<&Vec<Dataunit>>) -> TokioResult<()> {
        // Get col item because we need the datatype
        let col_item = &self.col_map_mapping[feed_name][col_name];

        // Convert the series into a byte sequence
        let block: Vec<u8> = if let Some(series) = series {
            series.iter()
                .map(|unit| col_item.datatype.to_bytes2(unit).unwrap())
                .collect::<Vec<Vec<u8>>>().concat()
        } else {
            vec![0u8; size * col_item.datatype.size()]
        };

        // Update the seq file
        self.raw_set(feed_name, col_name, ix, &block).await?;

        Ok(())
    }

    async fn _feed_open(&mut self, feed_name: &str, feed_item: FeedItem) -> TokioResult<()> {
        // Open col list file
        let col_list_path = Self::_get_col_list_path(&self.path, feed_name);
        let mut col_list = List::<ColItem, String>::new(col_list_path).await?;
        let col_map = col_list.map().await?;

        // Open all seq files
        self.col_map_mapping.insert(feed_name.to_string(), HashMap::new());
        self.seq_mapping.insert(feed_name.to_string(), HashMap::new());
        for (col_name, col_item) in col_map.into_iter() {
            // // Create a seq
            // let seq_path = Self::_get_seq_path(&self.path, feed_name, col_name);
            // let seq = Seq::new(seq_path, col_item.datatype.size()).await?;

            // // Add the seq to the mapping
            // self.seq_mapping.get_mut(feed_name).unwrap().insert(col_name.clone(), seq);

            self._col_open(feed_name, &col_name, col_item).await?;
        }

        // Update mappings
        self.feed_map.insert(feed_name.to_string(), feed_item);
        self.col_list_mapping.insert(feed_name.to_string(), col_list);
        // self.col_map_mapping.insert(feed_name.to_string(), col_map);

        Ok(())
    }

    fn _feed_close(&mut self, feed_name: &str) -> FeedItem {
        // Close all seq files by removing them from seq_mapping
        self.seq_mapping.remove(feed_name);

        // Close col list file by removing it from col_list_mapping
        self.col_list_mapping.remove(feed_name);
        self.col_map_mapping.remove(feed_name);

        // Update feed list
        self.feed_map.remove(feed_name).unwrap()
    }

    async fn _col_open(&mut self, feed_name: &str, col_name: &str, col_item: ColItem) -> TokioResult<()> {
        // Create a seq for the col and set the necessary size
        let seq_path = Self::_get_seq_path(&self.path, feed_name, col_name);
        let seq = Seq::new(seq_path, col_item.datatype.size()).await?;

        // Update the mappings
        self.col_map_mapping.get_mut(feed_name).unwrap().insert(col_name.to_string(), col_item);
        self.seq_mapping.get_mut(feed_name).unwrap().insert(col_name.to_string(), seq);

        Ok(())
    }

    fn _col_close(&mut self, feed_name: &str, col_name: &str) -> ColItem {
        // Close seq file by removing it from seq_mapping
        self.seq_mapping.get_mut(feed_name).unwrap().remove(col_name);

        // Remove col item from col_map_mapping and return it
        self.col_map_mapping.get_mut(feed_name).unwrap().remove(col_name).unwrap()
    }

    fn _get_feed_list_path(path: &str) -> String {
        path_concat!(path, "feed.list")
    }

    fn _get_col_list_path(path: &str, feed_name: &str) -> String {
        path_concat!(path, feed_name, "col.list")
    }

    fn _get_seq_path(path: &str, feed_name: &str, col_name: &str) -> String {
        path_concat!(path, feed_name, format!("{}.col", col_name))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // #[derive(Debug, Clone)]
    // struct Item1 {
    //     key: [u8; 8],
    //     val: i64,
    // }

    // impl ListKeyTrait<String> for Item1 {
    //     fn key(&self) -> String {
    //         bytes_to_str(&self.key).to_string()
    //     }
    // }

    #[tokio::test]
    async fn test() -> tokio::io::Result<()> {
        let mut conn = Connection::new("./tmp/lb3").await?;

        // conn.feed_add("xyz").await?;
        // conn.feed_remove("xyz").await?;
        // conn.feed_rename("xyz2", "xyz").await?;

        println!("Feed list: {:?}", conn.feed_list());

        // conn.col_add("xyz", "x", "Int64").await?;
        // conn.col_add("xyz", "y", "Float64").await?;
        // conn.col_rename("xyz", "y", "z").await?;
        // conn.col_remove("xyz", "x").await?;

        println!("Col list: {:?}", conn.col_list("xyz")?);

        println!("Size: {:?}", conn.size_get("xyz")?);

        let ds: Dataset = HashMap::from([
            ("x".to_string(), vec![Dataunit::I(2), Dataunit::I(5)]),
            ("y".to_string(), vec![Dataunit::F(2.15), Dataunit::F(5.55)]),
        ]);
        println!("ds = {:?}", ds);

        // conn.data_push("xyz", &ds).await?;

        println!("ds = {:?}", conn.data_get("xyz", 0, 2, &["x".to_string(), "y".to_string()]).await?);

        Ok(())

        // // Seq
        // let mut seq = Seq::new("./tmp/s1", 4).await?;
        // seq.resize(8).await?;
        // seq.push(b"qwer").await?;
        // println!("{:?}", seq.size().await?);
        // let mut block = [0u8; 4];
        // seq.get(5, &mut block).await?;
        // println!("{:?}", std::str::from_utf8(&block).unwrap());
        // seq.update(6, b"aaaa").await?;
        // seq.get(6, &mut block).await?;
        // println!("{:?}", std::str::from_utf8(&block).unwrap());

        // // Col
        // let mut col = Col::<i32>::new("./tmp/c1").await?;
        // col.resize(6).await?;
        // col.push(&25).await?;
        // println!("{:?}", col.size().await?);
        // println!("{:?}", col.get(3).await?);
        // col.update(3, &12).await?;
        // println!("{:?}", col.get(3).await?);
        // println!("{:?}", col.get_many(2, 4).await?);

        // // List
        // let mut lst = List::<Item1, String>::new("./tmp/l1").await?;
        // println!("{:?}", lst.list().await?);

        // lst.add(&Item1 { key: *b"qweasdrf", val: 25 }).await.ok();
        // println!("{:?}", lst.list().await?);

        // lst.modify(&"qweasdrf".to_string(), &Item1 { key: *b"12345678", val: 28 }).await?;
        // println!("{:?}", lst.list().await?);

        // lst.remove(&"12345678".to_string()).await?;
        // println!("{:?}", lst.list().await?);

        // lst.add(&Item1 { key: *b"12345678", val: 28 }).await?;
        // println!("{:?}", lst.list().await?);

        // lst.modify(&"12345678".to_string(), &Item1 { key: *b"qweasdrf", val: 25 }).await?;
        // println!("{:?}", lst.list().await?);

        // println!("{:?}", lst.detail(&"qweasdrf".to_string()).await?.val);
        // println!("{:?}", lst.detail(&"12345678".to_string()).await.err().unwrap().kind());

        // // myfunc(&[("price", &25.0), ("block_num", &6152)]);

        // // Lbase
        // // let mut conn = Lbase::new("./tmp/lbase1").await?;

        // // conn.feed_add("usdt").await.ok();

        // // let feeds: Vec<String> = conn.feed_list().await?;
        // // println!("feeds = {:?}", feeds);

        // // conn.col_add("usdt", ("price", Datatype::Float, 0.0)).await.ok();
        // // conn.col_add("usdt", ("block_num", Datatype::Int, 0)).await.ok();

        // // let cols: Vec<(String, Datatype, &any Any)> = conn.col_list().await?;
        // // println!("cols = {:?}", cols);

        // // let ix = conn.push("usdt", &[
        // //     ("price", vec![25.2, 25.1]),
        // //     ("block_num", vec![6153, 6154]),
        // // ]).await?;

        // // conn.patch("usdt",
        // //     5, 6,
        // //     &[
        // //         ("price", vec![25.5]),
        // //     ]
        // // ).await?;

        // // let data: Dataset = conn.get("usdt", &["price"], 5, 8).await?;
        // // let data: Dataset = conn.get_all("usdt", &["price", "block_num"]).await?;

        // // conn.truncate("usdt", 10).await?;

        // // println!("{:?}", std::mem::size_of::<FeedRecord>());
        // // println!("{:?}", std::mem::size_of::<ColRecord>());

        // // println!("{:?}", from_bytes::<f32>(&[0, 0, 0, 0]));

        // // let mut conn = Connection::new("./tmp/lbase1").await?;

        // // if !conn.feed_mgr.exists("tab1") {
        // //     conn.feed_mgr.add("tab1").await?;
        // // }

        // // println!("{:?}", conn.feed_mgr.list().await?);

        // // conn.feed_mgr.rename("tab1", "tab2").await?;

        // // println!("{:?}", conn.feed_mgr.list().await?);

        // // println!("col_mgr tab2: {:?}", conn.feed_mgr.col_mgr("tab2"));

        // // if conn.feed_mgr.exists("tab2") {
        // //     conn.feed_mgr.remove("tab2").await?;
        // // }

        // // println!("{:?}", conn.feed_mgr.list().await?);

        // // Return
        // Ok(())
    }

    // #[tokio::test]
    // async fn test2() -> tokio::io::Result<()> {
    //     let mut conn = Connection2::new("./tmp/lbase1").await?;

    //     if !conn.feed_exists("tab1") {
    //         conn.feed_add("tab1").await?;
    //     }

    //     println!("{:?}", conn.feed_list().await?);
    //     println!("{:?}", conn.feed_size("tab1"));

    //     // conn.feed_remove("tab1").await?;
    //     // conn.feed_rename("tab1", "tab2").await?;

    //     if !conn.col_exists("tab1", "ts") {
    //         conn.col_add("tab1", "ts", Datatype::Int32).await?;
    //     }

    //     if !conn.col_exists("tab1", "x") {
    //         conn.col_add("tab1", "x", Datatype::Float32).await?;
    //     }

    //     if !conn.col_exists("tab1", "y") {
    //         conn.col_add("tab1", "y", Datatype::Float32).await?;
    //     }

    //     println!("{:?}", conn.col_list("tab1").await?);

    //     // println!("{:?}", conn.col_datatype("tab1", "x").await?);
    //     // conn.col_remove("tab1", "y").await?;
    //     // conn.col_rename("tab1", "x", "z").await?;  // Type cannot be modified because of the block_size

    //     let ds = vec![
    //         ("ts", vec![60, 65]),
    //         ("x", vec![2.5, 3.5]),
    //         ("y", vec![1.0, -1.0]),
    //     ];
    //     conn.data_push("tab1", &ds).await?;  // Works as resize + update
    //     // conn.data_push_one("tab1", &vec![
    //     //      ("ts", 70),
    //     //      ("x", 4.0),
    //     //      ("y", 2.0),
    //     // ]).await?;  // Works as data_push, modifying the dataset

    //     conn.data_update("tab1", 1, &vec![
    //         ("y", vec![-2.0]),
    //     ]).await?;
    //     // conn.data_update_one("tab1", 2, &vec![
    //     //     ("y", -2.0),
    //     // ]).await?;

    //     // conn.feed_resize("tab1", 10).await?;

    //     let ds = conn.data_get("tab1", 1, 2, &["x", "y"]).await?;

    //     let ix = conn.data_search("tab1", "ts", 65).await?;
    //     println!("{:?}", ix);

    //     // ? DB Index (slowing down 'push')
    //     // ? Heap storage

    //     Ok(())
    // }
}
