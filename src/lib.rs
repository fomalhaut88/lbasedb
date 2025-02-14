// #![warn(missing_docs)]
#![feature(test)]
// #![feature(async_closure)]

// use std::fs::exists;
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


#[derive(Clone, Debug)]
pub struct FeedItem {
    name: NameType,
}


impl ListKeyTrait<String> for FeedItem {
    fn key(&self) -> String {
        bytes_to_str(&self.name).to_string()
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

    // Size mapping feed key -> size
    size_mapping: HashMap<String, usize>,
}


impl Connection {
    pub async fn new(path: &str) -> TokioResult<Self> {
        // Ensure the directory
        create_dir_all(path).await?;

        // List of feeds
        let mut feed_list = List::<FeedItem, String>::new(
            Self::_get_feed_list_path(path)
        ).await?;

        // Mapping feed_name -> feed
        let feed_map = feed_list.map().await?;

        // Mapping feed_name -> col_list
        let mut col_list_mapping = HashMap::new();

        // Mapping feed_name -> col_name -> col
        let mut col_map_mapping = HashMap::new();

        // Mapping feed_name -> col_name -> seq
        let mut seq_mapping = HashMap::new();

        // Mapping feed_name -> size
        let mut size_mapping = HashMap::new();

        // Loop for feed names
        for feed_name in feed_map.keys() {
            // List of columns
            let mut col_list = List::<ColItem, String>::new(
                Self::_get_col_list_path(path, feed_name)
            ).await?;

            // Mapping col_name -> col
            let col_map = col_list.map().await?;

            // Mapping col_name -> seq
            seq_mapping.insert(feed_name.clone(), HashMap::new());

            // Loop for cols
            for (col_name, col_rec) in col_map.iter() {
                // Create a seq
                let seq = Seq::new(
                    Self::_get_seq_path(path, feed_name, col_name),
                    col_rec.datatype.size()
                ).await?;

                // Update size
                if !size_mapping.contains_key(feed_name) {
                    size_mapping.insert(feed_name.clone(), seq.size().await?);
                }

                // Add the seq to the mapping
                seq_mapping.get_mut(feed_name).unwrap().insert(col_name.clone(), seq);
            }

            // Update mappings
            col_list_mapping.insert(feed_name.clone(), col_list);
            col_map_mapping.insert(feed_name.clone(), col_map);
        }

        Ok(Self {
            path: path.to_string(),
            feed_list,
            feed_map,
            col_list_mapping,
            col_map_mapping,
            seq_mapping,
            size_mapping,
        })
    }

    pub fn path(&self) -> String {
        self.path.clone()
    }

    pub fn feed_list(&self) -> Vec<String> {
        self.feed_map.keys().cloned().collect()
    }

    pub async fn feed_add(&mut self, feed_name: &str) -> TokioResult<()> {
        // Check whether it exists
        if self.feed_list.exists(&feed_name.to_string()) {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Create directory for the feed
            let feed_path = path_concat!(self.path.clone(), feed_name);
            create_dir_all(feed_path).await?;

            // Insert a new record into the list
            let feed_item = FeedItem { name: str_to_bytes::<256>(&feed_name) };
            self.feed_list.add(&feed_item).await?;

            // Update feed mapping
            self.feed_map.insert(feed_name.to_string(), feed_item);

            Ok(())
        }
    }

    pub async fn feed_remove(&mut self, feed_name: &str) -> TokioResult<()> {
        // Check whether it exists
        if !self.feed_list.exists(&feed_name.to_string()) {
            Err(ErrorKind::NotFound.into())
        } else {
            // Update feed mapping
            self.feed_map.remove(feed_name);

            // Remove from the list
            self.feed_list.remove(&feed_name.to_string()).await?;

            // Remove the directory
            let feed_path = path_concat!(self.path.clone(), feed_name);
            remove_dir_all(feed_path).await?;

            Ok(())
        }
    }

    pub async fn feed_rename(&mut self, feed_name: &str, feed_name_new: &str) -> TokioResult<()> {
        if !self.feed_list.exists(&feed_name.to_string()) {
            Err(ErrorKind::NotFound.into())
        } else if self.feed_list.exists(&feed_name_new.to_string()) {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Update feed list
            let feed_item = FeedItem { name: str_to_bytes::<256>(&feed_name_new) };
            self.feed_list.modify(&feed_name.to_string(), &feed_item).await?;

            // Update feed mapping
            self.feed_map.remove(feed_name);
            self.feed_map.insert(feed_name_new.to_string(), feed_item);

            // Rename the directory
            let feed_path = path_concat!(self.path.clone(), feed_name);
            let feed_path_new = path_concat!(self.path.clone(), feed_name_new);
            rename(feed_path, feed_path_new).await?;

            Ok(())
        }
    }

    pub fn col_list(&self, feed_name: &str) -> Vec<String> {
        self.col_map_mapping[feed_name].keys().cloned().collect()
    }

    pub async fn col_rename(&mut self, feed_name: &str, name: &str, new_name: &str) -> TokioResult<()> {
        if name != new_name {
            if self.col_map_mapping[feed_name].contains_key(new_name) {
                Err(ErrorKind::AlreadyExists.into())
            } else {
                // Remove old seq object in seq_mapping
                // Rename seq file                
                // Create a new seq object in seq_mapping
                // Update in col_list_mapping
                // Update in col_map_mapping

                let seq = self.seq_mapping.get_mut(feed_name).unwrap().remove(name).unwrap();

                // // Create a new seq object
                // let seq = Seq::new(
                //     Self::_get_seq_path(&self.path, feed_name, new_name),
                //     datatype.size()
                // ).await?;

                self.seq_mapping.get_mut(feed_name).unwrap().insert(new_name.to_string(), seq);

                Ok(())
            }
        } else {
            Ok(())
        }
    }

    pub async fn col_add(&mut self, feed_name: &str, name: &str, datatype: Datatype) -> TokioResult<()> {
        if self.col_map_mapping[feed_name].contains_key(name) {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Create a new seq object
            let seq = Seq::new(
                Self::_get_seq_path(&self.path, feed_name, name),
                datatype.size()
            ).await?;

            // Resize seq object to match the other cols
            let size = self.size_mapping[feed_name];
            seq.resize(size).await?;

            // Update in col_list_mapping
            let col_item = ColItem {
                name: str_to_bytes::<256>(&name),
                datatype,
            };
            self.col_list_mapping.get_mut(feed_name).unwrap().add(&col_item).await?;

            // Add the seq to seq_mapping
            self.seq_mapping.get_mut(feed_name).unwrap().insert(name.to_string(), seq);

            // Update in col_map_mapping
            self.col_map_mapping.get_mut(feed_name).unwrap().insert(name.to_string(), col_item);

            Ok(())
        }
    }

    pub async fn col_remove(&mut self, feed_name: &str, name: &str) -> TokioResult<()> {
        if !self.col_map_mapping[feed_name].contains_key(name) {
            Err(ErrorKind::NotFound.into())
        } else {
            // Update in col_map_mapping
            self.col_map_mapping.get_mut(feed_name).unwrap().remove(name);

            // Remove old seq object in seq_mapping
            self.seq_mapping.get_mut(feed_name).unwrap().remove(&name.to_string());

            // Update in col_list_mapping
            self.col_list_mapping.get_mut(feed_name).unwrap().remove(&name.to_string()).await?;

            // Remove seq file
            tokio::fs::remove_file(
                Self::_get_seq_path(&self.path, feed_name, name)
            ).await?;

            Ok(())
        }
    }

    pub fn size_get(&self, feed_name: &str) -> usize {
        self.size_mapping[feed_name]
    }

    pub async fn size_set(&mut self, feed_name: &str, size: usize) -> TokioResult<()> {
        // Resize all seq
        for seq in self.seq_mapping[feed_name].values() {
            seq.resize(size).await?;
        }
        // let mut js = JoinSet::new();
        // for seq in self.seq_mapping[feed_name].values() {
        //     js.spawn(seq.resize(size));
        // }
        // js.join_all().await;

        // Change the size
        *self.size_mapping.get_mut(feed_name).unwrap() = size;

        // Return
        Ok(())
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

    #[derive(Debug, Clone)]
    struct Item1 {
        key: [u8; 8],
        val: i64,
    }

    impl ListKeyTrait<String> for Item1 {
        fn key(&self) -> String {
            bytes_to_str(&self.key).to_string()
        }
    }

    #[tokio::test]
    async fn test() -> tokio::io::Result<()> {
        let mut conn = Connection::new("./tmp/lb2").await?;

        // conn.feed_add("fake_xyz").await?;
        // conn.feed_remove("fake_xyz").await?;
        // conn.feed_rename("fake_xyz1", "fake_xyz").await?;

        println!("Feed list: {:?}", conn.feed_list());

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
