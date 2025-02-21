//! `Conn` is a basic structure for the connection that provides the full
//! interface to the DBMS.

use std::collections::HashMap;

use tokio::io::Result as TokioResult;
// use tokio::task::JoinSet;
use tokio::io::ErrorKind;
use tokio::fs::{create_dir_all, remove_dir_all, rename};
use tokio::sync::Mutex;

use crate::path_concat;
use crate::seq::Seq;
use crate::list::List;
use crate::items::{FeedItem, ColItem};
use crate::datatype::Dataunit;
use crate::dataset::{Dataset, get_dataset_size};


/// Connection object that manages all the entities. Since it interacts with 
/// the file system and supports asynchronous interface, there is no need 
/// to use it in a multi threading way.
pub struct Conn {
    // Path to the directory where the data and settings are stored
    path: String,

    // Feed list object to manage the feeds options
    feed_list: Mutex<List<FeedItem, String>>,

    // Feed mapping feed key -> feed
    feed_map: Mutex<HashMap<String, FeedItem>>,

    // Col list objects that is a mapping feed key -> the list
    col_list_mapping: Mutex<HashMap<String, List<ColItem, String>>>,

    // Col mapping as double map feed key -> col key -> col
    col_map_mapping: Mutex<HashMap<String, HashMap<String, ColItem>>>,

    // Seq mapping as double map feed key -> col key -> seq
    seq_mapping: Mutex<HashMap<String, HashMap<String, Mutex<Seq>>>>,
}


impl Conn {
    /// Create a connection giving the path to the directory to store the data.
    /// If the path does not exist, the directory will be created.
    pub async fn new(path: &str) -> TokioResult<Self> {
        // Ensure the directory
        create_dir_all(path).await?;

        // List of feeds
        let feed_list = List::<FeedItem, String>::new(
            Self::_get_feed_list_path(path)
        ).await?;

        // Create instance
        let instance = Self {
            path: path.to_string(),
            feed_list: Mutex::new(feed_list),
            feed_map: Mutex::new(HashMap::new()),
            col_list_mapping: Mutex::new(HashMap::new()),
            col_map_mapping: Mutex::new(HashMap::new()),
            seq_mapping: Mutex::new(HashMap::new()),
        };

        // Open all feeds
        let feed_map = instance.feed_list.lock().await.map().await?;
        for (feed_name, feed_item) in feed_map.into_iter() {
            instance._feed_open(&feed_name, feed_item).await?;
        }

        Ok(instance)
    }

    /// Path to the data
    pub fn path(&self) -> String {
        self.path.clone()
    }

    /// List the feeds.
    pub async fn feed_list(&self) -> Vec<FeedItem> {
        self.feed_map.lock().await.values().cloned().collect()
    }

    /// Check if the feed exists.
    pub async fn feed_exists(&self, feed_name: &str) -> bool {
        self.feed_map.lock().await.contains_key(feed_name)
    }

    /// Add a new feed by its name.
    pub async fn feed_add(&self, feed_name: &str) -> TokioResult<()> {
        // Check whether it exists
        if self.feed_exists(feed_name).await {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Create directory for the feed
            let feed_path = path_concat!(self.path.clone(), feed_name);
            create_dir_all(feed_path).await?;

            // Insert a new record into the list
            let feed_item = FeedItem::new(feed_name);
            self.feed_list.lock().await.add(&feed_item).await?;

            // Open the feed
            self._feed_open(feed_name, feed_item).await?;

            Ok(())
        }
    }

    /// Remove the feed by its name.
    pub async fn feed_remove(&self, feed_name: &str) -> TokioResult<()> {
        // Check whether it exists
        if !self.feed_exists(feed_name).await {
            Err(ErrorKind::NotFound.into())
        } else {
            // Close the feed
            self._feed_close(feed_name).await;

            // Remove from the list
            self.feed_list.lock().await.remove(&feed_name.to_string()).await?;

            // Remove the directory
            let feed_path = path_concat!(self.path.clone(), feed_name);
            remove_dir_all(feed_path).await?;

            Ok(())
        }
    }

    /// Rename the feed.
    pub async fn feed_rename(&self, name: &str, name_new: &str) -> 
                             TokioResult<()> {
        if !self.feed_exists(name).await {
            Err(ErrorKind::NotFound.into())
        } else if self.feed_exists(name_new).await {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Close the feed
            let mut feed_item = self._feed_close(name).await;

            // Update feed list
            feed_item.rename(name_new);
            self.feed_list.lock().await
                .modify(&name.to_string(), &feed_item).await?;

            // Rename the directory
            let feed_path = path_concat!(self.path.clone(), name);
            let feed_path_new = path_concat!(self.path.clone(), name_new);
            rename(feed_path, feed_path_new).await?;

            // Open the feed
            self._feed_open(name_new, feed_item).await?;

            Ok(())
        }
    }

    /// List columns of the feed.
    pub async fn col_list(&self, feed_name: &str) -> TokioResult<Vec<ColItem>> {
        self.col_map_mapping.lock().await.get(feed_name)
            .map(|item| item.values().cloned().collect())
            .ok_or(ErrorKind::NotFound.into())
    }

    /// Check if the column exists in the feed.
    pub async fn col_exists(&self, feed_name: &str, col_name: &str) -> bool {
        self.col_map_mapping.lock().await[feed_name].contains_key(col_name)
    }

    /// Rename the column
    pub async fn col_rename(&self, feed_name: &str, name: &str, 
                            name_new: &str) -> TokioResult<()> {
        if !self.feed_exists(feed_name).await {
            Err(ErrorKind::NotFound.into())
        } else if !self.col_exists(feed_name, name).await {
            Err(ErrorKind::NotFound.into())
        } else if self.col_exists(feed_name, name_new).await {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Close the col
            let mut col_item = self._col_close(feed_name, name).await;

            // Update col list
            col_item.rename(name_new);
            self.col_list_mapping.lock().await.get_mut(feed_name).unwrap()
                .modify(&name.to_string(), &col_item).await?;

            // Rename the seq file
            let seq_path = Self::_get_seq_path(&self.path, feed_name, name);
            let seq_path_new = Self::_get_seq_path(&self.path, feed_name, 
                                                   name_new);
            rename(seq_path, seq_path_new.clone()).await?;

            // Open the col
            self._col_open(feed_name, name_new, col_item).await?;

            Ok(())
        }
    }

    /// Add a new column by its name and datatype.
    pub async fn col_add(&self, feed_name: &str, col_name: &str, 
                         datatype: &str) -> TokioResult<()> {
        if !self.feed_exists(feed_name).await {
            Err(ErrorKind::NotFound.into())
        } else if self.col_exists(feed_name, col_name).await {
            Err(ErrorKind::AlreadyExists.into())
        } else {
            // Create col item
            let col_item = ColItem::new(col_name, datatype);

            // Add col item in the list
            self.col_list_mapping.lock().await.get_mut(feed_name).unwrap()
                .add(&col_item).await?;

            // Open the col
            self._col_open(feed_name, col_name, col_item).await?;

            // Resize the seq
            let size = self.feed_map.lock().await[feed_name].size;
            let seq = &self.seq_mapping.lock().await[feed_name][col_name];
            seq.lock().await.resize(size).await?;

            Ok(())
        }
    }

    /// Remove the column.
    pub async fn col_remove(&self, feed_name: &str, col_name: &str) -> 
                            TokioResult<()> {
        if !self.feed_exists(feed_name).await {
            Err(ErrorKind::NotFound.into())
        } else if !self.col_exists(feed_name, col_name).await {
            Err(ErrorKind::NotFound.into())
        } else {
            // Close the col
            self._col_close(feed_name, col_name).await;

            // Remove col item from the list
            self.col_list_mapping.lock().await.get_mut(feed_name).unwrap()
                .remove(&col_name.to_string()).await?;

            // Remove seq file
            let seq_path = Self::_get_seq_path(&self.path, feed_name, col_name);
            tokio::fs::remove_file(seq_path).await?;

            Ok(())
        }
    }

    /// Get the size of the feed.
    pub async fn size_get(&self, feed_name: &str) -> TokioResult<usize> {
        self.feed_map.lock().await.get(feed_name)
            .map(|item| item.size)
            .ok_or(ErrorKind::NotFound.into())
    }

    /// Change the size of the feed including the sizes of all column files.
    pub async fn size_set(&self, feed_name: &str, size: usize) -> 
                          TokioResult<usize> {
        // Resize all seq
        for seq in self.seq_mapping.lock().await[feed_name].values() {
            seq.lock().await.resize(size).await?;
        }

        // TODO: Do it in parallel
        // let mut js = tokio::task::JoinSet::new();
        // for seq in self.seq_mapping[feed_name].values() {
        //     js.spawn(seq.resize(size));
        // }
        // js.join_all().await;

        // Change the size
        let mut feed_map = self.feed_map.lock().await;
        let feed_item = feed_map.get_mut(feed_name).unwrap();
        let old_size = feed_item.size;
        feed_item.size = size;
        self.feed_list.lock().await
            .modify(&feed_name.to_string(), feed_item).await?;

        // Return
        Ok(old_size)
    }

    /// Get dataset stored in the feed `feed_name`, having the size `size`
    /// and the columns `cols` with the offset `ix`.
    pub async fn data_get(&self, feed_name: &str, ix: usize, size: usize, 
                          cols: &[String]) -> TokioResult<Dataset> {
        let mut ds = HashMap::new();
        for col_name in cols.iter() {
            // Get datatype from col item
            let datatype = self.col_map_mapping.lock().await
                [feed_name][col_name].datatype.clone();

            // Get bytes from the seq file
            let block = self.raw_get(
                feed_name, col_name, ix, size * datatype.size()
            ).await?;

            // Convert bytes to a dataset series
            let series = block.chunks(datatype.size())
                .map(|chunk| datatype.from_bytes(chunk))
                .collect::<Vec<Dataunit>>();

            // Insert series into the dataset
            ds.insert(col_name.clone(), series);
        }
        Ok(ds)
    }

    /// Push the dataset to the feed. The missed columns will be zeros.
    pub async fn data_push(&self, feed_name: &str, ds: &Dataset) -> 
                           TokioResult<()> {
        // Get the dataset size
        let size = get_dataset_size(ds)?;

        // If the dataset is not empty
        if size > 0 {
            // Get the current feed size into ix
            let ix = self.feed_map.lock().await[feed_name].size;

            // Update the size of all cols
            self.size_set(feed_name, ix + size).await?;

            // Insert the data from the dataset
            self.data_patch(feed_name, ix, ds).await?;
        }

        Ok(())
    }

    /// Update the records in the feed with the given dataset. The missing
    /// columns will be filled with zeros. For preventing it use `data_patch`
    /// instead.
    pub async fn data_save(&self, feed_name: &str, ix: usize, 
                           ds: &Dataset) -> TokioResult<()> {
        let cols = self.col_map_mapping.lock().await[feed_name]
            .keys().cloned().collect::<Vec<String>>();
        self._data_update(feed_name, ix, ds, &cols).await?;
        Ok(())
    }

    /// Update the records in the feed with the given dataset. The missing
    /// columns will no change. For making them zero use `data_save`
    /// instead.
    pub async fn data_patch(&self, feed_name: &str, ix: usize, 
                            ds: &Dataset) -> TokioResult<()> {
        let cols = ds.keys().cloned().collect::<Vec<String>>();
        self._data_update(feed_name, ix, ds, &cols).await?;
        Ok(())
    }

    /// Get raw bytes have the size `size` (in bytes) of the column `col_name`
    /// in the feed `feed_name` with the offset `ix`.
    pub async fn raw_get(&self, feed_name: &str, col_name: &str, ix: usize, 
                         size: usize) -> TokioResult<Vec<u8>> {
        // Get seq object
        let seq = &self.seq_mapping.lock().await[feed_name][col_name];

        // Get bytes from the seq file into a buffer
        let mut block = vec![0u8; size];
        seq.lock().await.get(ix, &mut block).await?;

        Ok(block)
    }

    /// Update raw bytes from the `block` in the column `col_name` 
    /// of the feed `feed_name` with the offset `ix`.
    pub async fn raw_set(&self, feed_name: &str, col_name: &str, ix: usize, 
                         block: &[u8]) -> TokioResult<()> {
        // Get seq object
        let seq = &self.seq_mapping.lock().await[feed_name][col_name];

        // Update the seq file with the block
        seq.lock().await.update(ix, block).await?;  

        Ok(())
    }

    async fn _data_update(&self, feed_name: &str, ix: usize, ds: &Dataset, 
                          cols: &[String]) -> TokioResult<()> {
        // Get dataset size, it also check where the dataset is valid: 
        // all series have the same size
        let size = get_dataset_size(ds)?;

        // If the dataset is not empty
        if size > 0 {
            // Iterate the colunms
            // TODO: Do it in parallel
            for col_name in cols.iter() {
                // Update the seq file
                self._seq_update(feed_name, col_name, ix, size, 
                                 ds.get(col_name)).await?;
            }
        }

        Ok(())
    }

    async fn _seq_update(&self, feed_name: &str, col_name: &str, ix: usize, 
                         size: usize, series: Option<&Vec<Dataunit>>) -> 
                         TokioResult<()> {
        // Get col item because we need the datatype
        let col_item = &self.col_map_mapping.lock().await[feed_name][col_name];

        // Convert the series into a byte sequence
        let block: Vec<u8> = if let Some(series) = series {
            series.iter()
                .map(|unit| col_item.datatype.to_bytes(unit).unwrap())
                .collect::<Vec<Vec<u8>>>().concat()
        } else {
            vec![0u8; size * col_item.datatype.size()]
        };

        // Update the seq file
        self.raw_set(feed_name, col_name, ix, &block).await?;

        Ok(())
    }

    async fn _feed_open(&self, feed_name: &str, feed_item: FeedItem) -> 
                        TokioResult<()> {
        // Open col list file
        let col_list_path = Self::_get_col_list_path(&self.path, feed_name);
        let mut col_list = List::<ColItem, String>::new(col_list_path).await?;
        let col_map = col_list.map().await?;

        // Open all seq files
        self.col_map_mapping.lock().await
            .insert(feed_name.to_string(), HashMap::new());
        self.seq_mapping.lock().await
            .insert(feed_name.to_string(), HashMap::new());
        for (col_name, col_item) in col_map.into_iter() {
            self._col_open(feed_name, &col_name, col_item).await?;
        }

        // Update mappings
        self.feed_map.lock().await.insert(feed_name.to_string(), feed_item);
        self.col_list_mapping.lock().await
            .insert(feed_name.to_string(), col_list);
        
        Ok(())
    }

    async fn _feed_close(&self, feed_name: &str) -> FeedItem {
        // Close all seq files by removing them from seq_mapping
        self.seq_mapping.lock().await.remove(feed_name);

        // Close col list file by removing it from col_list_mapping
        self.col_list_mapping.lock().await.remove(feed_name);
        self.col_map_mapping.lock().await.remove(feed_name);

        // Update feed list
        self.feed_map.lock().await.remove(feed_name).unwrap()
    }

    async fn _col_open(&self, feed_name: &str, col_name: &str, 
                       col_item: ColItem) -> TokioResult<()> {
        // Create a seq for the col and set the necessary size
        let seq_path = Self::_get_seq_path(&self.path, feed_name, col_name);
        let seq = Seq::new(seq_path, col_item.datatype.size()).await?;

        // Update the mappings
        self.col_map_mapping.lock().await.get_mut(feed_name).unwrap()
            .insert(col_name.to_string(), col_item);
        self.seq_mapping.lock().await.get_mut(feed_name).unwrap()
            .insert(col_name.to_string(), Mutex::new(seq));

        Ok(())
    }

    async fn _col_close(&self, feed_name: &str, col_name: &str) -> ColItem {
        // Close seq file by removing it from seq_mapping
        self.seq_mapping.lock().await.get_mut(feed_name).unwrap()
            .remove(col_name);

        // Remove col item from col_map_mapping and return it
        self.col_map_mapping.lock().await.get_mut(feed_name).unwrap()
            .remove(col_name).unwrap()
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
