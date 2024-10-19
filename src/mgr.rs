use std::fs::exists;
use std::collections::HashMap;

use tokio::io::Result as TokioResult;
use tokio::fs::{create_dir, rename, remove_dir_all};

use crate::utils::{str_to_bytes, bytes_to_str, path_concat};
use crate::datatype::Datatype;
use crate::list::{List, ListKeyTrait};


#[derive(Debug, Clone)]
pub struct ColRecord {
    name: [u8; 256],
    datatype: Datatype,
}


impl ColRecord {
    pub fn new(name: &str, datatype: Datatype) -> Self {
        Self { name: str_to_bytes(name), datatype }
    }
}


impl ListKeyTrait<String> for ColRecord {
    fn key(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }
}


#[derive(Debug, Clone)]
pub struct FeedRecord {
    name: [u8; 256],
}


impl FeedRecord {
    pub fn new(name: &str) -> Self {
        Self { name: str_to_bytes(name) }
    }
}


impl ListKeyTrait<String> for FeedRecord {
    fn key(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }
}


#[derive(Debug)]
pub struct ColMgr {
    path: String,
    list: List<ColRecord, String>,
}


impl ColMgr {
    pub async fn new(path: &str) -> TokioResult<Self> {
        if !exists(path)? {
            create_dir(path).await?;
        }
        let mgr_filepath = path_concat(path, "col.mgr");
        let mut list = List::new(mgr_filepath).await?;
        Ok(Self { path: path.to_string(), list })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn exists(&self, name: &str) -> bool {
        self.list.exists(&name.to_string())
    }

    pub async fn list(&mut self) -> TokioResult<Vec<ColRecord>> {
        self.list.list().await
    }

    pub async fn add(&mut self, name: &str, datatype: Datatype) -> TokioResult<()> {
        // Add record to the list
        let rec = ColRecord::new(name, datatype);
        self.list.add(&rec).await?;

        // Ensure ColMgr
        let col_path = path_concat(&self.path, &name);
        // ...create Col file

        Ok(())
    }

    pub async fn remove(&mut self, name: &str) -> TokioResult<()> {
        // Remove ColMgr
        // ...remove Col file

        // Remove the record from the list
        self.list.remove(&name.to_string()).await?;

        Ok(())
    }

    pub async fn rename(&mut self, name: &str, new_name: &str) -> TokioResult<()> {
        // ...rename record in the list
        // ...rename Col file
        Ok(())
    }
}


#[derive(Debug)]
pub struct FeedMgr {
    path: String,
    list: List<FeedRecord, String>,
    col_mgr: HashMap<String, ColMgr>,
}


impl FeedMgr {
    pub async fn new(path: &str) -> TokioResult<Self> {
        if !exists(path)? {
            create_dir(path).await?;
        }
        let mgr_filepath = path_concat(path, "feed.mgr");
        let mut list = List::new(mgr_filepath).await?;
        let mut instance = Self { path: path.to_string(), list, col_mgr: HashMap::new() };
        instance._load_col_mgr().await?;
        Ok(instance)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn col_mgr(&mut self, feed: &str) -> Option<&mut ColMgr> {
        self.col_mgr.get_mut(feed)
    }

    pub fn exists(&self, feed: &str) -> bool {
        self.list.exists(&feed.to_string())
    }

    pub async fn list(&mut self) -> TokioResult<Vec<FeedRecord>> {
        self.list.list().await
    }

    pub async fn add(&mut self, feed: &str) -> TokioResult<()> {
        // Add record to the list
        let rec = FeedRecord::new(feed);
        self.list.add(&rec).await?;

        // Ensure ColMgr
        let col_path = path_concat(&self.path, &feed);
        let mgr = ColMgr::new(&col_path).await?;
        self.col_mgr.insert(feed.to_string(), mgr);

        Ok(())
    }

    pub async fn remove(&mut self, feed: &str) -> TokioResult<()> {
        // Remove ColMgr
        let mut mgr = self.col_mgr.get_mut(feed).unwrap();
        remove_dir_all(mgr.path()).await?;
        self.col_mgr.remove(feed);

        // Remove the record from the list
        self.list.remove(&feed.to_string()).await?;

        Ok(())
    }

    pub async fn rename(&mut self, feed: &str, new_feed: &str) -> TokioResult<()> {
        // Modify the record from the list
        let rec = FeedRecord::new(new_feed);
        self.list.modify(&feed.to_string(), &rec).await?;

        // Move the path
        let col_path = path_concat(&self.path, &feed);
        let new_col_path = path_concat(&self.path, &new_feed);
        rename(&col_path, &new_col_path).await?;

        // Update col_mgr
        let new_mgr = ColMgr::new(&new_col_path).await?;
        self.col_mgr.remove(feed);
        self.col_mgr.insert(new_feed.to_string(), new_mgr);

        Ok(())
    }

    pub async fn size(&self, feed: &str) -> TokioResult<usize> {
        Ok(0)
    }

    async fn _load_col_mgr(&mut self) -> TokioResult<()> {
        self.col_mgr.clear();

        for rec in self.list().await?.iter() {
            let feed = rec.key();
            let col_path = path_concat(&self.path, &feed);
            let mgr = ColMgr::new(&col_path).await?;
            self.col_mgr.insert(feed, mgr);
        }

        Ok(())
    }
}
