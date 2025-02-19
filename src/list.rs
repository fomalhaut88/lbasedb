use std::path::Path;
use std::hash::Hash;
use std::collections::HashMap;

use tokio::io::Result as TokioResult;
use tokio::io::ErrorKind;

use crate::col::Col;


/// Key trait so a struct can be stored in `List`.
pub trait ListKeyTrait<K> {
    /// Key function to calculate the unique key of a record.
    fn key(&self) -> K;
}


/// `List` implements methods to work with small lists stored as a `Col` object.
/// `List` keeps index map in the memory to reduce the access to the data
/// in the file, so if there are too many records, `List` object may be 
/// consuming. The main purpose of `List` the is inner data management between
/// files, data types, structeres and so on in the DBSM.
pub struct List<T, K> {
    col: Col<T>,
    ixmap: HashMap<K, usize>,
}


impl<K: Clone + Eq + Hash, T: Clone + ListKeyTrait<K>> List<T, K> {
    /// Create a new `List` object located at `path`.
    pub async fn new(path: impl AsRef<Path>) -> TokioResult<Self> {
        let mut col = Col::<T>::new(path).await?;
        let ixmap = Self::_build_ixmap(&mut col).await?;
        Ok(Self { col, ixmap })
    }

    /// Check whether the key exists.
    pub fn exists(&self, key: &K) -> bool {
        self.ixmap.contains_key(key)
    }

    /// Size of the list.
    pub async fn size(&self) -> TokioResult<usize> {
        self.col.size().await
    }

    /// List all records as a vector.
    pub async fn list(&mut self) -> TokioResult<Vec<T>> {
        self.col.get_all().await
    }

    /// Mapping of all records by key.
    pub async fn map(&mut self) -> TokioResult<HashMap<K, T>> {
        Ok(
            self.col.get_all().await?
                .into_iter()
                .map(|rec| (rec.key(), rec))
                .collect()
        )
    }

    /// Get record by key.
    pub async fn detail(&mut self, key: &K) -> TokioResult<T> {
        if let Some(&ix) = self.ixmap.get(key) {
            self.col.get(ix).await
        } else {
            Err(ErrorKind::NotFound.into())
        }
    }

    /// Add a new record.
    pub async fn add(&mut self, rec: &T) -> TokioResult<()> {
        let key = rec.key();
        if !self.ixmap.contains_key(&key) {
            let ix = self.col.push(rec).await?;
            self.ixmap.insert(key, ix);
            Ok(())
        } else {
            Err(ErrorKind::AlreadyExists.into())
        }
    }

    /// Remove the record by key.
    pub async fn remove(&mut self, key: &K) -> TokioResult<()> {
        if let Some(&ix) = self.ixmap.get(key) {
            let size = self.col.size().await?;
            let rec = self.col.get(size - 1).await?;
            self.col.update(ix, &rec).await?;
            self.col.resize(size - 1).await?;
            self.ixmap.remove(key);
            Ok(())
        } else {
            Err(ErrorKind::NotFound.into())
        }
    }

    /// Modify record by key.
    pub async fn modify(&mut self, key: &K, rec: &T) -> TokioResult<()> {
        if let Some(&ix) = self.ixmap.get(key) {
            let new_key = rec.key();

            if &new_key == key {
                self.col.update(ix, rec).await?;
                Ok(())
            } else if self.ixmap.contains_key(&new_key) {
                Err(ErrorKind::AlreadyExists.into())
            } else {
                self.col.update(ix, rec).await?;
                self.ixmap.remove(key);
                self.ixmap.insert(new_key, ix);
                Ok(())
            }
        } else {
            Err(ErrorKind::NotFound.into())
        }
    }

    async fn _build_ixmap(col: &mut Col<T>) -> TokioResult<HashMap<K, usize>> {
        Ok(col.get_all().await?
                .iter().enumerate()
                .map(|(ix, rec)| (rec.key(), ix))
                .collect()
        )
    }
}
