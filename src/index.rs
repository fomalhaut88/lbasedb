//! This module contains a hash index implemented as a binary tree,
//! where each node stores a hash value and the indices of its left
//! and right children for smaller and greater hash values.
//! The index helps find the position of a value with O(log N)
//! complexity. It should be used together with the corresponding
//! column or sequence, so it is important not to forget to update
//! its size or rebuild the tree when values are modified.
//! Note: since the rest of the structures are optimized for O(1)
//! insertion, maintaining the index increases insertion complexity
//! to O(log N), which may be undesirable.
//!
//! ```ignored
//! use lbasedb::index::{Index, calc_hash};
//!
//! let mut index = Index::new("./tmp/nums.index").await?;
//!
//! index.push(0, calc_hash(25i64)).await?;
//! index.push(1, calc_hash(36i64)).await?;
//! index.push(2, calc_hash(112i64)).await?;
//!
//! assert_eq!(index.get_any(calc_hash(&1i64)).await?, None);
//! assert_eq!(index.get_any(calc_hash(&36i64)).await?, Some(1));
//! ```

use std::path::Path;
use std::hash::{DefaultHasher, Hash, Hasher};

use tokio::io::{Result as TokioResult};

use crate::utils::{from_bytes, to_bytes};
use crate::seq::Seq;


const UNSET_VALUE: u64 = u64::MAX;


/// Index item that implements the tree node.
#[derive(Debug, Clone)]
pub struct IndexItem {
    ix: u64,
    hash: u64,
    left: u64,
    right: u64,
}


impl IndexItem {
    /// Create a new index item to store hash and location `ix`.
    fn new(ix: u64, hash: u64) -> Self {
        Self {
            ix, hash,
            left: UNSET_VALUE,
            right: UNSET_VALUE,
        }
    }
}


/// Index file-based structure that supports asynchronous interface.
pub struct Index {
    seq: Seq,
}


impl Index {
    /// Create a new index located at the given path.
    pub async fn new(path: impl AsRef<Path>) -> TokioResult<Self> {
        let seq = Seq::new(path, size_of::<IndexItem>()).await?;
        Ok(Self { seq })
    }

    /// Insert a new record for given hash.
    pub async fn push(&mut self, ix: usize, hash: u64) -> TokioResult<()> {
        let pos = self.seq.size().await? as u64;

        if let Some((parent_pos, mut parent_item)) =
                    self.find_last(hash).await {
            if hash < parent_item.hash {
                parent_item.left = pos;
            } else {
                parent_item.right = pos;
            }
            let parent_block = to_bytes(&parent_item);
            self.seq.update(parent_pos as usize, parent_block).await?;
        }

        let item = IndexItem::new(ix as u64, hash);
        let block = to_bytes(&item);
        self.seq.push(block).await?;

        Ok(())
    }

    /// Update hash of an element.
    pub async fn update(&mut self, ix: usize, hash_old: u64,
                        hash_new: u64) -> TokioResult<bool> {
        if self.remove(ix, hash_old).await? {
            self.push(ix, hash_new).await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Remove record from index.
    pub async fn remove(&mut self, ix: usize, hash: u64) ->
                        TokioResult<bool> {
        for (pos, mut item) in self.find_all(hash).await? {
            if item.ix == ix as u64 {
                item.ix = UNSET_VALUE;
                let block = to_bytes(&item);
                self.seq.update(pos as usize, &block).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get location of an element by its hash.
    pub async fn get_any(&mut self, hash: u64) -> TokioResult<Option<usize>> {
        for (_, item) in self.find_all(hash).await? {
            if item.ix != UNSET_VALUE {
                return Ok(Some(item.ix as usize));
            }
        }
        Ok(None)
    }

    /// Get locations of all elements by given hash.
    pub async fn get_all(&mut self, hash: u64) -> TokioResult<Vec<usize>> {
        Ok(self.find_all(hash).await?
               .into_iter()
               .filter(|(_, item)| item.ix != UNSET_VALUE)
               .map(|(_, item)| item.ix as usize)
               .collect())
    }

    /// Find last item by hash to join a new one. Hash may differ.
    async fn find_last(&mut self, hash: u64) -> Option<(u64, IndexItem)> {
        let mut block = vec![0u8; self.seq.block_size()];
        let mut pos: u64 = 0;

        while self.seq.get(pos as usize, &mut block).await.is_ok() {
            let item: &IndexItem = from_bytes(&block);

            if hash < item.hash {
                if item.left == UNSET_VALUE {
                    return Some((pos, item.clone()));
                } else {
                    pos = item.left;
                }
            } else {
                if item.right == UNSET_VALUE {
                    return Some((pos, item.clone()));
                } else {
                    pos = item.right;
                }
            }
        }

        None
    }

    /// Find all positions and items for given hash as a vector including
    /// removed ones.
    async fn find_all(&mut self, hash: u64) ->
                      TokioResult<Vec<(u64, IndexItem)>> {
        let mut res = Vec::new();

        let mut block = vec![0u8; self.seq.block_size()];
        let mut pos: u64 = 0;

        while self.seq.get(pos as usize, &mut block).await.is_ok() {
            let item: &IndexItem = from_bytes(&block);

            if hash < item.hash {
                if item.left == UNSET_VALUE {
                    break;
                } else {
                    pos = item.left;
                }
            } else {
                if hash == item.hash {
                    res.push((pos, item.clone()));
                }
                if item.right == UNSET_VALUE {
                    break;
                } else {
                    pos = item.right;
                }
            }
        }

        Ok(res)
    }
}


/// Calculate hash value of given hashable object. It is supposed to be
/// deterministic and look random with respect to comparison operations.
pub fn calc_hash<T: Hash>(obj: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() -> tokio::io::Result<()> {
        if tokio::fs::try_exists("./tmp/nums.index").await? {
            tokio::fs::remove_file("./tmp/nums.index").await?;
        }

        let mut index = Index::new("./tmp/nums.index").await?;

        index.push(0, calc_hash(&2u64)).await?;
        index.push(1, calc_hash(&5u64)).await?;
        index.push(2, calc_hash(&8u64)).await?;
        index.push(3, calc_hash(&3u64)).await?;
        index.push(4, calc_hash(&8u64)).await?;

        println!("{:?} - {:?}", 0, calc_hash(&2u64));
        println!("{:?} - {:?}", 1, calc_hash(&5u64));
        println!("{:?} - {:?}", 2, calc_hash(&8u64));
        println!("{:?} - {:?}", 3, calc_hash(&3u64));
        println!("{:?} - {:?}", 4, calc_hash(&8u64));

        assert_eq!(index.get_any(calc_hash(&1u64)).await?, None);
        assert_eq!(index.get_any(calc_hash(&2u64)).await?, Some(0));
        assert_eq!(index.get_any(calc_hash(&5u64)).await?, Some(1));
        assert_eq!(index.get_any(calc_hash(&3u64)).await?, Some(3));
        assert!(
            (index.get_any(calc_hash(&8u64)).await? == Some(2)) ||
            (index.get_any(calc_hash(&8u64)).await? == Some(4))
        );
        assert_eq!(index.get_any(calc_hash(&6u64)).await?, None);
        assert_eq!(index.get_any(calc_hash(&0u64)).await?, None);

        assert_eq!(index.get_all(calc_hash(&1u64)).await?, vec![]);
        assert_eq!(index.get_all(calc_hash(&5u64)).await?, vec![1]);
        assert_eq!(index.get_all(calc_hash(&8u64)).await?, vec![2, 4]);

        index.update(1, calc_hash(&5u64), calc_hash(&8u64)).await?;

        assert_eq!(index.get_any(calc_hash(&5u64)).await?, None);
        assert_eq!(index.get_all(calc_hash(&5u64)).await?, vec![]);
        assert_eq!(index.get_all(calc_hash(&8u64)).await?, vec![2, 4, 1]);

        index.push(5, calc_hash(&5u64)).await?;

        assert_eq!(index.get_any(calc_hash(&5u64)).await?, Some(5));
        assert_eq!(index.get_all(calc_hash(&5u64)).await?, vec![5]);

        index.push(6, calc_hash(&5u64)).await?;

        assert_eq!(index.get_all(calc_hash(&5u64)).await?, vec![5, 6]);

        Ok(())
    }
}
