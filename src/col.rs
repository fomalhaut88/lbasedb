//! `Col` is a wrapper over `Seq` for an arbitrary sized datatype so it can be 
//! represented as its bytes and stored in a file using the `Seq` interface.

use std::mem::size_of;
use std::path::Path;
use std::marker::PhantomData;

use tokio::io::Result as TokioResult;

use crate::utils::{to_bytes, from_bytes, to_bytes_many, from_bytes_many};
use crate::seq::Seq;


/// `Col` implements a storage for the data of type `T`. It supports
/// `push`, `get`, `update` asynchronous methods and their multiple extensions.
pub struct Col<T> {
    seq: Seq,
    phantom: PhantomData<T>,
}


impl<T: Clone> Col<T> {
    /// Create a `Col` instance located at `path`.
    pub async fn new(path: impl AsRef<Path>) -> TokioResult<Self> {
        let block_size = Self::block_size();
        let seq = Seq::new(path, block_size).await?;
        Ok(Self { seq, phantom: PhantomData })
    }

    /// Get size of the data instance in bytes.
    pub fn block_size() -> usize {
        size_of::<T>()
    }

    /// Get size of the file in the number of units sized with `block_size`.
    pub async fn size(&self) -> TokioResult<usize> {
        self.seq.size().await
    }

    /// Resize the file setting a new size `new_size` in the number of units 
    /// sized with `block_size`.
    pub async fn resize(&self, new_size: usize) -> TokioResult<()> {
        self.seq.resize(new_size).await
    }

    /// Push the data `x` to the end.
    pub async fn push(&mut self, x: &T) -> TokioResult<usize> {
        let block = to_bytes(x);
        let ix = self.seq.push(block).await?;
        Ok(ix)
    }

    /// Push multiple data `x` to the end.
    pub async fn push_many(&mut self, x: &[T]) -> TokioResult<usize> {
        let block = to_bytes_many(x);
        let ix = self.seq.push(block).await?;
        Ok(ix)
    }

    /// Get the instance located at `ix`.
    pub async fn get(&mut self, ix: usize) -> TokioResult<T> {
        let mut block = vec![0u8; Self::block_size()];
        self.seq.get(ix, &mut block).await?;
        let x: &T = from_bytes(&block);
        Ok(x.clone())
    }

    /// Get `count` instances located from `ix`.
    pub async fn get_many(&mut self, ix: usize, count: usize) -> 
            TokioResult<Vec<T>> {
        if count > 0 {
            let mut block = vec![0u8; Self::block_size() * count];
            self.seq.get(ix, &mut block).await?;
            let x: &[T] = from_bytes_many(&block);
            Ok(x.to_vec())
        } else {
            Ok(vec![])
        }
    }

    /// Get all instances.
    pub async fn get_all(&mut self) -> TokioResult<Vec<T>> {
        let size = self.seq.size().await?;
        if size > 0 {
            let mut block = vec![0u8; Self::block_size() * size];
            self.seq.get(0, &mut block).await?;
            let x: &[T] = from_bytes_many(&block);
            Ok(x.to_vec())
        } else {
            Ok(vec![])
        }
    }

    /// Update the instance located at `ix` with the data `x`.
    pub async fn update(&mut self, ix: usize, x: &T) -> TokioResult<()> {
        let block = to_bytes(x);
        self.seq.update(ix, &block).await?;
        Ok(())
    }

    /// Update the instances located from `ix` with the data in the slice `x`.
    pub async fn update_many(&mut self, ix: usize, x: &[T]) -> TokioResult<()> {
        let block = to_bytes_many(x);
        self.seq.update(ix, &block).await?;
        Ok(())
    }
}
