//! Asynchronous file-based heap allocator.
//!
//! This module provides a simple heap manager that allocates, resizes, 
//! writes, and reads variable-sized memory blocks within a file. 
//! Each allocated block is aligned to the next power of two to minimize 
//! fragmentation.
//!
//! The heap supports asynchronous operations using Tokio and is suitable 
//! for building persistent storage systems, such as custom databases or 
//! file-based caches.
//!
//! Key features:
//! - Asynchronous allocation and resizing
//! - File-backed storage with offset tracking
//! - Simple block management without free-space reuse
//!
//! ```ignore
//! let mut text_col = Col::<HeapItem>::new("./tmp/text.col").await?;
//! let mut text_heap = Heap::new("./tmp/text.heap").await?;
//!
//! let phrase = "This is Lbasedb library.";
//!
//! // Save text
//! let block: &[u8] = phrase.as_bytes();
//! let mut item = text_heap.alloc(block.len() as u64).await?;
//! text_heap.update(&item, block).await?;
//! let ix = text_col.push(&item).await?;
//!
//! assert_eq!(text_col.size().await?, 1);
//! assert_eq!(text_heap.size().await?, 32);
//!
//! // Update text
//! let phrase = "This is Lbasedb library. Updated text.";
//! let block: &[u8] = phrase.as_bytes();
//! text_heap.realloc(&mut item, phrase.len() as u64).await?;
//! text_heap.update(&item, block).await?;
//! text_col.update(ix, &item).await?;
//!
//! assert_eq!(text_col.size().await?, 1);
//! assert_eq!(text_heap.size().await?, 96);
//!
//! // Get text
//! let item = text_col.get(0).await?;
//! let mut block = vec![0u8; item.size as usize];
//! text_heap.get(&item, &mut block).await?;
//! let phrase: &str = str::from_utf8(&block).unwrap();
//!
//! assert_eq!(item.offset, 32);
//! assert_eq!(item.maxsize, 64);
//! assert_eq!(phrase, "This is Lbasedb library. Updated text.");
//! ```

use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::{ErrorKind, Result as TokioResult};
use tokio::io::{SeekFrom, AsyncSeekExt, AsyncWriteExt, AsyncReadExt};


/// Describes a memory block in the file-backed heap.
///
/// Contains the block's file offset, current size, and maximum allocated size.
#[derive(Debug, Clone)]
pub struct HeapItem {
    offset: u64,
    size: u64,
    maxsize: u64,
}


/// Asynchronous file-backed heap allocator.
///
/// Manages variable-sized memory blocks within a file.
pub struct Heap {
    file: File,
}


impl Heap {
    /// Opens or creates a heap file at the given path.
    pub async fn new(path: impl AsRef<Path>) -> TokioResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)
            .await?;
        Ok(Self { file })
    }

    /// Returns the current size of the heap file in bytes.
    pub async fn size(&self) -> TokioResult<usize> {
        let data = self.file.metadata().await?;
        Ok(data.len() as usize)
    }

    /// Allocates a new memory block of the given size.
    pub async fn alloc(&self, size: u64) -> TokioResult<HeapItem> {
        let offset = self.size().await? as u64;
        let maxsize = size.next_power_of_two();
        self.file.set_len(offset + maxsize).await?;
        Ok(HeapItem { offset, size, maxsize })
    }

    /// Resizes the memory block. Reallocates if the new size exceeds the 
    /// block's capacity. It returns `true` if reallocation happened else
    /// `false`.
    pub async fn realloc(&self, item: &mut HeapItem, 
                         size: u64) -> TokioResult<bool> {
        if size > item.maxsize {
            *item = self.alloc(size).await?;
            Ok(true)
        } else {
            item.size = size;
            Ok(false)
        }
    }

    /// Writes data to the specified memory block.
    pub async fn update(&mut self, item: &HeapItem, 
                        block: &[u8]) -> TokioResult<()> {
        if block.len() as u64 > item.maxsize {
            Err(ErrorKind::UnexpectedEof.into())
        } else {
            let pos = SeekFrom::Start(item.offset);
            self.file.seek(pos).await?;
            self.file.write_all(block).await?;
            self.file.flush().await?;
            Ok(())
        }
    }

    /// Reads data from the specified memory block.
    pub async fn get(&mut self, item: &HeapItem, 
                     block: &mut [u8]) -> TokioResult<()> {
        let pos = SeekFrom::Start(item.offset);
        self.file.seek(pos).await?;
        self.file.read_exact(&mut block[.. item.size as usize]).await?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::col::Col;

    #[tokio::test]
    async fn test() -> tokio::io::Result<()> {
        if tokio::fs::try_exists("./tmp/text.col").await? {
            tokio::fs::remove_file("./tmp/text.col").await?;
        }

        if tokio::fs::try_exists("./tmp/text.heap").await? {
            tokio::fs::remove_file("./tmp/text.heap").await?;
        }

        let mut text_col = Col::<HeapItem>::new("./tmp/text.col").await?;
        let mut text_heap = Heap::new("./tmp/text.heap").await?;

        let phrase = "This is Lbasedb library.";

        // Save text
        let block: &[u8] = phrase.as_bytes();
        let mut item = text_heap.alloc(block.len() as u64).await?;
        text_heap.update(&item, block).await?;
        let ix = text_col.push(&item).await?;

        assert_eq!(text_col.size().await?, 1);
        assert_eq!(text_heap.size().await?, 32);

        // Update text
        let phrase = "This is Lbasedb library. Updated text.";
        let block: &[u8] = phrase.as_bytes();
        text_heap.realloc(&mut item, phrase.len() as u64).await?;
        text_heap.update(&item, block).await?;
        text_col.update(ix, &item).await?;

        assert_eq!(text_col.size().await?, 1);
        assert_eq!(text_heap.size().await?, 96);

        // Get text
        let item = text_col.get(0).await?;
        let mut block = vec![0u8; item.size as usize];
        text_heap.get(&item, &mut block).await?;
        let phrase: &str = str::from_utf8(&block).unwrap();

        assert_eq!(item.offset, 32);
        assert_eq!(item.maxsize, 64);
        assert_eq!(phrase, "This is Lbasedb library. Updated text.");

        Ok(())
    }
}
