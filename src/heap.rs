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
    /// block's capacity.
    pub async fn realloc(&self, item: &mut HeapItem, 
                         size: u64) -> TokioResult<()> {
        if size > item.maxsize {
            *item = self.alloc(size).await?;
        } else {
            item.size = size;
        }
        Ok(())
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
