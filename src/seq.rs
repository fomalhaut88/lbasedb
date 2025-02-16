use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::Result as TokioResult;
use tokio::io::{SeekFrom, AsyncSeekExt, AsyncWriteExt, AsyncReadExt};

// TODO: Maybe it is necessary to implement throught tokio_uring 
// (https://docs.rs/tokio-uring/latest/tokio_uring/) that supports a faster 
// Linux interface. It provides `read_exact_at`, `write_all_at` and so on.


/// `Seq` is a basic unit to work with the file system. It implements
/// low level asynchronous functions to read and write block of data
/// represented as bytes. The stored content is managed as a sequence of
/// blocks with the same size (`block_size`). Each block can be accessed by
/// its index.
pub struct Seq {
    file: File,
    block_size: usize,
}


impl Seq {
    /// Create a `Seq` object located by the given `path` and having the given
    /// `block_size`. If no file exists, it creates an empty one.
    pub async fn new(path: impl AsRef<Path>, block_size: usize) -> 
            TokioResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)
            .await?;
        Ok(Self { file, block_size })
    }

    /// Get block size in bytes.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Get size of the file in the number of units sized with `block_size`.
    pub async fn size(&self) -> TokioResult<usize> {
        let data = self.file.metadata().await?;
        Ok(data.len() as usize / self.block_size)
    }

    /// Resize the file setting a new size `new_size` in the number of units 
    /// sized with `block_size`.
    pub async fn resize(&self, new_size: usize) -> TokioResult<()> {
        let byte_size = (new_size * self.block_size) as u64;
        self.file.set_len(byte_size).await?;
        Ok(())
    }

    /// Push a new data block to the end of the file. The size of `block`
    /// in bytes must be multiple of `block_size`, otherwise there can be 
    /// unpredictable behavior.
    pub async fn push(&mut self, block: &[u8]) -> TokioResult<usize> {
        let pos = SeekFrom::End(0);
        let offset = self.file.seek(pos).await?;
        self.file.write_all(block).await?;
        self.file.flush().await?;
        let ix = offset as usize / self.block_size;
        Ok(ix)
    }

    /// Get data located by the index `ix` and write it to the `block`.
    /// The size of `block` in bytes must be multiple of `block_size`, 
    /// otherwise there can be unpredictable behavior.
    pub async fn get(&mut self, ix: usize, block: &mut [u8]) -> 
            TokioResult<()> {
        let byte_ix = (ix * self.block_size) as u64;
        let pos = SeekFrom::Start(byte_ix);
        self.file.seek(pos).await?;
        self.file.read_exact(block).await?;
        Ok(())
    }

    /// Update data located by the index `ix` with the bytes in `block`.
    /// The size of `block` in bytes must be multiple of `block_size`, 
    /// otherwise there can be unpredictable behavior.
    pub async fn update(&mut self, ix: usize, block: &[u8]) -> TokioResult<()> {
        let byte_ix = (ix * self.block_size) as u64;
        let pos = SeekFrom::Start(byte_ix);
        self.file.seek(pos).await?;
        self.file.write_all(block).await?;
        self.file.flush().await?;
        Ok(())
    }

    /// Allocate next `len` blocks with zeros.
    pub async fn push_empty(&mut self, len: usize) -> TokioResult<usize> {
        let block = vec![0u8; len * self.block_size];
        let ix = self.push(&block).await?;
        Ok(ix)
    }
}
