use std::fs::exists;
use std::collections::HashMap;

use tokio::io::Result as TokioResult;
use tokio::fs::{create_dir_all, create_dir, remove_dir_all, rename};

pub mod utils;
pub mod seq;
pub mod col;
pub mod list;
pub mod datatype;
pub mod dataset;
pub mod mgr;

pub use crate::utils::*;
pub use crate::seq::*;
pub use crate::col::*;
pub use crate::list::*;
pub use crate::datatype::*;
pub use crate::dataset::*;
pub use crate::mgr::*;


use std::path::Path;


pub struct Connection {
    pub feed_mgr: FeedMgr,
}


impl Connection {
    pub async fn new(path: &str) -> TokioResult<Self> {
        let mut feed_mgr = FeedMgr::new(path).await?;
        Ok(Self { feed_mgr })
    }

    pub fn path(&self) -> &str {
        self.feed_mgr.path()
    }

    pub async fn push(&mut self, feed: &str) -> TokioResult<usize> {
        // self.feed_mgr.col_mgr(feed);
        Ok(0)
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
        // Seq
        let mut seq = Seq::new("./tmp/s1", 4).await?;
        seq.resize(8).await?;
        seq.push(b"qwer").await?;
        println!("{:?}", seq.size().await?);
        let mut block = [0u8; 4];
        seq.get(5, &mut block).await?;
        println!("{:?}", std::str::from_utf8(&block).unwrap());
        seq.update(6, b"aaaa").await?;
        seq.get(6, &mut block).await?;
        println!("{:?}", std::str::from_utf8(&block).unwrap());

        // Col
        let mut col = Col::<i32>::new("./tmp/c1").await?;
        col.resize(6).await?;
        col.push(&25).await?;
        println!("{:?}", col.size().await?);
        println!("{:?}", col.get(3).await?);
        col.update(3, &12).await?;
        println!("{:?}", col.get(3).await?);
        println!("{:?}", col.get_many(2, 4).await?);

        // List
        let mut lst = List::<Item1, String>::new("./tmp/l1").await?;
        println!("{:?}", lst.list().await?);

        lst.add(&Item1 { key: *b"qweasdrf", val: 25 }).await.ok();
        println!("{:?}", lst.list().await?);

        lst.modify(&"qweasdrf".to_string(), &Item1 { key: *b"12345678", val: 28 }).await?;
        println!("{:?}", lst.list().await?);

        lst.remove(&"12345678".to_string()).await?;
        println!("{:?}", lst.list().await?);

        lst.add(&Item1 { key: *b"12345678", val: 28 }).await?;
        println!("{:?}", lst.list().await?);

        lst.modify(&"12345678".to_string(), &Item1 { key: *b"qweasdrf", val: 25 }).await?;
        println!("{:?}", lst.list().await?);

        println!("{:?}", lst.detail(&"qweasdrf".to_string()).await?.val);
        println!("{:?}", lst.detail(&"12345678".to_string()).await.err().unwrap().kind());

        // myfunc(&[("price", &25.0), ("block_num", &6152)]);

        // Lbase
        // let mut conn = Lbase::new("./tmp/lbase1").await?;

        // conn.feed_add("usdt").await.ok();

        // let feeds: Vec<String> = conn.feed_list().await?;
        // println!("feeds = {:?}", feeds);

        // conn.col_add("usdt", ("price", Datatype::Float, 0.0)).await.ok();
        // conn.col_add("usdt", ("block_num", Datatype::Int, 0)).await.ok();

        // let cols: Vec<(String, Datatype, &any Any)> = conn.col_list().await?;
        // println!("cols = {:?}", cols);

        // let ix = conn.push("usdt", &[
        //     ("price", vec![25.2, 25.1]),
        //     ("block_num", vec![6153, 6154]),
        // ]).await?;

        // conn.patch("usdt",
        //     5, 6,
        //     &[
        //         ("price", vec![25.5]),
        //     ]
        // ).await?;

        // let data: Dataset = conn.get("usdt", &["price"], 5, 8).await?;
        // let data: Dataset = conn.get_all("usdt", &["price", "block_num"]).await?;

        // conn.truncate("usdt", 10).await?;

        // println!("{:?}", std::mem::size_of::<FeedRecord>());
        // println!("{:?}", std::mem::size_of::<ColRecord>());

        // println!("{:?}", from_bytes::<f32>(&[0, 0, 0, 0]));

        let mut conn = Connection::new("./tmp/lbase1").await?;

        if !conn.feed_mgr.exists("tab1") {
            conn.feed_mgr.add("tab1").await?;
        }

        println!("{:?}", conn.feed_mgr.list().await?);

        conn.feed_mgr.rename("tab1", "tab2").await?;

        println!("{:?}", conn.feed_mgr.list().await?);

        println!("col_mgr tab2: {:?}", conn.feed_mgr.col_mgr("tab2"));

        if conn.feed_mgr.exists("tab2") {
            conn.feed_mgr.remove("tab2").await?;
        }

        println!("{:?}", conn.feed_mgr.list().await?);

        // Return
        Ok(())
    }
}
