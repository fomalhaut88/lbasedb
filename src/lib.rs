pub mod utils;
pub mod seq;
pub mod col;
pub mod list;
pub mod types;

pub use crate::utils::*;
pub use crate::seq::*;
pub use crate::col::*;
pub use crate::list::*;
pub use crate::types::*;


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
            String::from_utf8(self.key.to_vec()).unwrap()
        }
    }

    #[tokio::test]
    async fn test() -> tokio::io::Result<()> {
        // Seq
        let mut seq = Seq::new("./tmp/s1", 4).await?;
        seq.truncate(8).await?;
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
        col.truncate(6).await?;
        col.push(&25).await?;
        println!("{:?}", col.size().await?);
        println!("{:?}", col.get(3).await?);
        col.update(3, &12).await?;
        println!("{:?}", col.get(3).await?);
        println!("{:?}", col.get_many(2, 4).await?);

        // List
        let mut lst = List::<Item1, String>::new("./tmp/l1").await?;
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

        // Return
        Ok(())
    }
}
