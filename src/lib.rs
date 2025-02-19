#![warn(missing_docs)]
#![feature(test)]

pub mod utils;
pub mod seq;
pub mod col;
pub mod list;
pub mod items;
pub mod datatype;
pub mod dataset;
pub mod conn;

pub use crate::utils::*;
pub use crate::seq::*;
pub use crate::col::*;
pub use crate::list::*;
pub use crate::items::*;
pub use crate::datatype::*;
pub use crate::dataset::*;
pub use crate::conn::*;


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() -> tokio::io::Result<()> {
        let mut conn = Conn::new("./tmp/lb3").await?;

        if !conn.feed_exists("xyz") {
            conn.feed_add("xyz").await?;
        }

        println!("Feed list: {:?}", conn.feed_list());

        if !conn.col_exists("xyz", "x") {
            conn.col_add("xyz", "x", "Int64").await?;
        }

        if !conn.col_exists("xyz", "y") {
            conn.col_add("xyz", "y", "Float64").await?;
        }

        println!(
            "Col list: {:?}", 
            conn.col_list("xyz")?.iter()
                .map(|i| bytes_to_str(&i.name).to_string())
                .collect::<Vec<String>>()
        );

        if conn.size_get("xyz")? == 0 {
            let ds: Dataset = std::collections::HashMap::from([
                ("x".to_string(), vec![Dataunit::I(2), Dataunit::I(5)]),
                ("y".to_string(), vec![Dataunit::F(2.15), Dataunit::F(5.55)]),
            ]);
            conn.data_push("xyz", &ds).await?;
        }

        println!("Size: {:?}", conn.size_get("xyz")?);

        let ds = conn.data_get("xyz", 0, 2, 
                               &["x".to_string(), "y".to_string()]).await?;
        println!("ds = {:?}", ds);

        Ok(())
    }
}
