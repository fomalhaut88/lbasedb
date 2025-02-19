# lbasedb

`lbasedb` is a powerful low level DBMS that is focused on dataset structure.
The algorithms are optimized for the compact storing the data and for high
performance on get and append operations. Particularly, due to this, 
deleting or indexing are not supported. The allowed data types are also
limited (integers, floats and bytes) for making easy integration with
C-like or similar common interfaces (like Python, CUDA, JSON and so on).
The database has asynchronous access to the entities powered by `tokio`.
It is supposed to be used for the data that have billions and more records
and thousands columns of simple data types that can be appended without
extra overhead.

## Installation

```
cargo add lbasedb
```

## Usage example:

```rust
use lbasedb::prelude::*;
use lbasedb::utils::bytes_to_str;

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
```
