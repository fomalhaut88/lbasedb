use crate::utils::{str_to_bytes, bytes_to_str};
use crate::datatype::Datatype;
use crate::list::ListKeyTrait;


/// Maximum size for the stored names
const MAX_NAME_SIZE: usize = 256;

/// Type for the names as a static byte array.
type NameType = [u8; MAX_NAME_SIZE];


/// Feed structure.
#[derive(Clone, Debug)]
pub struct FeedItem {
    /// Name of the feed.
    pub name: NameType,

    /// Size of the feed.
    pub size: usize,
}


impl ListKeyTrait<String> for FeedItem {
    fn key(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }
}


impl FeedItem {
    /// Create a feed object by name given as string.
    pub fn new(name: &str) -> Self {
        Self {
            name: str_to_bytes::<MAX_NAME_SIZE>(name),
            size: 0,
        }
    }

    /// Rename the feed.
    pub fn rename(&mut self, name: &str) {
        self.name = str_to_bytes::<MAX_NAME_SIZE>(name);
    }
}


/// Column structure.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ColItem {
    /// Name of the column,
    pub name: NameType,

    /// Datatype of the column.
    pub datatype: Datatype,
}


impl ListKeyTrait<String> for ColItem {
    fn key(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }
}


impl ColItem {
    /// Create a column object by the name as string and the datatype.
    pub fn new(name: &str, datatype: &str) -> Self {
        Self {
            name: str_to_bytes::<MAX_NAME_SIZE>(name),
            datatype: datatype.parse().unwrap(),
        }
    }

    /// Rename the column.
    pub fn rename(&mut self, name: &str) {
        self.name = str_to_bytes::<MAX_NAME_SIZE>(name);
    }
}
