//! `FeedItem` and `ColItem` are the basic entities of the DBMS that are 
//! responsible for the options of feeds (like tables or collections) and
//! cols (like columns of fields).

use crate::utils::{str_to_bytes, bytes_to_str, validate_allowed_name};
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
    pub fn new(name: &str) -> std::io::Result<Self> {
        validate_allowed_name(name)?;
        Ok(Self {
            name: str_to_bytes::<MAX_NAME_SIZE>(name),
            size: 0,
        })
    }

    /// Get name as string.
    pub fn get_name(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }

    /// Rename the feed.
    pub fn rename(&mut self, name: &str) -> std::io::Result<()> {
        validate_allowed_name(name)?;
        self.name = str_to_bytes::<MAX_NAME_SIZE>(name);
        Ok(())
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
    pub fn new(name: &str, datatype: &str) -> std::io::Result<Self> {
        validate_allowed_name(name)?;
        Ok(Self {
            name: str_to_bytes::<MAX_NAME_SIZE>(name),
            datatype: datatype.parse().unwrap(),
        })
    }

    /// Get name as string.
    pub fn get_name(&self) -> String {
        bytes_to_str(&self.name).to_string()
    }

    /// Get datatype as string.
    pub fn get_datatype(&self) -> String {
        self.datatype.to_string()
    }

    /// Rename the column.
    pub fn rename(&mut self, name: &str) -> std::io::Result<()> {
        validate_allowed_name(name)?;
        self.name = str_to_bytes::<MAX_NAME_SIZE>(name);
        Ok(())
    }
}
