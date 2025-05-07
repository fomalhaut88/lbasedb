//! Common functions of the library that mainly relate to byte converting.

use regex::Regex;
use std::io::{Error, ErrorKind};
use std::mem::size_of;
use std::slice::from_raw_parts;


/// Pattern for allowed names in feeds and columns.
pub const ALLOWED_NAME_PATTERN: &str = 
    r"^[a-zA-Z_][a-zA-Z_0-9]*(\.[a-zA-Z_][a-zA-Z_0-9]*)*$";


/// Represent `x` as its bytes (without copying).
pub fn to_bytes<T: Sized>(x: &T) -> &[u8] {
    let ptr = (x as *const T) as *const u8;
    let data = unsafe {
        from_raw_parts(ptr, size_of::<T>())
    };
    data
}


/// Represent bytes `block` as the data of `T` (without copying).
pub fn from_bytes<T: Sized>(block: &[u8]) -> &T {
    let ptr = (block as *const [u8]) as *const T;
    let data = unsafe {
        from_raw_parts(ptr, size_of::<T>())
    };
    data.first().unwrap()
}


/// Represent slice `x` as its bytes (without copying).
pub fn to_bytes_many<T: Sized>(x: &[T]) -> &[u8] {
    let ptr = (x as *const [T]) as *const u8;
    let size = x.len() * size_of::<T>();
    let data = unsafe {
        from_raw_parts(ptr, size)
    };
    data
}


/// Represent bytes `block` as the data slice of `T` (without copying).
pub fn from_bytes_many<T: Sized>(block: &[u8]) -> &[T] {
    let ptr = (block as *const [u8]) as *const T;
    let size = block.len() / size_of::<T>();
    let data = unsafe {
        from_raw_parts(ptr, size)
    };
    data
}


/// Represent string as bytes array with fixed size.
pub fn str_to_bytes<const N: usize>(s: &str) -> [u8; N] {
    let bytes = s.as_bytes();
    let size = std::cmp::min(bytes.len(), N);
    let mut buffer = [0u8; N];
    buffer[..size].clone_from_slice(&bytes[..size]);
    buffer
}


/// Represent bytes as string.
pub fn bytes_to_str(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap().trim_end_matches('\0')
}


/// Validates whether a given name is suitable for use as a feed or column name.
///
/// A valid name must consist of one or more non-empty segments separated by 
/// dots (`.`). Even a single-segment name must follow the rules below.
///
/// Each segment:
/// - May include uppercase and lowercase letters, digits, and underscores (`_`)
/// - Must **not** start with a digit
///
/// # Examples
///
/// Valid names:
/// - `qwe`
/// - `qwe1.rty2`
/// - `Qwe_123.Rty_456.Uio_789`
///
/// Invalid names:
/// - `.qwe`
/// - `rty.`
/// - `qwe..rty`
/// - `1qwe`
/// - `qwe.2rty`
/// - `qwe-rty`
pub fn validate_allowed_name(name: &str) -> std::io::Result<()> {
    if Regex::new(ALLOWED_NAME_PATTERN).unwrap().is_match(name) {
        Ok(())
    } else {
        Err(Error::new(ErrorKind::InvalidInput, name))
    }
}


/// Concatenate given paths.
#[macro_export]
macro_rules! path_concat {
    ($base:expr $(,$path:expr)*) => {{
        let mut res = std::path::PathBuf::from($base);
        $(
            res = res.join($path);
        )*
        res.display().to_string()
    }}
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_to_bytes() {
        assert_eq!(str_to_bytes("qwer"), [113, 119, 101, 114]);
        assert_eq!(str_to_bytes("qwerty"), [113, 119, 101, 114]);
        assert_eq!(str_to_bytes("qwe"), [113, 119, 101, 0]);
    }

    #[test]
    fn test_bytes_to_str() {
        assert_eq!(bytes_to_str(&[113, 119, 101, 114]), "qwer");
        assert_eq!(bytes_to_str(&[113, 119, 101, 0, 0]), "qwe");
    }

    #[test]
    fn test_path_concat() {
        assert_eq!(path_concat!("qwe", "asd"), "qwe/asd".to_string());
        assert_eq!(path_concat!("qwe/asd", "zxc"), "qwe/asd/zxc".to_string());
        assert_eq!(path_concat!("qwe/asd", "zxc/"), "qwe/asd/zxc/".to_string());
        assert_eq!(path_concat!("qwe/asd/", "zxc"), "qwe/asd/zxc".to_string());
        assert_eq!(path_concat!("/qwe/asd", "zxc"), "/qwe/asd/zxc".to_string());
    }

    #[test]
    fn test_validate_allowed_name() {
        assert!(validate_allowed_name("qwe").is_ok());
        assert!(validate_allowed_name("qwe1.rty2").is_ok());
        assert!(validate_allowed_name("Qwe_123.Rty_456.Uio_789").is_ok());
        assert!(validate_allowed_name(".qwe").is_err());
        assert!(validate_allowed_name("rty.").is_err());
        assert!(validate_allowed_name("qwe..rty").is_err());
        assert!(validate_allowed_name("1qwe").is_err());
        assert!(validate_allowed_name("qwe.2rty").is_err());
        assert!(validate_allowed_name("qwe-rty").is_err());
    }
}
