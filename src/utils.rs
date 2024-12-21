use std::mem::size_of;
use std::slice::from_raw_parts;


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
}
