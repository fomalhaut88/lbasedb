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
