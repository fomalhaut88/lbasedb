use std::any::Any;
use std::mem::size_of;

use crate::utils::{to_bytes, from_bytes};


#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Datatype {
    Int64,  // i64
    Float64,  // f64
    Int32,  // i32
    Float32,  // f32
    Bytes(usize),  // Vec<u8>
}


impl Datatype {
    /// Represent `x` as its byte slice without clonning, In case of mismatch
    /// `None` will be returned.
    pub fn to_bytes<'a>(&self, x: &'a dyn Any) -> Option<&'a [u8]> {
        match self {
            Self::Int64 => {
                x.downcast_ref::<i64>().map(|v| to_bytes(v))
            },
            Self::Float64 => {
                x.downcast_ref::<f64>().map(|v| to_bytes(v))
            },
            Self::Int32 => {
                x.downcast_ref::<i32>().map(|v| to_bytes(v))
            },
            Self::Float32 => {
                x.downcast_ref::<f32>().map(|v| to_bytes(v))
            },
            Self::Bytes(len) => {
                x.downcast_ref::<Vec<u8>>()
                    .filter(|v| v.len() == *len)
                    .map(Vec::as_ref)
            },
        }
    }

    /// Converts a byte slice into a boxed data with copying.
    pub fn from_bytes<'a>(&self, block: &'a [u8]) -> Box<dyn Any> {
        match self {
            Self::Int64 => {
                Box::new(*from_bytes::<i64>(block))
            },
            Self::Float64 => {
                Box::new(*from_bytes::<f64>(block))
            },
            Self::Int32 => {
                Box::new(*from_bytes::<i32>(block))
            },
            Self::Float32 => {
                Box::new(*from_bytes::<f32>(block))
            },
            Self::Bytes(len) => {
                let mut v = block.to_vec();
                v.resize(*len, 0u8);
                Box::new(v)
            },
        }
    }

    /// Size in bytes.
    pub fn size(&self) -> usize {
        match self {
            Self::Int64 => size_of::<i64>(),
            Self::Float64 => size_of::<f64>(),
            Self::Int32 => size_of::<i32>(),
            Self::Float32 => size_of::<f32>(),
            Self::Bytes(len) => *len,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes() {
        // Common
        assert_eq!(
            Datatype::Int64.to_bytes(&65i64), 
            Some([65, 0, 0, 0, 0, 0, 0, 0].as_ref())
        );
        assert_eq!(
            Datatype::Int32.to_bytes(&65i32), 
            Some([65, 0, 0, 0].as_ref())
        );
        assert_eq!(
            Datatype::Float64.to_bytes(&2.718281828f64), 
            Some([155, 145, 4, 139, 10, 191, 5, 64].as_ref())
        );
        assert_eq!(
            Datatype::Float32.to_bytes(&2.7182818f32), 
            Some([84, 248, 45, 64].as_ref())
        );

        // Type mismatch
        assert_eq!(
            Datatype::Int64.to_bytes(&65i32), 
            None
        );
    }

    #[test]
    fn test_from_bytes() {
        // Common
        assert_eq!(
            Datatype::Int64.from_bytes(&[65, 0, 0, 0, 0, 0, 0, 0])
                .downcast_ref::<i64>(), 
            Some(&65)
        );
        assert_eq!(
            Datatype::Int32.from_bytes(&[65, 0, 0, 0])
                .downcast_ref::<i32>(), 
            Some(&65)
        );
        assert_eq!(
            Datatype::Float64.from_bytes(&[155, 145, 4, 139, 10, 191, 5, 64])
                .downcast_ref::<f64>(), 
            Some(&2.718281828)
        );
        assert_eq!(
            Datatype::Float32.from_bytes(&[84, 248, 45, 64])
                .downcast_ref::<f32>(), 
            Some(&2.7182818)
        );

        // Type mismatch
        assert_eq!(
            Datatype::Int64.from_bytes(&[65, 0, 0, 0, 0, 0, 0, 0])
                .downcast_ref::<i32>(), 
            None
        );
    }

    #[test]
    fn test_bytes() {
        // Initial vector of bytes
        let v: Vec<u8> = vec![155, 145, 4, 139];

        // Wrong size
        assert_eq!(Datatype::Bytes(3).to_bytes(&v), None);
        assert_eq!(Datatype::Bytes(5).to_bytes(&v), None);

        // Correct size, converting
        let block: &[u8] = Datatype::Bytes(4).to_bytes(&v).unwrap();
        assert_eq!(block, [155, 145, 4, 139]);

        // Unpacking from block of bytes
        assert_eq!(
            Datatype::Bytes(4).from_bytes(block).downcast_ref::<Vec<u8>>(), 
            Some(&vec![155, 145, 4, 139])
        );
        assert_eq!(
            Datatype::Bytes(2).from_bytes(block).downcast_ref::<Vec<u8>>(), 
            Some(&vec![155, 145])
        );
        assert_eq!(
            Datatype::Bytes(6).from_bytes(block).downcast_ref::<Vec<u8>>(), 
            Some(&vec![155, 145, 4, 139, 0, 0])
        );
    }

    #[test]
    fn test_size() {
        assert_eq!(Datatype::Int64.size(), 8);
        assert_eq!(Datatype::Int32.size(), 4);
        assert_eq!(Datatype::Float64.size(), 8);
        assert_eq!(Datatype::Float32.size(), 4);
        assert_eq!(Datatype::Bytes(5).size(), 5);
    }
}
