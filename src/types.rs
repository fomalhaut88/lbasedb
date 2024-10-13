use std::any::Any;

use crate::utils::{to_bytes, from_bytes};


pub enum Types {
    Int,
    Float,
    // Bool,
    // String(usize),
    // Bytes(usize),
    // Datetime,
}


impl Types {
    pub fn to_bytes<'a>(&self, x: &'a dyn Any) -> Option<&'a [u8]> {
        match self {
            Self::Int => {
                x.downcast_ref::<i64>().map(|v| to_bytes(v))
            },
            Self::Float => {
                x.downcast_ref::<f64>().map(|v| to_bytes(v))
            },
        }
    }

    pub fn from_bytes<'a>(&self, block: &'a [u8]) -> &'a dyn Any {
        match self {
            Self::Int => {
                from_bytes::<i64>(block)
            },
            Self::Float => {
                from_bytes::<f64>(block)
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes() {
        assert_eq!(
            Types::Int.to_bytes(&65i64), 
            Some([65, 0, 0, 0, 0, 0, 0, 0].as_ref())
        );
        assert_eq!(
            Types::Float.to_bytes(&2.718281828f64), 
            Some([155, 145, 4, 139, 10, 191, 5, 64].as_ref())
        );
    }

    #[test]
    fn test_from_bytes() {
        assert_eq!(
            Types::Int.from_bytes(&[65, 0, 0, 0, 0, 0, 0, 0])
                .downcast_ref::<i64>(), 
            Some(&65)
        );
        assert_eq!(
            Types::Float.from_bytes(&[155, 145, 4, 139, 10, 191, 5, 64])
                .downcast_ref::<f64>(), 
            Some(&2.718281828)
        );
    }
}
