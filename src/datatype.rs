use std::any::Any;
use std::mem::size_of;
use std::str::FromStr;

use base64::prelude::*;

use crate::utils::{to_bytes, from_bytes};


#[derive(Debug, Clone, PartialEq)]
pub enum Dataunit {
    I(i64),
    F(f64),
    S(String),
}


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

    /// Represent `x` as its byte slice without clonning, In case of mismatch
    /// `None` will be returned.
    pub fn to_bytes2(&self, x: &Dataunit) -> Option<Vec<u8>> {
        match self {
            Self::Int64 => {
                if let Dataunit::I(x) = x {
                    Some(to_bytes(x).to_vec())
                } else {
                    None
                }
            },
            Self::Int32 => {
                if let Dataunit::I(x) = x {
                    Some(to_bytes(&(*x as i32)).to_vec())
                } else {
                    None
                }
            },
            Self::Float64 => {
                if let Dataunit::F(x) = x {
                    Some(to_bytes(x).to_vec())
                } else {
                    None
                }
            },
            Self::Float32 => {
                if let Dataunit::F(x) = x {
                    Some(to_bytes(&(*x as f32)).to_vec())
                } else {
                    None
                }
            },
            Self::Bytes(len) => {
                if let Dataunit::S(x) = x {
                    let mut block = BASE64_STANDARD.decode(x).unwrap();
                    block.resize(*len, 0);
                    Some(block)
                } else {
                    None
                }
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

    /// Converts a byte slice into a boxed data with copying.
    pub fn from_bytes2(&self, block: &[u8]) -> Dataunit {
        match self {
            Self::Int64 => {
                Dataunit::I(*from_bytes::<i64>(block))
            },
            Self::Int32 => {
                Dataunit::I((*from_bytes::<i32>(block)).into())
            },
            Self::Float64 => {
                Dataunit::F(*from_bytes::<f64>(block))
            },
            Self::Float32 => {
                Dataunit::F((*from_bytes::<f32>(block)).into())
            },
            Self::Bytes(len) => {
                let string = BASE64_STANDARD.encode(&block[..*len]);
                Dataunit::S(string)
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


impl ToString for Datatype {
    fn to_string(&self) -> String {
        match self {
            Self::Int64 => "Int64".to_string(),
            Self::Float64 => "Float64".to_string(),
            Self::Int32 => "Int32".to_string(),
            Self::Float32 => "Float32".to_string(),
            Self::Bytes(len) => format!("Bytes[{}]", len),
        }
    }
}


impl FromStr for Datatype {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Int64" => Ok(Self::Int64),
            "Float64" => Ok(Self::Float64),
            "Int32" => Ok(Self::Int32),
            "Float32" => Ok(Self::Float32),
            _ => {
                let len_str = s
                    .strip_prefix("Bytes[")
                    .and_then(|s| s.strip_suffix(']'))
                    .ok_or("Unknown datatype".to_string())?;

                let len = len_str.parse::<usize>()
                    .map_err(|_| "Unknown datatype".to_string())?;

                Ok(Self::Bytes(len))
            },
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

    #[test]
    fn test_convert_string() {
        assert_eq!(Datatype::Int32.to_string(), "Int32");
        assert_eq!(Datatype::Bytes(25).to_string(), "Bytes[25]");

        assert_eq!("Int32".parse::<Datatype>(), Ok(Datatype::Int32));
        assert_eq!("Bytes[25]".parse::<Datatype>(), Ok(Datatype::Bytes(25)));

        assert_eq!("Boolean".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
        assert_eq!("Bytes[xxx]".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
        assert_eq!("Bytes[-12]".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
    }

    #[test]
    fn test_dataunit_convert() {
        assert_eq!(
            Datatype::Int64.to_bytes2(&Dataunit::I(25)).unwrap(), 
            vec![25, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            Datatype::Int32.to_bytes2(&Dataunit::I(25)).unwrap(), 
            vec![25, 0, 0, 0]
        );
        assert_eq!(
            Datatype::Float64.to_bytes2(&Dataunit::F(3.14)).unwrap(), 
            vec![31, 133, 235, 81, 184, 30, 9, 64]
        );
        assert_eq!(
            Datatype::Float32.to_bytes2(&Dataunit::F(3.14)).unwrap(), 
            vec![195, 245, 72, 64]
        );
        assert_eq!(
            Datatype::Bytes(5).to_bytes2(
                &Dataunit::S("+uwgVQA=".to_string())
            ).unwrap(), 
            vec![250, 236, 32, 85, 0]
        );

        assert_eq!(
            Datatype::Int64.from_bytes2(&[25, 0, 0, 0, 0, 0, 0, 0]), 
            Dataunit::I(25)
        );
        assert_eq!(
            Datatype::Int32.from_bytes2(&[25, 0, 0, 0]), 
            Dataunit::I(25)
        );
        assert_eq!(
            Datatype::Float64.from_bytes2(&[31, 133, 235, 81, 184, 30, 9, 64]), 
            Dataunit::F(3.14)
        );
        assert_eq!(
            Datatype::Float32.from_bytes2(&[195, 245, 72, 64]), 
            Dataunit::F(3.140000104904175)
        );
        assert_eq!(
            Datatype::Bytes(5).from_bytes2(&[250, 236, 32, 85, 0]), 
            Dataunit::S("+uwgVQA=".to_string())
        );
    }
}
