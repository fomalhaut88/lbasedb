//! Converting between datatypes for different purposes: into bytes and back,
//! serializations, from and into strings and so on.

use std::mem::size_of;
use std::str::FromStr;

use base64::prelude::*;
use serde::{Serialize, Deserialize};

use crate::utils::{to_bytes, from_bytes};


/// A dataunit for convenient integration. It supports integers, floats and
/// strings that should represent fixed size bytes encrypted with Base64.
/// It is compatible with `serde` serialization so it may be used in
/// API interfaces like, for example, `actix_web` provides.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Dataunit {
    /// Integer
    I(i64),

    /// Float
    F(f64),

    /// String
    S(String),
}


/// Allowed datatypes for the stored data. It manages the converting between
/// basic datatypes and bytes in the file. Integers and floats cast and convert
/// normally, bytes convert to strings and back according the Base64 algorithm.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Datatype {
    /// 64-bit integer.
    Int64,

    /// 64-bit float.
    Float64,

    /// 32-bit integer.
    Int32,

    /// 32-bit float.
    Float32,

    /// Bytes with the fized size.
    Bytes(usize),

    /// ASCII string with the size limit.
    String(usize),

    /// Arbitrary size bytes.
    Blob,

    /// Arbitrary size string.
    Text,
}


impl Datatype {
    /// Represent `x` as its bytes, In case of mismatch `None` will be returned.
    pub fn to_bytes(&self, x: &Dataunit) -> Option<Vec<u8>> {
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
            Self::String(len) => {
                if let Dataunit::S(x) = x {
                    let mut block = x.as_bytes().to_vec();
                    block.resize(*len, 0);
                    Some(block)
                } else {
                    None
                }
            },
            Self::Blob => {
                if let Dataunit::S(x) = x {
                    let block = BASE64_STANDARD.decode(x).unwrap();
                    Some(block)
                } else {
                    None
                }
            },
            Self::Text => {
                if let Dataunit::S(x) = x {
                    let block = x.as_bytes().to_vec();
                    Some(block)
                } else {
                    None
                }
            },
        }
    }

    /// Converts a byte slice into a data unit.
    pub fn from_bytes(&self, block: &[u8]) -> Dataunit {
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
            Self::String(len) => {
                let string = String::from_utf8_lossy(&block[..*len])
                    .trim_end_matches('\0').to_string();
                Dataunit::S(string)
            },
            Self::Blob => {
                let string = BASE64_STANDARD.encode(block);
                Dataunit::S(string)
            },
            Self::Text => {
                let string = String::from_utf8_lossy(block)
                    .trim_end_matches('\0').to_string();
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
            Self::String(len) => *len,
            Self::Blob => 0,
            Self::Text => 0,
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
            Self::String(len) => format!("String[{}]", len),
            Self::Blob => "Blob".to_string(),
            Self::Text => "Text".to_string(),
        }
    }
}


fn unpack_from_pattern(text: &str, pref: &str, suff: &str) -> Option<String> {
    text.strip_prefix(&pref)
        .and_then(|s| s.strip_suffix(&suff))
        .and_then(|s| Some(s.to_string()))
}


impl FromStr for Datatype {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Int64" => Ok(Self::Int64),
            "Float64" => Ok(Self::Float64),
            "Int32" => Ok(Self::Int32),
            "Float32" => Ok(Self::Float32),
            "Blob" => Ok(Self::Blob),
            "Text" => Ok(Self::Text),
            _ => {
                if let Some(len_str) = unpack_from_pattern(s, "Bytes[", "]") {
                    let len = len_str.parse::<usize>()
                        .map_err(|_| "Unknown datatype".to_string())?;
                    Ok(Self::Bytes(len))
                } else if let Some(len_str) = unpack_from_pattern(s, "String[", 
                                                                  "]") {
                    let len = len_str.parse::<usize>()
                        .map_err(|_| "Unknown datatype".to_string())?;
                    Ok(Self::String(len))
                } else {
                    Err("Unknown datatype".to_string())
                }
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size() {
        assert_eq!(Datatype::Int64.size(), 8);
        assert_eq!(Datatype::Int32.size(), 4);
        assert_eq!(Datatype::Float64.size(), 8);
        assert_eq!(Datatype::Float32.size(), 4);
        assert_eq!(Datatype::Bytes(5).size(), 5);
        assert_eq!(Datatype::String(6).size(), 6);
        assert_eq!(Datatype::Blob.size(), 0);
        assert_eq!(Datatype::Text.size(), 0);
    }

    #[test]
    fn test_convert_string() {
        assert_eq!(Datatype::Int32.to_string(), "Int32");
        assert_eq!(Datatype::Bytes(25).to_string(), "Bytes[25]");
        assert_eq!(Datatype::String(26).to_string(), "String[26]");

        assert_eq!("Int32".parse::<Datatype>(), Ok(Datatype::Int32));
        assert_eq!("Bytes[25]".parse::<Datatype>(), Ok(Datatype::Bytes(25)));
        assert_eq!("String[26]".parse::<Datatype>(), Ok(Datatype::String(26)));

        assert_eq!("Boolean".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
        assert_eq!("Bytes[xxx]".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
        assert_eq!("Bytes[-12]".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
        assert_eq!("String[xxx]".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
        assert_eq!("String[-12]".parse::<Datatype>(), 
                   Err("Unknown datatype".to_string()));
    }

    #[test]
    fn test_dataunit_convert() {
        assert_eq!(
            Datatype::Int64.to_bytes(&Dataunit::I(25)).unwrap(), 
            vec![25, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            Datatype::Int32.to_bytes(&Dataunit::I(25)).unwrap(), 
            vec![25, 0, 0, 0]
        );
        assert_eq!(
            Datatype::Float64.to_bytes(&Dataunit::F(3.14)).unwrap(), 
            vec![31, 133, 235, 81, 184, 30, 9, 64]
        );
        assert_eq!(
            Datatype::Float32.to_bytes(&Dataunit::F(3.14)).unwrap(), 
            vec![195, 245, 72, 64]
        );
        assert_eq!(
            Datatype::Bytes(5).to_bytes(
                &Dataunit::S("+uwgVQA=".to_string())
            ).unwrap(), 
            vec![250, 236, 32, 85, 0]
        );
        assert_eq!(
            Datatype::String(6).to_bytes(
                &Dataunit::S("Qwe".to_string())
            ).unwrap(), 
            vec![81, 119, 101, 0, 0, 0]
        );
        assert_eq!(
            Datatype::Blob.to_bytes(
                &Dataunit::S("+uwgVQA=".to_string())
            ).unwrap(), 
            vec![250, 236, 32, 85, 0]
        );
        assert_eq!(
            Datatype::Text.to_bytes(
                &Dataunit::S("Qwe".to_string())
            ).unwrap(), 
            vec![81, 119, 101]
        );

        assert_eq!(
            Datatype::Int64.from_bytes(&[25, 0, 0, 0, 0, 0, 0, 0]), 
            Dataunit::I(25)
        );
        assert_eq!(
            Datatype::Int32.from_bytes(&[25, 0, 0, 0]), 
            Dataunit::I(25)
        );
        assert_eq!(
            Datatype::Float64.from_bytes(&[31, 133, 235, 81, 184, 30, 9, 64]), 
            Dataunit::F(3.14)
        );
        assert_eq!(
            Datatype::Float32.from_bytes(&[195, 245, 72, 64]), 
            Dataunit::F(3.140000104904175)
        );
        assert_eq!(
            Datatype::Bytes(5).from_bytes(&[250, 236, 32, 85, 0]), 
            Dataunit::S("+uwgVQA=".to_string())
        );
        assert_eq!(
            Datatype::String(5).from_bytes(&[81, 119, 101, 0, 0, 0]), 
            Dataunit::S("Qwe".to_string())
        );
        assert_eq!(
            Datatype::Blob.from_bytes(&[250, 236, 32, 85, 0]), 
            Dataunit::S("+uwgVQA=".to_string())
        );
        assert_eq!(
            Datatype::Text.from_bytes(&[81, 119, 101, 0]), 
            Dataunit::S("Qwe".to_string())
        );
    }
}
