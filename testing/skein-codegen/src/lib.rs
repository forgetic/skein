//! A schema parser and generator for bounded codecs (codec.md, sections 3-6).
//! The schema and declaration tree are author-owned; this crate has no peer
//! input or running machine state. [`parse`] validates a schema before code
//! generation.

extern crate alloc;

mod check;
mod parse;

pub use parse::{Error, parse};

/// One schema version of a family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub family: String,
    pub version: u16,
    pub declarations: Vec<Declaration>,
}

/// A record or enumeration in declaration order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Declaration {
    Record(Record),
    Enum(Enumeration),
}

/// An ordered group of fields, optionally with a version prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub line: usize,
    pub name: String,
    pub versioned: bool,
    pub fields: Vec<Field>,
}

/// An ordered group of variants, each holding at most one earlier record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enumeration {
    pub line: usize,
    pub name: String,
    pub variants: Vec<Variant>,
}

/// A named field of a record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub line: usize,
    pub name: String,
    pub ty: Type,
}

/// A named variant with an optional record payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Variant {
    pub line: usize,
    pub name: String,
    pub record: Option<String>,
}

/// A field's bounded wire type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    U8,
    U16,
    U32,
    U64,
    Bool,
    Duration,
    Fixed(u32),
    Bytes(u32),
    Text(u32),
    List(u32, Box<Type>),
    Option(Box<Type>),
    Named(String),
}
