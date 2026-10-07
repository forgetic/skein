//! Declaration order, names and encoded ceilings (codec.md, sections 3-4).

use alloc::collections::{BTreeMap, BTreeSet};

use crate::{Declaration, Enumeration, Record, Schema, Type, parse::Error};

fn error(line: usize, message: impl Into<String>) -> Error {
    Error { line, message: message.into() }
}

fn checked_add(left: u64, right: u64, line: usize) -> Result<u64, Error> {
    left.checked_add(right).ok_or_else(|| error(line, "encoded worst case overflows u64"))
}

fn size_of_type(ty: &Type, prior: &BTreeMap<String, (bool, u64)>, line: usize) -> Result<u64, Error> {
    match ty {
        Type::U8 | Type::Bool => Ok(1),
        Type::U16 => Ok(2),
        Type::U32 => Ok(4),
        Type::U64 | Type::Duration => Ok(8),
        Type::Fixed(bound) => Ok(u64::from(*bound)),
        Type::Bytes(bound) | Type::Text(bound) => checked_add(4, u64::from(*bound), line),
        Type::List(count, item) => {
            let item_size = size_of_type(item, prior, line)?;
            let contents = u64::from(*count)
                .checked_mul(item_size)
                .ok_or_else(|| error(line, "encoded worst case overflows u64"))?;
            checked_add(4, contents, line)
        }
        Type::Option(item) => checked_add(1, size_of_type(item, prior, line)?, line),
        Type::Named(name) => prior
            .get(name)
            .map(|(_, size)| *size)
            .ok_or_else(|| error(line, format!("type {name} used before declaration"))),
    }
}

fn record_size(record: &Record, prior: &BTreeMap<String, (bool, u64)>) -> Result<u64, Error> {
    let mut names = BTreeSet::new();
    let mut size = if record.versioned { 2 } else { 0 };
    for field in &record.fields {
        if !names.insert(&field.name) {
            return Err(error(field.line, format!("duplicate field {}", field.name)));
        }
        let field_size = size_of_type(&field.ty, prior, field.line)?;
        size = checked_add(size, field_size, field.line)?;
    }
    Ok(size)
}

fn enum_size(enumeration: &Enumeration, prior: &BTreeMap<String, (bool, u64)>) -> Result<u64, Error> {
    if enumeration.variants.is_empty() {
        return Err(error(enumeration.line, "enum has no variants"));
    }
    if enumeration.variants.len() > 256 {
        return Err(error(enumeration.line, "enum has more than 256 variants"));
    }
    let mut names = BTreeSet::new();
    let mut biggest = 0;
    for variant in &enumeration.variants {
        if !names.insert(&variant.name) {
            return Err(error(variant.line, format!("duplicate variant {}", variant.name)));
        }
        if let Some(record_name) = &variant.record {
            let (is_record, size) = prior
                .get(record_name)
                .ok_or_else(|| error(variant.line, format!("type {record_name} used before declaration")))?;
            if !is_record {
                return Err(error(variant.line, format!("variant {} must hold one record", variant.name)));
            }
            biggest = biggest.max(*size);
        }
    }
    checked_add(1, biggest, enumeration.line)
}

pub(crate) fn check(schema: &Schema) -> Result<(), Error> {
    let mut prior = BTreeMap::new();
    for declaration in &schema.declarations {
        let (name, line, is_record, size) = match declaration {
            Declaration::Record(record) => (&record.name, record.line, true, record_size(record, &prior)?),
            Declaration::Enum(enumeration) => {
                (&enumeration.name, enumeration.line, false, enum_size(enumeration, &prior)?)
            }
        };
        if prior.contains_key(name) {
            return Err(error(line, format!("duplicate type {name}")));
        }
        if size > u64::from(u32::MAX) {
            return Err(error(line, format!("encoded worst case for {name} exceeds u32")));
        }
        prior.insert(name.clone(), (is_record, size));
    }
    Ok(())
}
