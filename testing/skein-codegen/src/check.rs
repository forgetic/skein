//! Declaration order, names and encoded ceilings (codec.md, sections 3-4).

use alloc::collections::{BTreeMap, BTreeSet};

use crate::{Declaration, Enumeration, Record, Schema, Type, emit_limits, parse::Error};

fn error(line: usize, message: impl Into<String>) -> Error {
    Error { line, message: message.into() }
}

fn keyword(name: &str) -> bool {
    matches!(
        name,
        "as" | "async"
            | "await"
            | "become"
            | "box"
            | "break"
            | "const"
            | "continue"
            | "crate"
            | "do"
            | "dyn"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "final"
            | "fn"
            | "for"
            | "gen"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "macro"
            | "macro_rules"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "override"
            | "priv"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "try"
            | "type"
            | "typeof"
            | "union"
            | "unsafe"
            | "unsized"
            | "use"
            | "virtual"
            | "where"
            | "while"
            | "yield"
    )
}

fn valid_type_name(name: &str) -> bool {
    let mut characters = name.chars();
    characters.next().is_some_and(|character| character.is_ascii_uppercase())
        && characters.all(|character| character.is_ascii_alphanumeric())
        && !keyword(name)
}

fn valid_member_name(name: &str) -> bool {
    let mut characters = name.chars();
    characters.next().is_some_and(|character| character.is_ascii_lowercase())
        && characters.all(|character| character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_')
        && !keyword(name)
}

fn reserved_field_name(name: &str) -> bool {
    matches!(
        name,
        "new"
            | "check"
            | "measure"
            | "encode"
            | "decode"
            | "decode_from"
            | "into_parts"
            | "worst_case_bytes"
            | "worst_case_heap"
    )
}

fn field_line_for_path(schema: &Schema, path: &str) -> usize {
    for declaration in &schema.declarations {
        if let Declaration::Record(record) = declaration {
            for field in &record.fields {
                if format!("{}{}", record.name, emit_limits::pascal(&field.name)) == path {
                    return field.line;
                }
            }
        }
    }
    unreachable!("each bound path belongs to a checked field")
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
        if !valid_member_name(&field.name) || reserved_field_name(&field.name) {
            return Err(error(field.line, format!("invalid generated field name {}", field.name)));
        }
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
    let mut emitted_variants = BTreeSet::new();
    let mut biggest = 0;
    for variant in &enumeration.variants {
        if !valid_member_name(&variant.name) {
            return Err(error(variant.line, format!("invalid generated variant name {}", variant.name)));
        }
        if !names.insert(&variant.name) {
            return Err(error(variant.line, format!("duplicate variant {}", variant.name)));
        }
        let emitted = emit_limits::pascal(&variant.name);
        if !emitted_variants.insert(emitted.clone()) {
            return Err(error(variant.line, format!("generated variant {emitted} is used twice")));
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
    let mut emitted_types = BTreeSet::from([
        "Box".to_string(),
        "Limits".to_string(),
        "List".to_string(),
        "Option".to_string(),
        "Path".to_string(),
        "Problem".to_string(),
        "Result".to_string(),
    ]);
    let mut paths = BTreeSet::new();
    for declaration in &schema.declarations {
        let (name, line, is_record, size) = match declaration {
            Declaration::Record(record) => (&record.name, record.line, true, record_size(record, &prior)?),
            Declaration::Enum(enumeration) => {
                (&enumeration.name, enumeration.line, false, enum_size(enumeration, &prior)?)
            }
        };
        if !valid_type_name(name) {
            return Err(error(line, format!("invalid generated type name {name}")));
        }
        if prior.contains_key(name) {
            return Err(error(line, format!("duplicate type {name}")));
        }
        if !emitted_types.insert(name.clone()) {
            return Err(error(line, format!("generated type name {name} is used twice")));
        }
        match declaration {
            Declaration::Record(record) => {
                let parts = format!("{}Parts", record.name);
                if !emitted_types.insert(parts.clone()) {
                    return Err(error(record.line, format!("generated type name {parts} is used twice")));
                }
                for field in &record.fields {
                    let path = format!("{}{}", record.name, emit_limits::pascal(&field.name));
                    if !paths.insert(path.clone()) {
                        return Err(error(field.line, format!("generated path {path} is used twice")));
                    }
                }
                let tail = format!("{}Tail", record.name);
                if !paths.insert(tail.clone()) {
                    return Err(error(record.line, format!("generated path {tail} is used twice")));
                }
                if record.versioned {
                    let version = format!("{}Version", record.name);
                    if !paths.insert(version.clone()) {
                        return Err(error(record.line, format!("generated path {version} is used twice")));
                    }
                }
            }
            Declaration::Enum(enumeration) => {
                let path = format!("{}Tag", enumeration.name);
                if !paths.insert(path.clone()) {
                    return Err(error(enumeration.line, format!("generated path {path} is used twice")));
                }
            }
        }
        if size > u64::from(u32::MAX) {
            return Err(error(line, format!("encoded worst case for {name} exceeds u32")));
        }
        prior.insert(name.clone(), (is_record, size));
    }
    let mut bound_names = BTreeSet::new();
    for (name, path) in emit_limits::bound_paths(schema) {
        if !bound_names.insert(name.clone()) {
            return Err(error(
                field_line_for_path(schema, &path),
                format!("generated limit name {name} is used twice"),
            ));
        }
    }
    Ok(())
}
