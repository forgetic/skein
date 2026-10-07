#![expect(clippy::format_push_string, reason = "generator assembles reviewed source fragments")]

//! Limits and typed field paths for codec.md, section 4.

use crate::{Declaration, Schema, Type};

pub(crate) fn snake(name: &str) -> String {
    let mut out = String::new();
    for character in name.chars() {
        if character.is_ascii_uppercase() {
            if !out.is_empty() {
                out.push('_');
            }
            out.push(character.to_ascii_lowercase());
        } else {
            out.push(character);
        }
    }
    out
}

pub(crate) fn pascal(name: &str) -> String {
    let mut out = String::new();
    let mut upper = true;
    for character in name.chars() {
        if character == '_' {
            upper = true;
        } else if upper {
            out.push(character.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(character);
        }
    }
    out
}

pub(crate) fn bound_names(schema: &Schema) -> Vec<(String, u32)> {
    let mut result = Vec::new();
    for declaration in &schema.declarations {
        if let Declaration::Record(record) = declaration {
            for field in &record.fields {
                let prefix = format!("{}_{}", snake(&record.name), field.name);
                collect_bound_names(&field.ty, &prefix, &mut result);
            }
        }
    }
    result
}

pub(crate) fn bound_paths(schema: &Schema) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for declaration in &schema.declarations {
        if let Declaration::Record(record) = declaration {
            for field in &record.fields {
                let prefix = format!("{}_{}", snake(&record.name), field.name);
                let path = format!("{}{}", record.name, pascal(&field.name));
                let mut bounds = Vec::new();
                collect_bound_names(&field.ty, &prefix, &mut bounds);
                for (name, _) in bounds {
                    result.push((name, path.clone()));
                }
            }
        }
    }
    result
}

fn collect_bound_names(ty: &Type, name: &str, result: &mut Vec<(String, u32)>) {
    match ty {
        Type::Bytes(bound) | Type::Text(bound) => result.push((name.into(), *bound)),
        Type::List(bound, item) => {
            result.push((name.into(), *bound));
            collect_bound_names(item, &format!("{name}_item"), result);
        }
        Type::Option(item) => collect_bound_names(item, &format!("{name}_some"), result),
        Type::U8
        | Type::U16
        | Type::U32
        | Type::U64
        | Type::Bool
        | Type::Duration
        | Type::Fixed(_)
        | Type::Named(_) => {}
    }
}

pub(crate) fn emit(schema: &Schema, out: &mut String) {
    let bounds = bound_names(schema);
    if bounds.is_empty() {
        out.push_str("/// The family has no adjustable bounds.\n#[derive(Clone, Debug, PartialEq, Eq, Hash)]\npub struct Limits {\n");
    } else {
        out.push_str("/// The family's adjustable limits, each no larger than its ceiling.\n#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]\npub struct Limits {\n");
    }
    for (name, _) in &bounds {
        out.push_str(&format!("    pub {name}: u32,\n"));
    }
    out.push_str("}\n\n/// The schema's maximum limits.\npub const CEILINGS: Limits = Limits {\n");
    for (name, bound) in &bounds {
        out.push_str(&format!("    {name}: {bound},\n"));
    }
    out.push_str("};\n\n/// A field or tag that caused a codec problem.\n#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]\npub enum Path {\n");
    for declaration in &schema.declarations {
        match declaration {
            Declaration::Record(record) => {
                for field in &record.fields {
                    let variant = format!("{}{}", record.name, pascal(&field.name));
                    out.push_str(&format!("    /// The {} field of {}.\n    {variant},\n", field.name, record.name));
                }
                if record.versioned {
                    out.push_str(&format!("    /// The version of {}.\n    {}Version,\n", record.name, record.name));
                }
                out.push_str(&format!("    /// Bytes after {}.\n    {}Tail,\n", record.name, record.name));
            }
            Declaration::Enum(enumeration) => {
                out.push_str(&format!("    /// The tag of {}.\n    {}Tag,\n", enumeration.name, enumeration.name));
            }
        }
    }
    out.push_str("}\n\n/// A decoding or bound failure at a schema path.\n#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]\npub struct Problem {\n    pub path: Path,\n    pub reason: skein_codec::Reason,\n}\n\n");
}
