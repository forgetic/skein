#![expect(clippy::format_push_string, reason = "generator assembles reviewed source fragments")]

//! Checked wire and heap ceilings for codec.md, section 4.

use crate::{Declaration, Enumeration, Record, Schema, Type, emit_limits};

fn rust_type(ty: &Type) -> String {
    match ty {
        Type::U8 => "u8".into(),
        Type::U16 => "u16".into(),
        Type::U32 => "u32".into(),
        Type::U64 => "u64".into(),
        Type::Bool => "bool".into(),
        Type::Duration => "skein_lib::Duration".into(),
        Type::Fixed(bound) => format!("[u8; {bound}]"),
        Type::Bytes(_) | Type::Text(_) => "Box<[u8]>".into(),
        Type::List(_, item) => format!("List<{}>", rust_type(item)),
        Type::Option(item) => format!("Option<{}>", rust_type(item)),
        Type::Named(name) => name.clone(),
    }
}

fn bytes_expr(ty: &Type, bound: &str) -> String {
    match ty {
        Type::U8 | Type::Bool => "1_u64".into(),
        Type::U16 => "2_u64".into(),
        Type::U32 => "4_u64".into(),
        Type::U64 | Type::Duration => "8_u64".into(),
        Type::Fixed(length) => format!("u64::from({length}_u32)"),
        Type::Bytes(_) | Type::Text(_) => format!("4_u64.checked_add(u64::from(limits.{bound}))?"),
        Type::List(_, item) => {
            let item_expr = bytes_expr(item, &format!("{bound}_item"));
            format!("4_u64.checked_add(u64::from(limits.{bound}).checked_mul({item_expr})?)?")
        }
        Type::Option(item) => {
            let item_expr = bytes_expr(item, &format!("{bound}_some"));
            format!("1_u64.checked_add({item_expr})?")
        }
        Type::Named(name) => format!("{name}::worst_case_bytes(limits)?"),
    }
}

fn heap_expr(ty: &Type, bound: &str) -> String {
    match ty {
        Type::U8 | Type::U16 | Type::U32 | Type::U64 | Type::Bool | Type::Duration | Type::Fixed(_) => "0_u64".into(),
        Type::Bytes(_) | Type::Text(_) => format!("u64::from(limits.{bound})"),
        Type::List(_, item) => {
            let item_expr = heap_expr(item, &format!("{bound}_item"));
            format!(
                "List::<{}>::worst_case(limits.{bound})?.checked_add(u64::from(limits.{bound}).checked_mul({item_expr})?)?",
                rust_type(item)
            )
        }
        Type::Option(item) => heap_expr(item, &format!("{bound}_some")),
        Type::Named(name) => format!("{name}::worst_case_heap(limits)?"),
    }
}

fn emit_record(record: &Record, out: &mut String) {
    let size_mut = if record.fields.is_empty() { "" } else { "mut " };
    out.push_str(&format!("impl {} {{\n    /// Maximum encoded bytes under these limits.\n    #[must_use]\n    pub fn worst_case_bytes(limits: &Limits) -> Option<u64> {{\n        if !limits_valid(limits) {{ return None; }}\n        let {size_mut}size = {}_u64;\n", record.name, if record.versioned { 2_u64 } else { 0_u64 }));
    for field in &record.fields {
        let bound = format!("{}_{}", emit_limits::snake(&record.name), field.name);
        out.push_str(&format!("        size = size.checked_add({})?;\n", bytes_expr(&field.ty, &bound)));
    }
    let heap_mut = if record.fields.is_empty() { "" } else { "mut " };
    out.push_str(&format!("        Some(size)\n    }}\n\n    /// Maximum heap held by one decoded value under these limits.\n    #[must_use]\n    pub fn worst_case_heap(limits: &Limits) -> Option<u64> {{\n        if !limits_valid(limits) {{ return None; }}\n        let {heap_mut}heap = 0_u64;\n"));
    for field in &record.fields {
        let bound = format!("{}_{}", emit_limits::snake(&record.name), field.name);
        out.push_str(&format!("        heap = heap.checked_add({})?;\n", heap_expr(&field.ty, &bound)));
    }
    out.push_str("        Some(heap)\n    }\n}\n\n");
}

fn emit_enum(enumeration: &Enumeration, out: &mut String) {
    let biggest_mut = if enumeration.variants.iter().any(|variant| variant.record.is_some()) { "mut " } else { "" };
    out.push_str(&format!("impl {} {{\n    /// Maximum encoded bytes among variants.\n    #[must_use]\n    pub fn worst_case_bytes(limits: &Limits) -> Option<u64> {{\n        if !limits_valid(limits) {{ return None; }}\n        let {biggest_mut}biggest = 0_u64;\n", enumeration.name));
    for variant in &enumeration.variants {
        if let Some(record) = &variant.record {
            out.push_str(&format!("        biggest = biggest.max({record}::worst_case_bytes(limits)?);\n"));
        }
    }
    out.push_str(&format!("        1_u64.checked_add(biggest)\n    }}\n\n    /// Maximum heap held by any decoded variant.\n    #[must_use]\n    pub fn worst_case_heap(limits: &Limits) -> Option<u64> {{\n        if !limits_valid(limits) {{ return None; }}\n        let {biggest_mut}biggest = 0_u64;\n"));
    for variant in &enumeration.variants {
        if let Some(record) = &variant.record {
            out.push_str(&format!("        biggest = biggest.max({record}::worst_case_heap(limits)?);\n"));
        }
    }
    out.push_str("        Some(biggest)\n    }\n}\n\n");
}

pub(crate) fn emit(schema: &Schema, out: &mut String) {
    let bounds = emit_limits::bound_names(schema);
    if bounds.is_empty() {
        out.push_str("fn limits_valid(_limits: &Limits) -> bool { true }\n\n");
    } else {
        out.push_str("fn limits_valid(limits: &Limits) -> bool {\n");
        let checks = bounds.iter().map(|(name, _)| format!("limits.{name} <= CEILINGS.{name}")).collect::<Vec<_>>();
        out.push_str(&format!("    {}\n}}\n\n", checks.join(" && ")));
    }
    for declaration in &schema.declarations {
        match declaration {
            Declaration::Record(record) => emit_record(record, out),
            Declaration::Enum(enumeration) => emit_enum(enumeration, out),
        }
    }
    let biggest_mut = if schema.declarations.is_empty() { "" } else { "mut " };
    out.push_str(&format!("/// Maximum wire bytes among this family's top-level types.\n#[must_use]\npub fn worst_case_bytes(limits: &Limits) -> Option<u64> {{\n    if !limits_valid(limits) {{ return None; }}\n    let {biggest_mut}biggest = 0_u64;\n"));
    for declaration in &schema.declarations {
        let name = match declaration {
            Declaration::Record(record) => &record.name,
            Declaration::Enum(enumeration) => &enumeration.name,
        };
        out.push_str(&format!("    biggest = biggest.max({name}::worst_case_bytes(limits)?);\n"));
    }
    out.push_str(&format!("    Some(biggest)\n}}\n\n/// Maximum heap held by one decoded top-level value.\n#[must_use]\npub fn worst_case_heap(limits: &Limits) -> Option<u64> {{\n    if !limits_valid(limits) {{ return None; }}\n    let {biggest_mut}biggest = 0_u64;\n"));
    for declaration in &schema.declarations {
        let name = match declaration {
            Declaration::Record(record) => &record.name,
            Declaration::Enum(enumeration) => &enumeration.name,
        };
        out.push_str(&format!("    biggest = biggest.max({name}::worst_case_heap(limits)?);\n"));
    }
    out.push_str("    Some(biggest)\n}\n");
}
