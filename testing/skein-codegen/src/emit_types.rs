#![expect(clippy::format_push_string, reason = "generator assembles reviewed source fragments")]

//! Sealed record and enum values for codec.md, section 4.

use crate::{Declaration, Enumeration, Field, Record, Schema, Type, emit_limits};

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

fn accessor_type(ty: &Type) -> String {
    match ty {
        Type::U8 | Type::U16 | Type::U32 | Type::U64 | Type::Bool | Type::Duration => rust_type(ty),
        Type::Bytes(_) | Type::Text(_) => "&[u8]".into(),
        Type::Fixed(_) | Type::List(_, _) | Type::Option(_) | Type::Named(_) => format!("&{}", rust_type(ty)),
    }
}

fn accessor_expr(field: &Field) -> String {
    match field.ty {
        Type::U8 | Type::U16 | Type::U32 | Type::U64 | Type::Bool | Type::Duration => format!("self.{}", field.name),
        Type::Fixed(_) | Type::List(_, _) | Type::Option(_) | Type::Named(_) | Type::Bytes(_) | Type::Text(_) => {
            format!("&self.{}", field.name)
        }
    }
}

fn emit_check(ty: &Type, value: &str, bound_name: &str, path: &str, depth: usize, out: &mut String) {
    let problem = format!("Problem {{ path: Path::{path}, reason: skein_codec::Reason::Bound }}");
    match ty {
        Type::Bytes(_) | Type::Text(_) => {
            out.push_str(&format!("        if {value}.len() > usize::try_from(limits.{bound_name}.min(CEILINGS.{bound_name})).expect(\"u32 fits usize\") {{ return Err({problem}); }}\n"));
            if let Type::Text(_) = ty {
                out.push_str(&format!("        if !skein_codec::text_is_valid({value}.as_ref()) {{ return Err(Problem {{ path: Path::{path}, reason: skein_codec::Reason::Utf8 }}); }}\n"));
            }
        }
        Type::List(_, item) => {
            out.push_str(&format!("        if {value}.capacity() > limits.{bound_name}.min(CEILINGS.{bound_name}) {{ return Err({problem}); }}\n"));
            let item_name = format!("item_{depth}");
            out.push_str(&format!("        for {item_name} in {value}.as_slice() {{\n"));
            emit_check(
                item,
                &item_name,
                &format!("{bound_name}_item"),
                path,
                depth.checked_add(1).expect("schema nesting fits usize"),
                out,
            );
            out.push_str("        }\n");
        }
        Type::Option(item) => {
            let item_name = format!("some_{depth}");
            if matches!(
                **item,
                Type::U8 | Type::U16 | Type::U32 | Type::U64 | Type::Bool | Type::Duration | Type::Fixed(_)
            ) {
                return;
            }
            out.push_str(&format!("        if let Some({item_name}) = {value}.as_ref() {{\n"));
            emit_check(
                item,
                &item_name,
                &format!("{bound_name}_some"),
                path,
                depth.checked_add(1).expect("schema nesting fits usize"),
                out,
            );
            out.push_str("        }\n");
        }
        Type::Named(_) => out.push_str(&format!("        {value}.check(limits)?;\n")),
        Type::U8 | Type::U16 | Type::U32 | Type::U64 | Type::Bool | Type::Duration | Type::Fixed(_) => {}
    }
}

fn emit_limit_checks(schema: &Schema, out: &mut String) {
    for (name, path) in emit_limits::bound_paths(schema) {
        out.push_str(&format!("        if limits.{name} > CEILINGS.{name} {{ return Err(Problem {{ path: Path::{path}, reason: skein_codec::Reason::Bound }}); }}\n"));
    }
}

fn emit_record(schema: &Schema, record: &Record, out: &mut String) {
    out.push_str(&format!(
        "/// Movable fields of {}.\n#[derive(Clone, Debug, PartialEq, Eq, Hash)]\npub struct {}Parts {{\n",
        record.name, record.name
    ));
    for field in &record.fields {
        out.push_str(&format!(
            "    /// The {} field.\n    pub {}: {},\n",
            field.name,
            field.name,
            rust_type(&field.ty)
        ));
    }
    out.push_str("}\n\n");
    out.push_str(&format!(
        "/// {} in this codec family.\n#[derive(Clone, Debug, PartialEq, Eq, Hash)]\npub struct {} {{\n",
        record.name, record.name
    ));
    for field in &record.fields {
        out.push_str(&format!("    {}: {},\n", field.name, rust_type(&field.ty)));
    }
    out.push_str("}\n\n");
    out.push_str(&format!("impl {} {{\n", record.name));
    let members = record.fields.iter().map(|field| field.name.as_str()).collect::<Vec<_>>().join(", ");
    out.push_str(&format!("    /// Makes a value within the given limits.\n    pub fn new(limits: &Limits, parts: {}Parts) -> Result<Self, Problem> {{\n        let {}Parts {{ {members} }} = parts;\n        let value = Self {{ {members} }};\n        value.check(limits)?;\n        Ok(value)\n    }}\n\n", record.name, record.name));
    for field in &record.fields {
        out.push_str(&format!(
            "    /// Reads the {} field.\n    #[must_use]\n    pub fn {}(&self) -> {} {{ {} }}\n\n",
            field.name,
            field.name,
            accessor_type(&field.ty),
            accessor_expr(field)
        ));
    }
    let moved =
        record.fields.iter().map(|field| format!("{}: self.{}", field.name, field.name)).collect::<Vec<_>>().join(", ");
    out.push_str(&format!("    /// Moves the fields out without copying.\n    #[must_use]\n    pub fn into_parts(self) -> {}Parts {{ {}Parts {{ {moved} }} }}\n\n", record.name, record.name));
    out.push_str("    fn check(&self, limits: &Limits) -> Result<(), Problem> {\n");
    emit_limit_checks(schema, out);
    for field in &record.fields {
        let bound_name = format!("{}_{}", emit_limits::snake(&record.name), field.name);
        let path = format!("{}{}", record.name, emit_limits::pascal(&field.name));
        emit_check(&field.ty, &format!("self.{}", field.name), &bound_name, &path, 0, out);
    }
    out.push_str("        Ok(())\n    }\n}\n\n");
}

fn emit_enum(schema: &Schema, enumeration: &Enumeration, out: &mut String) {
    out.push_str(&format!(
        "/// {} in this codec family.\n#[derive(Clone, Debug, PartialEq, Eq, Hash)]\npub enum {} {{\n",
        enumeration.name, enumeration.name
    ));
    for variant in &enumeration.variants {
        let name = emit_limits::pascal(&variant.name);
        match &variant.record {
            Some(record) => {
                out.push_str(&format!("    /// {} carrying {}.\n    {name}({record}),\n", variant.name, record));
            }
            None => out.push_str(&format!("    /// {} without a payload.\n    {name},\n", variant.name)),
        }
    }
    out.push_str("}\n\n");
    out.push_str(&format!("impl {} {{\n    /// Checks the payload against the given limits.\n    pub fn new(limits: &Limits, value: Self) -> Result<Self, Problem> {{\n        value.check(limits)?;\n        Ok(value)\n    }}\n\n", enumeration.name));
    out.push_str("    fn check(&self, limits: &Limits) -> Result<(), Problem> {\n        match self {\n");
    // Every arm is explicit; payload-free variants can share one arm.
    let unit_variants = enumeration
        .variants
        .iter()
        .filter(|variant| variant.record.is_none())
        .map(|variant| format!("Self::{}", emit_limits::pascal(&variant.name)))
        .collect::<Vec<_>>();
    for variant in &enumeration.variants {
        let name = emit_limits::pascal(&variant.name);
        if variant.record.is_some() {
            out.push_str(&format!("            Self::{name}(record) => record.check(limits),\n"));
        }
    }
    if !unit_variants.is_empty() {
        out.push_str(&format!("            {} => Ok(()),\n", unit_variants.join(" | ")));
    }
    out.push_str("        }?;\n");
    emit_limit_checks(schema, out);
    out.push_str("        Ok(())\n    }\n}\n\n");
}

pub(crate) fn emit(schema: &Schema, out: &mut String) {
    for declaration in &schema.declarations {
        match declaration {
            Declaration::Record(record) => emit_record(schema, record, out),
            Declaration::Enum(enumeration) => emit_enum(schema, enumeration, out),
        }
    }
}
