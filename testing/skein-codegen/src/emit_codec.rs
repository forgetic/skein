#![expect(clippy::format_push_string, reason = "generator assembles reviewed source fragments")]

//! Measured, canonical wire operations for codec.md, section 4.

use crate::{Declaration, Enumeration, Record, Schema, Type, emit_limits, emit_types};

fn member(record: &Record, name: &str) -> String {
    match emit_types::bool_index(record, name) {
        Some(_) => format!("self.{name}()"),
        None => format!("self.{name}"),
    }
}

fn add_size(size: &str, out: &mut String) {
    out.push_str(&format!("        size = size.checked_add({size}).expect(\"schema ceilings fit u32\");\n"));
}

fn fixed_size(ty: &Type) -> Option<u32> {
    match ty {
        Type::U8 | Type::Bool => Some(1),
        Type::U16 => Some(2),
        Type::U32 => Some(4),
        Type::U64 | Type::Duration => Some(8),
        Type::Fixed(bound) => Some(*bound),
        Type::Bytes(_) | Type::Text(_) | Type::List(_, _) | Type::Option(_) | Type::Named(_) => None,
    }
}

fn measure_type(ty: &Type, value: &str, depth: usize, out: &mut String) {
    match ty {
        Type::U8 | Type::Bool => add_size("1", out),
        Type::U16 => add_size("2", out),
        Type::U32 => add_size("4", out),
        Type::U64 | Type::Duration => add_size("8", out),
        Type::Fixed(bound) => add_size(&emit_limits::literal(*bound), out),
        Type::Bytes(_) | Type::Text(_) => {
            add_size("4", out);
            add_size(&format!("u32::try_from({value}.len()).expect(\"field ceiling fits u32\")"), out);
        }
        Type::List(_, item) => {
            add_size("4", out);
            let item_name = format!("item_{depth}");
            out.push_str(&format!("        for {item_name} in {value}.as_slice() {{\n"));
            measure_type(item, &item_name, depth.checked_add(1).expect("schema depth"), out);
            out.push_str("        }\n");
        }
        Type::Option(item) => {
            add_size("1", out);
            match fixed_size(item) {
                Some(fixed) => {
                    out.push_str(&format!("        if {value}.is_some() {{\n"));
                    add_size(&emit_limits::literal(fixed), out);
                    out.push_str("        }\n");
                }
                None => {
                    let item_name = format!("some_{depth}");
                    out.push_str(&format!("        if let Some({item_name}) = {value} {{\n"));
                    measure_type(item, &item_name, depth.checked_add(1).expect("schema depth"), out);
                    out.push_str("        }\n");
                }
            }
        }
        Type::Named(_) => add_size(&format!("{value}.measure()"), out),
    }
}

fn encode_type(ty: &Type, value: &str, depth: usize, out: &mut String) {
    match ty {
        Type::U8 => out.push_str(&format!("        writer.put(&[*{value}])?;\n")),
        Type::U16 | Type::U32 | Type::U64 => {
            out.push_str(&format!("        writer.put(&{value}.to_be_bytes())?;\n"));
        }
        Type::Bool => out.push_str(&format!("        writer.put(&[u8::from(*{value})])?;\n")),
        Type::Duration => out.push_str(&format!("        writer.put(&{value}.as_nanos().to_be_bytes())?;\n")),
        Type::Fixed(_) => out.push_str(&format!("        writer.put({value})?;\n")),
        Type::Bytes(_) | Type::Text(_) => {
            out.push_str(&format!(
                "        writer.put(&u32::try_from({value}.len()).expect(\"field ceiling fits u32\").to_be_bytes())?;\n"
            ));
            out.push_str(&format!("        writer.put({value}.as_ref())?;\n"));
        }
        Type::List(_, item) => {
            out.push_str(&format!("        writer.put(&{value}.len().to_be_bytes())?;\n"));
            let item_name = format!("item_{depth}");
            out.push_str(&format!("        for {item_name} in {value}.as_slice() {{\n"));
            encode_type(item, &item_name, depth.checked_add(1).expect("schema depth"), out);
            out.push_str("        }\n");
        }
        Type::Option(item) => {
            let item_name = format!("some_{depth}");
            out.push_str(&format!(
                "        match {value} {{\n            Some({item_name}) => {{\n                writer.put(&[1_u8])?;\n"
            ));
            encode_type(item, &item_name, depth.checked_add(1).expect("schema depth"), out);
            out.push_str("            }\n            None => writer.put(&[0_u8])?,\n        }\n");
        }
        Type::Named(_) => out.push_str(&format!("        {value}.encode(writer)?;\n")),
    }
}

fn short(path: &str) -> String {
    format!("Problem {{ path: Path::{path}, reason: skein_codec::Reason::Short }}")
}

fn min_bytes(ty: &Type, schema: &Schema) -> u32 {
    match ty {
        Type::U8 | Type::Bool | Type::Option(_) => 1,
        Type::U16 => 2,
        Type::U32 | Type::Bytes(_) | Type::Text(_) | Type::List(_, _) => 4,
        Type::U64 | Type::Duration => 8,
        Type::Fixed(bound) => *bound,
        Type::Named(name) => {
            let declaration = schema
                .declarations
                .iter()
                .find(|declaration| match declaration {
                    Declaration::Record(record) => record.name == *name,
                    Declaration::Enum(enumeration) => enumeration.name == *name,
                })
                .expect("named type declared earlier");
            match declaration {
                Declaration::Record(record) => {
                    let mut size = if record.versioned { 2_u32 } else { 0_u32 };
                    for field in &record.fields {
                        size = size.checked_add(min_bytes(&field.ty, schema)).expect("minimum fits checked ceiling");
                    }
                    size
                }
                Declaration::Enum(enumeration) => {
                    let mut size = u32::MAX;
                    for variant in &enumeration.variants {
                        let payload_size = match &variant.record {
                            Some(record) => min_bytes(&Type::Named(record.clone()), schema),
                            None => 0,
                        };
                        size = size.min(payload_size);
                    }
                    size.checked_add(1).expect("minimum fits checked ceiling")
                }
            }
        }
    }
}

fn empty_unversioned_record(schema: &Schema, name: &str) -> bool {
    schema.declarations.iter().any(|declaration| match declaration {
        Declaration::Record(record) => record.name == name && record.fields.is_empty() && !record.versioned,
        Declaration::Enum(_) => false,
    })
}

fn decode_type(ty: &Type, schema: &Schema, bound_name: &str, path: &str, depth: usize) -> String {
    let short = short(path);
    match ty {
        Type::U8 => format!("reader.u8().ok_or({short})?"),
        Type::U16 => format!("reader.u16().ok_or({short})?"),
        Type::U32 => format!("reader.u32().ok_or({short})?"),
        Type::U64 => format!("reader.u64().ok_or({short})?"),
        Type::Bool => format!(
            "match reader.u8().ok_or({short})? {{ 0 => false, 1 => true, _ => return Err(Problem {{ path: Path::{path}, reason: skein_codec::Reason::Bool }}) }}"
        ),
        Type::Duration => format!("skein_lib::Duration::from_nanos(reader.u64().ok_or({short})?)"),
        Type::Fixed(bound) => format!(
            "{{ let bytes = reader.bytes({}).ok_or({short})?; bytes.try_into().expect(\"fixed length checked\") }}",
            emit_limits::literal(*bound)
        ),
        Type::Bytes(_) => format!(
            "{{ let length = match skein_codec::read_len(reader, limits.{bound_name}.min(CEILINGS.{bound_name})) {{ Ok(length) => length, Err(reason) => return Err(Problem {{ path: Path::{path}, reason }}), }}; Box::from(reader.bytes(length).ok_or({short})?) }}"
        ),
        Type::Text(_) => format!(
            "match skein_codec::read_text(reader, limits.{bound_name}.min(CEILINGS.{bound_name})) {{ Ok(text) => text, Err(reason) => return Err(Problem {{ path: Path::{path}, reason }}), }}"
        ),
        Type::List(_, item) => {
            let item_expr = decode_type(
                item,
                schema,
                &format!("{bound_name}_item"),
                path,
                depth.checked_add(1).expect("schema depth"),
            );
            let list_name = format!("items_{depth}");
            let minimum = min_bytes(item, schema);
            let precheck = if minimum == 0 {
                String::new()
            } else {
                format!("if count > reader.remaining() / {} {{ return Err({short}); }} ", emit_limits::literal(minimum))
            };
            format!(
                "{{ let count = match skein_codec::read_count(reader, limits.{bound_name}.min(CEILINGS.{bound_name})) {{ Ok(count) => count, Err(reason) => return Err(Problem {{ path: Path::{path}, reason }}), }}; {precheck}let mut {list_name} = List::with_capacity(count); for _index in 0_u32..count {{ let item = {item_expr}; {list_name}.push(item).expect(\"count within capacity\"); }} {list_name} }}"
            )
        }
        Type::Option(item) => {
            let item_expr = decode_type(
                item,
                schema,
                &format!("{bound_name}_some"),
                path,
                depth.checked_add(1).expect("schema depth"),
            );
            format!(
                "match reader.u8().ok_or({short})? {{ 0 => None, 1 => Some({item_expr}), _ => return Err(Problem {{ path: Path::{path}, reason: skein_codec::Reason::Tag }}) }}"
            )
        }
        Type::Named(name) => {
            if empty_unversioned_record(schema, name) && emit_limits::bound_names(schema).is_empty() {
                format!("{name}::decode_from(limits, reader)")
            } else {
                format!("{name}::decode_from(limits, reader)?")
            }
        }
    }
}

fn emit_record(schema: &Schema, record: &Record, out: &mut String) {
    if record.fields.is_empty() && !record.versioned {
        out.push_str(&format!("impl {} {{\n    /// Measures this empty record's wire encoding.\n    #[must_use]\n    pub fn measure(&self) -> u32 {{ 0_u32 }}\n\n", record.name));
    } else {
        out.push_str(&format!("impl {} {{\n    /// Measures this record's wire encoding.\n    #[must_use]\n    pub fn measure(&self) -> u32 {{\n        let mut size = 0_u32;\n", record.name));
        if record.versioned {
            add_size("2", out);
        }
        for field in &record.fields {
            if fixed_size(&field.ty).is_none() {
                out.push_str(&format!("        let field_{} = &{};\n", field.name, member(record, &field.name)));
            }
            measure_type(&field.ty, &format!("field_{}", field.name), 0, out);
        }
        out.push_str("        size\n    }\n\n");
    }
    out.push_str("    /// Writes into a writer with room for the measured bytes.\n    pub fn encode(&self, writer: &mut skein_lib::Writer) -> Result<(), skein_lib::Overflow> {\n");
    if record.versioned {
        out.push_str(&format!("        writer.put(&{}_u16.to_be_bytes())?;\n", emit_limits::literal(schema.version)));
    }
    for field in &record.fields {
        out.push_str(&format!("        let field_{} = &{};\n", field.name, member(record, &field.name)));
        encode_type(&field.ty, &format!("field_{}", field.name), 0, out);
    }
    if record.fields.is_empty() && !record.versioned {
        out.push_str("        writer.put(&[])\n    }\n\n    /// Reads a whole record and refuses trailing bytes.\n    pub fn decode(limits: &Limits, reader: &mut skein_lib::Reader<'_>) -> Result<Self, Problem> {\n");
    } else {
        out.push_str("        Ok(())\n    }\n\n    /// Reads a whole record and refuses trailing bytes.\n    pub fn decode(limits: &Limits, reader: &mut skein_lib::Reader<'_>) -> Result<Self, Problem> {\n");
    }
    if record.fields.is_empty() && !record.versioned && emit_limits::bound_names(schema).is_empty() {
        out.push_str("        let value = Self::decode_from(limits, reader);\n");
    } else {
        out.push_str("        let value = Self::decode_from(limits, reader)?;\n");
    }
    out.push_str(&format!("        if !reader.is_empty() {{ return Err(Problem {{ path: Path::{}Tail, reason: skein_codec::Reason::Trailing }}); }}\n        Ok(value)\n    }}\n\n", record.name));
    if record.fields.is_empty() && !record.versioned {
        if emit_limits::bound_names(schema).is_empty() {
            out.push_str(&format!("    fn decode_from(limits: &Limits, _reader: &mut skein_lib::Reader<'_>) -> Self {{\n        Self::new(limits, {}Parts {{}})\n    }}\n}}\n\n", record.name));
        } else {
            out.push_str(&format!("    fn decode_from(limits: &Limits, _reader: &mut skein_lib::Reader<'_>) -> Result<Self, Problem> {{\n        Self::new(limits, {}Parts {{}})\n    }}\n}}\n\n", record.name));
        }
        return;
    }
    out.push_str(
        "    fn decode_from(limits: &Limits, reader: &mut skein_lib::Reader<'_>) -> Result<Self, Problem> {\n",
    );
    if record.versioned {
        out.push_str(&format!("        let version = reader.u16().ok_or({})?;\n        if version != {} {{ return Err(Problem {{ path: Path::{}Version, reason: skein_codec::Reason::Version }}); }}\n", short(&format!("{}Version", record.name)), emit_limits::literal(schema.version), record.name));
    }
    for field in &record.fields {
        let bound_name = format!("{}_{}", emit_limits::snake(&record.name), field.name);
        let path = format!("{}{}", record.name, emit_limits::pascal(&field.name));
        let expr = decode_type(&field.ty, schema, &bound_name, &path, 0);
        out.push_str(&format!("        let decoded_{} = {expr};\n", field.name));
    }
    let mut members = record
        .fields
        .iter()
        .filter(|field| !emit_types::grouped_bools(record) || !matches!(field.ty, Type::Bool))
        .map(|field| format!("{}: decoded_{}", field.name, field.name))
        .collect::<Vec<_>>();
    if emit_types::grouped_bools(record) {
        let bools = record
            .fields
            .iter()
            .filter(|field| matches!(field.ty, Type::Bool))
            .map(|field| format!("decoded_{}", field.name))
            .collect::<Vec<_>>()
            .join(", ");
        members.insert(0, format!("skein_bools: [{bools}]"));
    }
    let members = members.join(", ");
    if emit_limits::bound_names(schema).is_empty() {
        out.push_str(&format!("        Ok(Self::new(limits, {}Parts {{ {members} }}))\n    }}\n}}\n\n", record.name));
    } else {
        out.push_str(&format!("        Self::new(limits, {}Parts {{ {members} }})\n    }}\n}}\n\n", record.name));
    }
}

fn emit_enum(schema: &Schema, enumeration: &Enumeration, out: &mut String) {
    out.push_str(&format!("impl {} {{\n    /// Measures this variant's wire encoding.\n    #[must_use]\n    pub fn measure(&self) -> u32 {{\n        match self {{\n", enumeration.name));
    let unit_variants = enumeration
        .variants
        .iter()
        .filter(|variant| variant.record.is_none())
        .map(|variant| format!("Self::{}", emit_limits::pascal(&variant.name)))
        .collect::<Vec<_>>();
    for variant in &enumeration.variants {
        let name = emit_limits::pascal(&variant.name);
        if variant.record.is_some() {
            out.push_str(&format!("            Self::{name}(record) => 1_u32.checked_add(record.measure()).expect(\"schema ceilings fit u32\"),\n"));
        }
    }
    if !unit_variants.is_empty() {
        out.push_str(&format!("            {} => 1,\n", unit_variants.join(" | ")));
    }
    out.push_str("        }\n    }\n\n    /// Writes this variant's tag and payload.\n    pub fn encode(&self, writer: &mut skein_lib::Writer) -> Result<(), skein_lib::Overflow> {\n        match self {\n");
    for (index, variant) in enumeration.variants.iter().enumerate() {
        let name = emit_limits::pascal(&variant.name);
        let tag = u8::try_from(index).expect("at most 256 variants");
        match &variant.record {
            Some(_) => out.push_str(&format!(
                "            Self::{name}(record) => {{ writer.put(&[{tag}_u8])?; record.encode(writer)?; }}\n"
            )),
            None => out.push_str(&format!("            Self::{name} => writer.put(&[{tag}_u8])?,\n")),
        }
    }
    out.push_str("        }\n        Ok(())\n    }\n\n    /// Reads a whole variant and refuses trailing bytes.\n    pub fn decode(limits: &Limits, reader: &mut skein_lib::Reader<'_>) -> Result<Self, Problem> {\n        let value = Self::decode_from(limits, reader)?;\n");
    out.push_str(&format!("        if !reader.is_empty() {{ return Err(Problem {{ path: Path::{}Tag, reason: skein_codec::Reason::Trailing }}); }}\n        Ok(value)\n    }}\n\n", enumeration.name));
    out.push_str(&format!("    fn decode_from(limits: &Limits, reader: &mut skein_lib::Reader<'_>) -> Result<Self, Problem> {{\n        let tag = reader.u8().ok_or({})?;\n        let value = match tag {{\n", short(&format!("{}Tag", enumeration.name))));
    for (index, variant) in enumeration.variants.iter().enumerate() {
        let name = emit_limits::pascal(&variant.name);
        let tag = u8::try_from(index).expect("at most 256 variants");
        match &variant.record {
            Some(record) => {
                let suffix = if empty_unversioned_record(schema, record) && emit_limits::bound_names(schema).is_empty()
                {
                    ""
                } else {
                    "?"
                };
                out.push_str(&format!(
                    "            {tag} => Self::{name}({record}::decode_from(limits, reader){suffix}),\n"
                ));
            }
            None => out.push_str(&format!("            {tag} => Self::{name},\n")),
        }
    }
    if emit_limits::bound_names(schema).is_empty() {
        out.push_str(&format!("            _ => return Err(Problem {{ path: Path::{}Tag, reason: skein_codec::Reason::Tag }}),\n        }};\n        Ok(Self::new(limits, value))\n    }}\n}}\n\n", enumeration.name));
    } else {
        out.push_str(&format!("            _ => return Err(Problem {{ path: Path::{}Tag, reason: skein_codec::Reason::Tag }}),\n        }};\n        Self::new(limits, value)\n    }}\n}}\n\n", enumeration.name));
    }
}

pub(crate) fn emit(schema: &Schema, out: &mut String) {
    for declaration in &schema.declarations {
        match declaration {
            Declaration::Record(record) => emit_record(schema, record, out),
            Declaration::Enum(enumeration) => emit_enum(schema, enumeration, out),
        }
    }
}
