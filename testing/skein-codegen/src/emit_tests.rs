#![expect(clippy::format_push_string, reason = "generator assembles reviewed source fragments")]

//! Fixed-rule golden values and generated round trips (codec.md, section 6).

use crate::{Declaration, Enumeration, Golden, Record, Schema, Type, Variant, emit_limits, uses_box};

fn declaration<'a>(schema: &'a Schema, name: &str) -> &'a Declaration {
    schema
        .declarations
        .iter()
        .find(|declaration| match declaration {
            Declaration::Record(record) => record.name == name,
            Declaration::Enum(enumeration) => enumeration.name == name,
        })
        .expect("named type declared earlier")
}

fn pattern(bound: u32, text: bool) -> Vec<u8> {
    let source: &[u8] = if text { b"abc" } else { &[0, 0x7f, 0xff] };
    source.iter().copied().take(usize::try_from(bound).expect("u32 fits usize")).collect()
}

fn write_record(record: &Record, full: bool, schema: &Schema, out: &mut Vec<u8>) {
    if record.versioned {
        out.extend_from_slice(&schema.version.to_be_bytes());
    }
    for field in &record.fields {
        write_type(&field.ty, full, schema, out);
    }
}

fn write_variant(enumeration: &Enumeration, variant: &Variant, full: bool, schema: &Schema, out: &mut Vec<u8>) {
    let index =
        enumeration.variants.iter().position(|candidate| candidate.name == variant.name).expect("variant in enum");
    out.push(u8::try_from(index).expect("at most 256 variants"));
    if let Some(record) = &variant.record {
        match declaration(schema, record) {
            Declaration::Record(record) => write_record(record, full, schema, out),
            Declaration::Enum(_) => unreachable!("a variant only holds a record"),
        }
    }
}

fn write_type(ty: &Type, full: bool, schema: &Schema, out: &mut Vec<u8>) {
    match ty {
        Type::U8 => out.push(if full { u8::MAX } else { 0 }),
        Type::U16 => out.extend_from_slice(&(if full { u16::MAX } else { 0 }).to_be_bytes()),
        Type::U32 => out.extend_from_slice(&(if full { u32::MAX } else { 0 }).to_be_bytes()),
        Type::U64 | Type::Duration => out.extend_from_slice(&(if full { u64::MAX } else { 0 }).to_be_bytes()),
        Type::Bool => out.push(u8::from(full)),
        Type::Fixed(bound) => {
            for _ in 0_u32..*bound {
                out.push(if full { 0xa5 } else { 0 });
            }
        }
        Type::Bytes(bound) | Type::Text(bound) => {
            let bytes = if full { pattern(*bound, matches!(ty, Type::Text(_))) } else { Vec::new() };
            out.extend_from_slice(&u32::try_from(bytes.len()).expect("pattern fits u32").to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        Type::List(bound, item) => {
            let count = u32::from(full && *bound > 0);
            out.extend_from_slice(&count.to_be_bytes());
            if count > 0 {
                write_type(item, true, schema, out);
            }
        }
        Type::Option(item) => {
            out.push(u8::from(full));
            if full {
                write_type(item, true, schema, out);
            }
        }
        Type::Named(name) => match declaration(schema, name) {
            Declaration::Record(record) => write_record(record, full, schema, out),
            Declaration::Enum(enumeration) => {
                let variant = enumeration.variants.first().expect("enum nonempty");
                write_variant(enumeration, variant, full, schema, out);
            }
        },
    }
}

fn bytes_literal(bytes: &[u8]) -> String {
    let elements = bytes.iter().map(u8::to_string).collect::<Vec<_>>().join(", ");
    format!("&[{elements}]")
}

fn value_expr(ty: &Type, full: bool, schema: &Schema) -> String {
    match ty {
        Type::U8 => if full { "u8::MAX" } else { "0_u8" }.into(),
        Type::U16 => if full { "u16::MAX" } else { "0_u16" }.into(),
        Type::U32 => if full { "u32::MAX" } else { "0_u32" }.into(),
        Type::U64 => if full { "u64::MAX" } else { "0_u64" }.into(),
        Type::Bool => full.to_string(),
        Type::Duration => format!("skein_lib::Duration::from_nanos({})", if full { "u64::MAX" } else { "0_u64" }),
        Type::Fixed(bound) => format!("[{}_u8; {bound}]", if full { 0xa5_u8 } else { 0_u8 }),
        Type::Bytes(bound) | Type::Text(bound) => {
            let bytes = if full { pattern(*bound, matches!(ty, Type::Text(_))) } else { Vec::new() };
            let elements = bytes.iter().map(|byte| format!("{byte}_u8")).collect::<Vec<_>>().join(", ");
            format!("Box::from([{elements}].as_slice())")
        }
        Type::List(bound, item) => {
            if full && *bound > 0 {
                format!(
                    "{{ let mut items = skein_lib::List::with_capacity(1); items.push({}).expect(\"one slot\"); items }}",
                    value_expr(item, true, schema)
                )
            } else {
                "skein_lib::List::with_capacity(0)".into()
            }
        }
        Type::Option(item) => {
            if full {
                format!("Some({})", value_expr(item, true, schema))
            } else {
                "None".into()
            }
        }
        Type::Named(name) => match declaration(schema, name) {
            Declaration::Record(record) => record_expr(record, full, schema),
            Declaration::Enum(enumeration) => {
                let variant = enumeration.variants.first().expect("enum nonempty");
                variant_expr(enumeration, variant, full, schema)
            }
        },
    }
}

fn record_expr(record: &Record, full: bool, schema: &Schema) -> String {
    let fields = record
        .fields
        .iter()
        .map(|field| format!("{}: {}", field.name, value_expr(&field.ty, full, schema)))
        .collect::<Vec<_>>()
        .join(", ");
    let call = format!("{}::new(&CEILINGS, {}Parts {{ {fields} }})", record.name, record.name);
    if emit_limits::bound_names(schema).is_empty() {
        call
    } else {
        format!("{call}.expect(\"golden within ceilings\")")
    }
}

fn variant_expr(enumeration: &Enumeration, variant: &Variant, full: bool, schema: &Schema) -> String {
    let name = emit_limits::pascal(&variant.name);
    match &variant.record {
        Some(record) => match declaration(schema, record) {
            Declaration::Record(record) => {
                format!("{}::{name}({})", enumeration.name, record_expr(record, full, schema))
            }
            Declaration::Enum(_) => unreachable!("a variant only holds a record"),
        },
        None => format!("{}::{name}", enumeration.name),
    }
}

fn test_case(name: &str, ty: &str, value: &str, bytes: &[u8], out: &mut String) {
    let literal = bytes_literal(bytes);
    out.push_str(&format!("    #[test]\n    fn {name}() {{\n        let value = {value};\n        let golden: &[u8] = {literal};\n        let mut writer = skein_lib::Writer::new(usize::try_from(value.measure()).expect(\"size fits usize\"));\n        value.encode(&mut writer).expect(\"measured room\");\n        assert_eq!(writer.finish().as_ref(), golden);\n        assert_eq!({ty}::decode(&CEILINGS, &mut skein_lib::Reader::new(golden)), Ok(value));\n    }}\n\n"));
}

fn bound_test(record: &Record, field_index: usize, schema: &Schema, out: &mut String) {
    let field = record.fields.get(field_index).expect("field index");
    let (bound, item, is_list) = match &field.ty {
        Type::Bytes(bound) | Type::Text(bound) => (*bound, Vec::new(), false),
        Type::List(bound, item) => {
            let mut bytes = Vec::new();
            write_type(item, false, schema, &mut bytes);
            (*bound, bytes, true)
        }
        Type::U8
        | Type::U16
        | Type::U32
        | Type::U64
        | Type::Bool
        | Type::Duration
        | Type::Fixed(_)
        | Type::Option(_)
        | Type::Named(_) => return,
    };
    let Some(over) = bound.checked_add(1) else {
        return;
    };
    let mut prefix = Vec::new();
    if record.versioned {
        prefix.extend_from_slice(&schema.version.to_be_bytes());
    }
    let mut suffix = Vec::new();
    for (index, other) in record.fields.iter().enumerate() {
        if index < field_index {
            write_type(&other.ty, false, schema, &mut prefix);
        } else if index > field_index {
            write_type(&other.ty, false, schema, &mut suffix);
        }
    }
    let name = format!("bound_{}_{}", emit_limits::snake(&record.name), field.name);
    let path = format!("{}{}", record.name, emit_limits::pascal(&field.name));
    let item_length = if is_list { item.len() } else { 1 };
    out.push_str(&format!("    #[test]\n    fn {name}() {{\n        fn wire(count: u32, payload: bool) -> Box<[u8]> {{\n            let body = usize::try_from(count).expect(\"u32 fits usize\").checked_mul({item_length}).expect(\"schema ceiling\");\n            let header = {}_usize.checked_add(4).expect(\"header size\");\n            let total = if payload {{ let with_body = header.checked_add(body).expect(\"body size\"); with_body.checked_add({}_usize).expect(\"wire size\") }} else {{ header }};\n            let mut writer = skein_lib::Writer::new(total);\n            writer.put({}).expect(\"prefix room\");\n            writer.put(&count.to_be_bytes()).expect(\"length room\");\n            if payload {{\n", prefix.len(), suffix.len(), bytes_literal(&prefix)));
    if is_list {
        out.push_str(&format!(
            "                for _item in 0_u32..count {{ writer.put({}).expect(\"item room\"); }}\n",
            bytes_literal(&item)
        ));
    } else {
        out.push_str("                writer.put(&skein_lib::bytes::zeroed(body)).expect(\"body room\");\n");
    }
    out.push_str(&format!("                writer.put({}).expect(\"suffix room\");\n            }}\n            writer.finish()\n        }}\n        {record}::decode(&CEILINGS, &mut skein_lib::Reader::new(&wire({bound}, true))).expect(\"field at ceiling\");\n        let problem = {record}::decode(&CEILINGS, &mut skein_lib::Reader::new(&wire({over}, false))).expect_err(\"field past ceiling\");\n        assert_eq!((problem.path, problem.reason), (Path::{path}, skein_codec::Reason::Bound));\n    }}\n\n", bytes_literal(&suffix), record = record.name));
}

pub(crate) fn emit(schema: &Schema, out: &mut String) -> Vec<Golden> {
    let mut goldens = Vec::new();
    let has_bound_tests = schema.declarations.iter().any(|declaration| match declaration {
        Declaration::Record(record) => record.fields.iter().any(|field| match field.ty {
            Type::Bytes(bound) | Type::Text(bound) | Type::List(bound, _) => bound < u32::MAX,
            Type::U8
            | Type::U16
            | Type::U32
            | Type::U64
            | Type::Bool
            | Type::Duration
            | Type::Fixed(_)
            | Type::Option(_)
            | Type::Named(_) => false,
        }),
        Declaration::Enum(_) => false,
    });
    let mut imports = vec!["CEILINGS".to_string()];
    if has_bound_tests {
        imports.push("Path".to_string());
    }
    for declaration in &schema.declarations {
        match declaration {
            Declaration::Record(record) => {
                imports.push(record.name.clone());
                imports.push(format!("{}Parts", record.name));
            }
            Declaration::Enum(enumeration) => imports.push(enumeration.name.clone()),
        }
    }
    out.push_str(&format!("\n#[cfg(test)]\nmod golden_tests {{\n    use super::{{{}}};\n", imports.join(", ")));
    if schema.declarations.iter().any(|declaration| match declaration {
        Declaration::Record(record) => record.fields.iter().any(|field| uses_box(&field.ty)),
        Declaration::Enum(_) => false,
    }) || has_bound_tests
    {
        out.push_str("    use alloc::boxed::Box;\n");
    }
    out.push('\n');
    for declaration in &schema.declarations {
        match declaration {
            Declaration::Record(record) => {
                for (label, full) in [("smallest", false), ("full", true)] {
                    let mut bytes = Vec::new();
                    write_record(record, full, schema, &mut bytes);
                    let name = format!("record_{}_{}", emit_limits::snake(&record.name), label);
                    test_case(&name, &record.name, &record_expr(record, full, schema), &bytes, out);
                    goldens.push(Golden { name: format!("{name}.bin"), bytes });
                }
            }
            Declaration::Enum(enumeration) => {
                for variant in &enumeration.variants {
                    for (label, full) in [("smallest", false), ("full", true)] {
                        let mut bytes = Vec::new();
                        write_variant(enumeration, variant, full, schema, &mut bytes);
                        let name = format!("enum_{}_{}_{}", emit_limits::snake(&enumeration.name), variant.name, label);
                        test_case(
                            &name,
                            &enumeration.name,
                            &variant_expr(enumeration, variant, full, schema),
                            &bytes,
                            out,
                        );
                        goldens.push(Golden { name: format!("{name}.bin"), bytes });
                    }
                }
            }
        }
    }
    for declaration in &schema.declarations {
        if let Declaration::Record(record) = declaration {
            for field_index in 0..record.fields.len() {
                bound_test(record, field_index, schema, out);
            }
        }
    }
    out.push_str("}\n");
    goldens
}
