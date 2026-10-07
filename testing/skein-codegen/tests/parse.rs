#[cfg(test)]
mod tests {
    use skein_codegen::{Declaration, Type, parse};

    #[test]
    fn all_types_and_declarations_parse() {
        let schema = parse("family sample 1\nrecord Earlier {}\nenum Choice { absent, present: Earlier }\nversioned record All {\n    a: u8\n    b: u16\n    c: u32\n    d: u64\n    e: bool\n    f: duration\n    g: fixed 7\n    h: bytes 8\n    i: text 9\n    j: list 10 option Choice\n}\n").expect("valid schema");
        assert_eq!(schema.family, "sample");
        assert_eq!(schema.version, 1);
        assert_eq!(schema.declarations.len(), 3);
        let Declaration::Record(record) = schema.declarations.get(2).expect("third declaration") else {
            unreachable!("the third declaration is a record");
        };
        assert!(record.versioned);
        assert_eq!(record.fields.len(), 10);
        assert_eq!(
            record.fields.get(9).expect("tenth field").ty,
            Type::List(10, Box::new(Type::Option(Box::new(Type::Named("Choice".into())))))
        );
    }

    fn refuses(source: &str, line: usize, fragment: &str) {
        let error = parse(source).expect_err("schema should be refused");
        assert_eq!(error.line, line, "{error}");
        assert!(error.message.contains(fragment), "{error}");
    }

    #[test]
    fn declaration_order_and_duplicate_names_are_checked() {
        refuses("family f 1\nrecord A { b: B }\nrecord B {}", 2, "before declaration");
        refuses("family f 1\nrecord A {}\nenum A { x }", 3, "duplicate type");
        refuses("family f 1\nrecord A { x: u8 x: u16 }", 2, "duplicate field");
        refuses("family f 1\nenum A { x, x }", 2, "duplicate variant");
    }

    #[test]
    fn variants_must_hold_a_prior_record_and_fit_in_a_tag() {
        refuses("family f 1\nenum A { x: B }\nrecord B {}", 2, "before declaration");
        refuses("family f 1\nenum A { x }\nenum B { x: A }", 3, "must hold one record");
        refuses("family f 1\nenum A { x: { a: u8 } }", 2, "invalid Rust name");
        let variants = (0_u16..257_u16).map(|number| format!("v{number}")).collect::<Vec<_>>().join(", ");
        refuses(&format!("family f 1\nenum A {{ {variants} }}"), 2, "256 variants");
    }

    #[test]
    fn huge_worst_cases_are_refused() {
        refuses("family f 1\nrecord A { b: list 4294967295 u64 }", 2, "exceeds u32");
        refuses("family f 1\nrecord A { b: bytes 4294967295 }", 2, "exceeds u32");
    }

    #[test]
    fn syntax_errors_name_their_lines() {
        refuses("family f 1\nrecord A {\n a: ?\n}", 3, "unexpected character");
        refuses("family f 1\nrecord A {\n a text 3\n}", 3, "expected :");
    }

    #[test]
    fn names_that_would_break_generated_rust_are_refused() {
        refuses("family f 1\nrecord type {}", 2, "invalid generated type name");
        refuses("family f 1\nrecord lowercase {}", 2, "invalid generated type name");
        refuses("family f 1\nrecord Box {}", 2, "generated type name Box");
        refuses("family f 1\nrecord A {}\nrecord AParts {}", 3, "generated type name AParts");
        refuses("family f 1\nrecord A { type: u8 }", 2, "invalid generated field name");
        refuses("family f 1\nrecord A { new: u8 }", 2, "invalid generated field name");
        refuses("family f 1\nrecord A { Title: u8 }", 2, "invalid generated field name");
        refuses("family f 1\nenum A { Read }", 2, "invalid generated variant name");
        refuses("family f 1\nenum A { foo_bar, foo__bar }", 2, "generated variant FooBar");
        refuses("family f 1\nrecord A { tail: u8 }", 2, "generated path ATail");
        refuses("family f 1\nversioned record A { version: u8 }", 2, "generated path AVersion");
        refuses("family f 1\nrecord A { b_item: bytes 1 b: list 1 bytes 1 }", 2, "generated limit name a_b_item");
        parse("family f 1\nrecord A { version: u8 }").expect("unversioned record has no version path");
    }
}
