//! Regeneration check for codec.md, section 6.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use skein_codegen::{generate, parse};

    fn check(schema_path: &str, generated_path: &str) {
        let source = fs::read_to_string(schema_path).expect("schema readable");
        let schema = parse(&source).expect("schema valid");
        let output = generate(&schema);
        let write = std::env::var_os("SKEIN_CODEGEN_WRITE").is_some_and(|value| value == "1");
        if write {
            fs::write(generated_path, &output.rust).expect("write generated code");
        } else {
            let committed = fs::read_to_string(generated_path).expect("generated code committed");
            assert_eq!(
                committed, output.rust,
                "run SKEIN_CODEGEN_WRITE=1 cargo nextest run -p skein-codec-tests --test drift"
            );
        }
        let golden_directory = Path::new("golden/v1");
        for golden in output.goldens {
            let path = golden_directory.join(golden.name);
            if write {
                fs::create_dir_all(golden_directory).expect("create golden directory");
                fs::write(&path, golden.bytes).expect("write golden bytes");
            } else {
                let committed = fs::read(&path).expect("golden bytes committed");
                assert_eq!(
                    committed, golden.bytes,
                    "run SKEIN_CODEGEN_WRITE=1 cargo nextest run -p skein-codec-tests --test drift"
                );
            }
        }
    }

    #[test]
    fn sample_schema_code_and_goldens_have_no_drift() {
        check("schema/sample-v1.schema", "src/generated/v1.rs");
    }

    #[test]
    fn scalar_schema_code_and_goldens_have_no_drift() {
        check("schema/scalars-v1.schema", "src/scalars/v1.rs");
    }
}
