//! Command line entry point for codec.md, section 6.

use std::path::Path;

use skein_codegen::{generate, parse};

fn main() -> Result<(), Box<dyn core::error::Error>> {
    let mut args = std::env::args();
    let _command = args.next();
    let schema_path = args.next().ok_or("usage: skein-codegen <schema> <out-dir>")?;
    let out_dir = args.next().ok_or("usage: skein-codegen <schema> <out-dir>")?;
    if args.next().is_some() {
        return Err("usage: skein-codegen <schema> <out-dir>".into());
    }
    let source = std::fs::read_to_string(&schema_path)?;
    let schema = parse(&source)?;
    std::fs::create_dir_all(&out_dir)?;
    let output = generate(&schema);
    let path = Path::new(&out_dir).join(format!("v{}.rs", schema.version));
    std::fs::write(path, output.rust)?;
    let schema_directory = Path::new(&schema_path).parent().ok_or("schema has no parent directory")?;
    let crate_directory = schema_directory.parent().ok_or("schema directory has no parent")?;
    let golden_directory = crate_directory.join("golden").join(format!("v{}", schema.version));
    std::fs::create_dir_all(&golden_directory)?;
    for golden in output.goldens {
        std::fs::write(golden_directory.join(golden.name), golden.bytes)?;
    }
    Ok(())
}
