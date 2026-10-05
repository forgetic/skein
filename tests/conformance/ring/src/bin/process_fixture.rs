//! A child executable for the ring's process conformance scenarios.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args();
    let _program = args.next();
    match args.next().as_deref() {
        Some("echo") => {
            let input = args.next().expect("input descriptor");
            let output = args.next().expect("output descriptor");
            let mut reader = File::open(format!("/proc/self/fd/{input}")).expect("child input pipe");
            let mut writer =
                OpenOptions::new().write(true).open(format!("/proc/self/fd/{output}")).expect("child output pipe");
            let mut bytes = [0_u8; 4096];
            loop {
                let n = reader.read(&mut bytes).expect("read child input");
                if n == 0 {
                    break;
                }
                writer.write_all(&bytes[..n]).expect("write child output");
            }
            ExitCode::SUCCESS
        }
        Some("exit") => {
            let code = args.next().expect("exit code").parse::<u8>().expect("numeric exit code");
            ExitCode::from(code)
        }
        Some("never") => loop {
            std::thread::park();
        },
        other => panic!("unknown fixture program {other:?}"),
    }
}
