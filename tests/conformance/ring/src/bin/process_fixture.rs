//! A child executable for the ring's process conformance scenarios.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, ExitCode, Stdio};

#[expect(
    clippy::zombie_processes,
    reason = "the process-tree fixture deliberately leaves a descendant for the keeper to settle"
)]
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
        Some(mode @ ("fork-exit" | "fork-live" | "escape-exit")) => {
            let mut command = if mode == "escape-exit" {
                let mut command = Command::new("/usr/bin/setsid");
                command.arg("/bin/sh");
                command
            } else {
                Command::new("/bin/sh")
            };
            let mut child = command
                .args(["-c", "printf 'ready\n' >&2; exec sleep 60"])
                .stderr(Stdio::piped())
                .spawn()
                .expect("the fixture starts its descendant");
            let mut ready = String::new();
            BufReader::new(child.stderr.take().expect("readiness pipe"))
                .read_line(&mut ready)
                .expect("descendant readiness");
            assert_eq!(ready, "ready\n");
            eprintln!("descendant:{}", child.id());
            if mode == "fork-live" {
                loop {
                    std::thread::park();
                }
            }
            ExitCode::SUCCESS
        }
        Some("never") => loop {
            std::thread::park();
        },
        other => panic!("unknown fixture program {other:?}"),
    }
}
