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
        Some(mode @ ("fork-exit" | "fork-live" | "escape-exit" | "escape-zombie-exit")) => {
            let mut command = if matches!(mode, "escape-exit" | "escape-zombie-exit") {
                let mut command = Command::new("/usr/bin/setsid");
                command.arg("/bin/sh");
                command
            } else {
                Command::new("/bin/sh")
            };
            let script = if mode == "escape-zombie-exit" {
                "printf 'ready\n' >&2; exit 0"
            } else {
                "printf 'ready\n' >&2; exec sleep 60"
            };
            let mut child =
                command.args(["-c", script]).stderr(Stdio::piped()).spawn().expect("the fixture starts its descendant");
            let mut ready = String::new();
            BufReader::new(child.stderr.take().expect("readiness pipe"))
                .read_line(&mut ready)
                .expect("descendant readiness");
            assert_eq!(ready, "ready\n");
            if mode == "escape-zombie-exit" {
                // Leave the exited descendant unreaped, after it has left its
                // original group/session and the cgroup's live population.
                let status = format!("/proc/{}/status", child.id());
                while !std::fs::read_to_string(&status).expect("unreaped descendant status").contains("State:\tZ") {
                    std::hint::spin_loop();
                }
            }
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
        Some(mode @ ("supervise-term" | "supervise-ignore" | "supervise-grandchild")) => {
            let mut command = Command::new("/usr/bin/setsid");
            command.arg("/bin/sh");
            let script = match mode {
                "supervise-term" => {
                    "trap 'printf tree-term\\n >&2; exit 0' TERM; printf 'ready\\n'; while :; do :; done"
                }
                "supervise-ignore" => "trap '' TERM; printf 'ready\\n'; exec sleep 60",
                "supervise-grandchild" => {
                    "/usr/bin/setsid /bin/sh -c \"trap 'printf tree-term\\\\n >&2; exit 0' TERM; printf 'ready\\\\n'; while :; do :; done\" & printf 'grandchild:%s\\n' \"$!\" >&2; wait"
                }
                _ => unreachable!("the fixture selected its supervised mode"),
            };
            let mut child = command
                .args(["-c", script])
                .stdout(Stdio::piped())
                .spawn()
                .expect("the fixture starts its detached descendant");
            let mut ready = String::new();
            BufReader::new(child.stdout.take().expect("readiness pipe"))
                .read_line(&mut ready)
                .expect("descendant readiness");
            assert_eq!(ready, "ready\n");
            eprintln!("descendant:{}", child.id());
            loop {
                std::thread::park();
            }
        }
        other => panic!("unknown fixture program {other:?}"),
    }
}
