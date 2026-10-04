//! main's own tests: its configuration from the arguments, and startup's
//! checks of its limits and memory. The loop runs in tests/echo, over the
//! simulator and the real ring.

use std::net::{Ipv4Addr, SocketAddr};
use std::vec;

use crate::{ADDRESS, Configuration, MEMORY, configure, limits, startup};

fn args(list: &[&str]) -> vec::IntoIter<String> {
    let mut args = Vec::new();
    for arg in list {
        args.push((*arg).to_owned());
    }
    args.into_iter()
}

#[test]
fn the_address_and_the_memory_come_from_the_arguments_or_their_defaults() {
    let defaults = configure(args(&[])).expect("no arguments is a configuration");
    assert_eq!(defaults.addr, ADDRESS.parse::<SocketAddr>().expect("an address"));
    assert_eq!(defaults.memory, MEMORY);
    let given = configure(args(&["127.0.0.1:0", "--memory", "1000"])).expect("both given");
    assert_eq!(given.addr, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    assert_eq!(given.memory, 1000);
}

#[test]
fn arguments_that_do_not_parse_are_refused() {
    assert!(configure(args(&["nowhere"])).is_err(), "not an address");
    assert!(configure(args(&["--memory"])).is_err(), "no bytes");
    assert!(configure(args(&["--memory", "lots"])).is_err(), "not a number");
    assert!(configure(args(&["127.0.0.1:1", "127.0.0.1:2"])).is_err(), "one address");
}

#[test]
fn the_default_limits_pass_startup_within_the_default_memory() {
    let configuration = configure(args(&[])).expect("defaults");
    let worst = startup(&configuration).expect("the defaults start");
    assert!(worst <= MEMORY, "within the memory: {worst}");
    assert_eq!(limits().check(), Ok(()));
}

#[test]
fn startup_refuses_a_worst_case_past_the_memory_and_limits_that_cannot_run() {
    let configuration = configure(args(&[])).expect("defaults");
    let small = Configuration { memory: 1 << 20, ..configuration };
    let refused = startup(&small).expect_err("a mebibyte is too little");
    assert!(refused.contains("worst case"), "says why: {refused}");
    let mut unusable = configuration;
    unusable.limits.io.intake = 16;
    let refused = startup(&unusable).expect_err("the intake cannot hold a line");
    assert!(refused.contains("Read"), "says why: {refused}");
}
