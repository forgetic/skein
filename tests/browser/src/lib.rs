//! A machine world for `skein-browser`: the fake speaks complete CDP
//! documents over the same stream records as Chromium. Tests step both peers
//! without a process or clock sleep.

pub mod referee;
pub mod world;

#[cfg(test)]
mod tests;
