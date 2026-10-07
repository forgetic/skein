//! The journal against a plain queue model over seeded sequences.

use skein_lib_tests::journal::check;

#[test]
fn journal_follows_the_model() {
    check(0x3A_01, 300);
}
