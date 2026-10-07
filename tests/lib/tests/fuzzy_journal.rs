//! The journal against a plain queue model over many seeded sequences.

use skein_lib_tests::journal::check;

#[test]
fn many_journal_sequences_follow_the_model() {
    check(0x3A_01, 20_000);
}
