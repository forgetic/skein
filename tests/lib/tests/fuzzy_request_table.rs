//! Request-table promises under many seeds, bounded outputs and restarts.

use skein_lib_tests::request_table::check;

#[test]
fn many_request_sequences_follow_the_obligation_model() {
    let _trace = check(0xA5_31, 4_096);
}
