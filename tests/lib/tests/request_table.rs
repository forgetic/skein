//! The request table's store and owner promises against an independent ledger.

use skein_lib_tests::request_table::check;

#[test]
fn request_table_follows_the_obligation_model_and_replays() {
    assert_eq!(check(0xA5_31, 64), check(0xA5_31, 64));
}
