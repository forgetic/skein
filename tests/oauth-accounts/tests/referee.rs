use skein_oauth_accounts_world::world::Contracts;

#[test]
#[should_panic(expected = "nothing lent before its keep terminal")]
fn a_candidate_lent_before_kept_fails_the_referee() {
    let mut referee = Contracts::with_pending_grant();
    referee.keep(1);
    referee.granted(1);
}
#[test]
#[should_panic(expected = "each candidate keeps in flight has a key of its own")]
fn a_duplicate_keep_fails_the_referee() {
    let mut referee = Contracts::with_pending_grant();
    referee.keep(1);
    referee.keep(1);
}
#[test]
#[should_panic(expected = "held notices name a new generation")]
fn a_duplicate_grant_terminal_fails_the_referee() {
    let mut referee = Contracts::with_pending_grant();
    referee.granted(0);
    referee.granted(0);
}
#[test]
#[should_panic(expected = "one Closed")]
fn a_duplicate_closed_fails_the_referee() {
    let mut referee = Contracts::with_pending_grant();
    referee.granted(0);
    referee.closing();
    referee.closed();
    referee.closed();
}
#[test]
#[should_panic(expected = "Closed is the last owner event")]
fn an_event_after_closed_fails_the_referee() {
    let mut referee = Contracts::with_pending_grant();
    referee.granted(0);
    referee.closing();
    referee.closed();
    referee.keep(1);
}
#[test]
#[should_panic(expected = "signed in before kept")]
fn sign_in_success_before_keep_fails_the_referee() {
    let mut referee = skein_oauth_accounts_world::sign_in::Contracts::new();
    referee.signed_in(1);
}
#[test]
#[should_panic(expected = "a sign-in and keeper terminals ends once")]
fn a_duplicate_sign_in_terminal_fails_the_referee() {
    let mut referee = skein_oauth_accounts_world::sign_in::Contracts::new();
    referee.failed();
    referee.failed();
}
#[test]
#[should_panic(expected = "every sign-in and keeper terminals has ended")]
fn closed_with_a_pending_sign_in_fails_the_referee() {
    let mut referee = skein_oauth_accounts_world::sign_in::Contracts::new();
    referee.closed();
}
