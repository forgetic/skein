use skein_oauth_accounts_world::world::{self, Story};
use skein_world::Memory;

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn a_held_grant_is_refreshed_at_its_lead_and_granted_again() {
    drop(world::run(23, Story::Lead, false, Memory::Checked));
}
#[test]
fn a_rotated_refresh_token_is_kept_before_it_is_lent() {
    drop(world::run(27, Story::Rotate, true, Memory::Checked));
}
#[test]
fn a_refresh_without_a_new_refresh_token_keeps_the_old_one() {
    drop(world::run(29, Story::Omit, false, Memory::Checked));
}
#[test]
fn a_rejected_token_refreshes_once_and_a_second_rejection_fails() {
    drop(world::run(31, Story::Rejected, false, Memory::Unchecked));
    drop(world::run(33, Story::Repeated, false, Memory::Unchecked));
}
#[test]
fn a_keep_that_fails_lends_nothing_new_and_the_old_generation_serves() {
    drop(world::run(35, Story::NotKept, false, Memory::Checked));
}
#[test]
fn a_grant_nobody_holds_is_not_refreshed() {
    drop(world::run(37, Story::Released, false, Memory::Checked));
}
#[test]
fn a_close_in_each_state_drains_and_says_closed_last() {
    for story in [Story::CloseConnecting, Story::CloseSending, Story::CloseKeeping] {
        drop(world::run(39, story, false, Memory::Checked));
    }
}
#[test]
fn an_abort_after_a_close_cancels_what_still_runs() {
    drop(world::run(41, Story::Abort, false, Memory::Checked));
}
#[test]
fn account_socket_and_owner_boundary_order_replay() {
    let first = world::run(43, Story::Rotate, true, Memory::Unchecked);
    let second = world::run(43, Story::Rotate, true, Memory::Unchecked);
    world::assert_replay(&first, &second);
}

#[test]
fn a_rate_limited_refresh_settles_its_connection_before_retrying() {
    drop(world::run(101, Story::Retry, true, Memory::Checked));
}
