use skein_oauth_accounts_world::world::{self, Story};

#[test]
fn refresh_keeping_rejection_release_and_teardown_cross_real_http_streams() {
    for story in [
        Story::Lead,
        Story::Rotate,
        Story::Omit,
        Story::NotKept,
        Story::Rejected,
        Story::Repeated,
        Story::Released,
        Story::CloseConnecting,
        Story::CloseSending,
        Story::CloseKeeping,
        Story::Abort,
    ] {
        let (facts, _) = world::protocol(83, story);
        assert_eq!(facts.last(), Some(&world::Fact::Closed));
        match story {
            Story::Abort | Story::Repeated => assert!(facts.iter().any(|fact| matches!(fact, world::Fact::Failed(_)))),
            Story::Released | Story::NotKept => assert!(facts.contains(&world::Fact::Granted(0))),
            Story::Retry
            | Story::Lead
            | Story::Rotate
            | Story::Omit
            | Story::Rejected
            | Story::CloseConnecting
            | Story::CloseSending
            | Story::CloseKeeping => assert!(facts.contains(&world::Fact::Granted(1))),
        }
    }
}

#[test]
fn stream_fragments_and_owner_terminals_replay() {
    assert_eq!(world::protocol(97, Story::Rotate), world::protocol(97, Story::Rotate));
}
