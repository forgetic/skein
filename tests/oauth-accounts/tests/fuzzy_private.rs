use skein_lib::Rng;
use skein_oauth_accounts_world::private::{self, Story};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn seeded_private_file_failures_and_keeper_settlement_replay() {
    let stories = [
        Story::Refresh,
        Story::KeepFailed,
        Story::KeepStalled,
        Story::CloseKeeping,
        Story::AbortKeeping,
        Story::Missing,
        Story::Unreadable,
        Story::FileOwner,
        Story::FileMode,
        Story::FileLink,
        Story::FileLinks,
        Story::RootMode,
        Story::Load,
        Story::LoadFailed,
        Story::LoadStalled,
        Story::Fixed,
        Story::Conflict,
        Story::TooLarge,
        Story::CloseLoading,
        Story::AbortLoading,
    ];
    for seed in 0..32 {
        let mut random = Rng::new(seed);
        let story = stories[usize::try_from(random.below(20)).expect("story index")];
        let mut first = private::run(seed, story);
        let mut second = private::run(seed, story);
        assert_eq!(first.trace, second.trace);
        assert_eq!(private::facts(&first.procs), private::facts(&second.procs));
        first.machine.finish();
        second.machine.finish();
    }
}
