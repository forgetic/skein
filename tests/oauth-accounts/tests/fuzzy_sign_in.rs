use skein_oauth_accounts_world::sign_in::{self, Story};
#[test]
fn browser_arrival_wrong_redirects_and_close_replay_under_short_operations() {
    for seed in 0..32 {
        let story = match seed % 8 {
            0 => Story::WrongPath,
            1 => Story::WrongHost,
            2 => Story::LongHead,
            3 => Story::CloseWaiting,
            4 => Story::Cancel,
            5 => Story::Abort,
            6 => Story::Confidential,
            _ => Story::Public,
        };
        let first = sign_in::run(seed, story, true, false, skein_world::Memory::Unchecked);
        let second = sign_in::run(seed, story, true, false, skein_world::Memory::Unchecked);
        sign_in::assert_replay(&first, &second);
    }
}
