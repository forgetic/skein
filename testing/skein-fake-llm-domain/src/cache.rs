//! Bounded prefix digests written by admitted calls; no prompt bytes or timers.
//! Reads use the query's scope and marker frontier at injected time. Oldest
//! writes give way when full. Fake token lengths are content bytes divided by four. Contract: fake-llm.md, section 2.1.

use crate::api::{Caching, Mark, Part, Query, Role, ToolChoice};
use skein_lib::{Duration, Queue, Time};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Entry {
    scope: Option<[u8; 16]>,
    tokens: u64,
    digest: u64,
    written: Time,
}

/// The fixed-capacity cache and its immutable lifetime.
#[derive(Debug)]
pub(crate) struct Cache {
    entries: Queue<Entry>,
    lifetime: Duration,
}

impl Cache {
    pub(crate) fn new(capacity: u32, lifetime: Duration) -> Cache {
        Cache { entries: Queue::with_capacity(capacity), lifetime }
    }

    fn matches(&self, scope: Option<[u8; 16]>, prefix: Prefix, now: Time) -> u64 {
        let mut read = 0;
        for entry in &self.entries {
            if entry.scope == scope
                && entry.tokens == prefix.tokens()
                && entry.digest == prefix.digest
                && now.saturating_since(entry.written) < self.lifetime
            {
                read = read.max(entry.tokens);
            }
        }
        read
    }

    fn store(&mut self, scope: Option<[u8; 16]>, prefix: Prefix, now: Time) -> u64 {
        if self.entries.capacity() == 0 {
            return 0;
        }
        for _ in 0..self.entries.len() {
            let entry = self.entries.pop().expect("one entry per bounded rotation");
            if now.saturating_since(entry.written) < self.lifetime
                && !(entry.scope == scope && entry.digest == prefix.digest && entry.tokens == prefix.tokens())
            {
                self.entries.push(entry);
            }
        }
        if self.entries.room() == 0 {
            let _oldest = self.entries.pop().expect("full nonempty cache");
        }
        self.entries.push(Entry { scope, tokens: prefix.tokens(), digest: prefix.digest, written: now });
        prefix.tokens()
    }
}

pub(crate) fn worst_case(capacity: u32) -> Option<u64> {
    Queue::<Entry>::worst_case(capacity)
}

pub(crate) fn valid(query: &Query) -> bool {
    match &query.caching {
        Caching::Scope(_) | Caching::Unscoped => true,
        Caching::Marks(marks) => {
            if marks.len() > 4 {
                return false;
            }
            let mut previous = None;
            for mark in marks {
                if let Some(previous) = previous
                    && !ordered(previous, *mark)
                {
                    return false;
                }
                match mark {
                    Mark::System => {}
                    Mark::Part { message, part } => {
                        let Some(message) = query.messages.get(usize::try_from(*message).expect("u32 fits usize"))
                        else {
                            return false;
                        };
                        if message.parts.get(usize::try_from(*part).expect("u32 fits usize")).is_none() {
                            return false;
                        }
                    }
                }
                previous = Some(*mark);
            }
            true
        }
    }
}

fn ordered(previous: Mark, next: Mark) -> bool {
    match previous {
        Mark::System => true,
        Mark::Part { message, part } => match next {
            Mark::System => false,
            Mark::Part { message: next_message, part: next_part } => {
                message < next_message || (message == next_message && part <= next_part)
            }
        },
    }
}

pub(crate) fn tokens(query: &Query) -> u64 {
    let mut prefix = Prefix::system(query);
    prefix.choice(&query.choice);
    for message in &query.messages {
        prefix.role(message.role);
        for part in &message.parts {
            prefix.part(part);
        }
    }
    prefix.tokens()
}

pub(crate) fn read(cache: &Cache, query: &Query, now: Time) -> u64 {
    let mut prefix = Prefix::system(query);
    let mut read = cache.matches(scope(&query.caching), prefix, now);
    prefix.choice(&query.choice);
    for (message_index, message) in query.messages.iter().enumerate() {
        prefix.role(message.role);
        for (part_index, part) in message.parts.iter().enumerate() {
            prefix.part(part);
            if allowed(&query.caching, message_index, part_index) {
                read = read.max(cache.matches(scope(&query.caching), prefix, now));
            }
        }
    }
    match &query.caching {
        Caching::Marks(marks) if marks.is_empty() => 0,
        Caching::Scope(_) | Caching::Unscoped => read.max(cache.matches(scope(&query.caching), prefix, now)),
        Caching::Marks(_) => read,
    }
}

pub(crate) fn write(cache: &mut Cache, query: &Query, now: Time, read: u64) -> u64 {
    let mut prefix = Prefix::system(query);
    let mut written = 0;
    if marked(&query.caching, Mark::System) {
        written = cache.store(scope(&query.caching), prefix, now);
    }
    prefix.choice(&query.choice);
    for (message_index, message) in query.messages.iter().enumerate() {
        prefix.role(message.role);
        for (part_index, part) in message.parts.iter().enumerate() {
            prefix.part(part);
            let position = Mark::Part {
                message: u32::try_from(message_index).expect("admitted message count"),
                part: u32::try_from(part_index).expect("admitted part count"),
            };
            if marked(&query.caching, position) {
                written = written.max(cache.store(scope(&query.caching), prefix, now));
            }
        }
    }
    match &query.caching {
        Caching::Scope(_) | Caching::Unscoped => written = cache.store(scope(&query.caching), prefix, now),
        Caching::Marks(_) => {}
    }
    written.saturating_sub(read)
}

fn scope(caching: &Caching) -> Option<[u8; 16]> {
    match caching {
        Caching::Scope(key) => Some(*key),
        Caching::Unscoped | Caching::Marks(_) => None,
    }
}

fn marked(caching: &Caching, position: Mark) -> bool {
    match caching {
        Caching::Scope(_) | Caching::Unscoped => false,
        Caching::Marks(marks) => marks.contains(&position),
    }
}

fn allowed(caching: &Caching, message: usize, part: usize) -> bool {
    match caching {
        Caching::Scope(_) | Caching::Unscoped => true,
        Caching::Marks(marks) => match marks.last() {
            Some(Mark::Part { message: last_message, part: last_part }) => {
                message < usize::try_from(*last_message).expect("u32 fits usize")
                    || (message == usize::try_from(*last_message).expect("u32 fits usize")
                        && part <= usize::try_from(*last_part).expect("u32 fits usize"))
            }
            Some(Mark::System) | None => false,
        },
    }
}

#[derive(Clone, Copy)]
struct Prefix {
    digest: u64,
    bytes: u64,
}

impl Prefix {
    fn system(query: &Query) -> Prefix {
        let mut prefix = Prefix { digest: 0xcbf2_9ce4_8422_2325, bytes: 0 };
        prefix.field(&query.model);
        prefix.number(u64::try_from(query.tools.len()).expect("admitted tools"));
        for tool in &query.tools {
            prefix.text(&tool.name);
            prefix.text(&tool.description);
            prefix.text(&tool.parameters);
        }
        prefix.text(&query.system);
        prefix
    }

    fn tokens(self) -> u64 {
        self.bytes / 4
    }

    fn byte(&mut self, byte: u8) {
        self.digest ^= u64::from(byte);
        self.digest = self.digest.wrapping_mul(0x0000_0100_0000_01b3);
    }

    fn number(&mut self, number: u64) {
        for byte in number.to_be_bytes() {
            self.byte(byte);
        }
    }

    fn field(&mut self, bytes: &[u8]) {
        self.number(u64::try_from(bytes.len()).expect("admitted bytes"));
        for byte in bytes {
            self.byte(*byte);
        }
    }

    fn text(&mut self, bytes: &[u8]) {
        self.field(bytes);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).expect("admitted bytes"))
            .expect("query bytes admitted before caching");
    }

    fn choice(&mut self, choice: &ToolChoice) {
        match choice {
            ToolChoice::Auto => self.byte(0),
            ToolChoice::None => self.byte(1),
            ToolChoice::Only(names) => {
                self.byte(2);
                for name in names {
                    self.field(name);
                }
            }
        }
    }

    fn role(&mut self, role: Role) {
        self.byte(match role {
            Role::User => 0,
            Role::Assistant => 1,
        });
    }

    fn part(&mut self, part: &Part) {
        match part {
            Part::Text { text } => {
                self.byte(0);
                self.text(text);
            }
            Part::Opaque { bytes } => {
                self.byte(1);
                self.text(bytes);
            }
            Part::ToolCall { id, name, arguments } => {
                self.byte(2);
                self.text(id);
                self.text(name);
                self.text(arguments);
            }
            Part::ToolOutput { id, output, is_error } => {
                self.byte(3);
                self.byte(u8::from(*is_error));
                self.text(id);
                self.text(output);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::Message;
    use alloc::boxed::Box;

    fn query(scope: Option<[u8; 16]>) -> Query {
        Query {
            caching: match scope {
                Some(scope) => Caching::Scope(scope),
                None => Caching::Unscoped,
            },
            model: b"model".as_slice().into(),
            system: b"abcd".as_slice().into(),
            tools: Box::new([]),
            choice: ToolChoice::Auto,
            messages: Box::new([message(b"abcd")]),
            max_tokens: 10,
        }
    }

    fn message(text: &[u8]) -> Message {
        Message { role: Role::User, parts: Box::new([Part::Text { text: text.into() }]) }
    }

    #[test]
    fn actual_writes_choose_the_longest_scoped_prefix_and_content_mismatches_miss() {
        let mut cache = Cache::new(3, Duration::from_secs(300));
        let first = query(Some([1; 16]));
        assert_eq!(read(&cache, &first, Time::ZERO), 0);
        assert_eq!(write(&mut cache, &first, Time::ZERO, 0), 2);
        let mut second = first.clone();
        second.messages = Box::new([message(b"abcd"), message(b"abcdefgh")]);
        assert_eq!(read(&cache, &second, Time::ZERO), 2);
        assert_eq!(write(&mut cache, &second, Time::ZERO, 2), 2);
        let mut third = second.clone();
        third.messages = Box::new([message(b"abcd"), message(b"abcdefgh"), message(b"ijkl")]);
        assert_eq!(read(&cache, &third, Time::ZERO), 4);
        third.caching = Caching::Scope([2; 16]);
        assert_eq!(read(&cache, &third, Time::ZERO), 0);
        third = second.clone();
        third.messages[0].parts = Box::new([Part::Text { text: b"abce".as_slice().into() }]);
        assert_eq!(read(&cache, &third, Time::ZERO), 0);
        third = second;
        third.model = b"other".as_slice().into();
        assert_eq!(read(&cache, &third, Time::ZERO), 0);
    }

    #[test]
    fn expiry_is_at_the_lifetime_and_a_new_write_replaces_its_old_clock() {
        let mut cache = Cache::new(2, Duration::from_nanos(10));
        let query = query(Some([1; 16]));
        assert_eq!(write(&mut cache, &query, Time::ZERO, 0), 2);
        assert_eq!(read(&cache, &query, Time::from_nanos(9)), 2);
        assert_eq!(read(&cache, &query, Time::from_nanos(10)), 0);
        assert_eq!(write(&mut cache, &query, Time::from_nanos(10), 0), 2);
        assert_eq!(read(&cache, &query, Time::from_nanos(19)), 2);
        assert_eq!(read(&cache, &query, Time::from_nanos(20)), 0);
    }

    #[test]
    fn the_oldest_entry_gives_way_and_rewriting_a_prefix_does_not_fill_duplicate_slots() {
        let mut cache = Cache::new(2, Duration::from_secs(300));
        for scope in [1, 2, 3] {
            assert_eq!(write(&mut cache, &query(Some([scope; 16])), Time::from_nanos(u64::from(scope)), 0), 2);
        }
        assert_eq!(read(&cache, &query(Some([1; 16])), Time::from_nanos(3)), 0);
        for scope in [2, 3] {
            assert_eq!(read(&cache, &query(Some([scope; 16])), Time::from_nanos(3)), 2);
        }
        assert_eq!(write(&mut cache, &query(Some([3; 16])), Time::from_nanos(4), 2), 0);
        assert_eq!(cache.entries.len(), 2);
    }

    #[test]
    fn marked_reads_stop_at_the_frontier_and_choice_changes_keep_only_system_hits() {
        let mut cache = Cache::new(8, Duration::from_secs(300));
        let mut first = query(None);
        first.caching = Caching::Marks(Box::new([Mark::System, Mark::Part { message: 0, part: 0 }]));
        assert_eq!(write(&mut cache, &first, Time::ZERO, 0), 2);
        let mut second = first.clone();
        second.messages = Box::new([message(b"abcd"), message(b"abcdefgh")]);
        second.caching = Caching::Marks(Box::new([Mark::System, Mark::Part { message: 1, part: 0 }]));
        assert_eq!(read(&cache, &second, Time::ZERO), 2, "moved tail still reads the previous written tail");
        assert_eq!(write(&mut cache, &second, Time::ZERO, 2), 2);
        second.caching = first.caching.clone();
        assert_eq!(read(&cache, &second, Time::ZERO), 2, "a shorter frontier cannot read later content");
        second.choice = ToolChoice::None;
        assert_eq!(read(&cache, &second, Time::ZERO), 1, "tools/system remain cached across a choice change");
        second.caching = Caching::Marks(Box::new([]));
        assert_eq!(read(&cache, &second, Time::ZERO), 0);
        assert_eq!(write(&mut cache, &second, Time::ZERO, 0), 0);
    }

    #[test]
    fn positions_are_bounded_and_a_disabled_table_reports_no_write() {
        let mut query = query(None);
        query.caching = Caching::Marks(Box::new([Mark::Part { message: 1, part: 0 }]));
        assert!(!valid(&query));
        query.caching = Caching::Marks(Box::new([Mark::Part { message: 0, part: 0 }, Mark::System]));
        assert!(!valid(&query));
        query.caching = Caching::Marks(Box::new([Mark::System; 5]));
        assert!(!valid(&query));
        query.caching = Caching::Unscoped;
        let mut cache = Cache::new(0, Duration::from_secs(300));
        assert_eq!(write(&mut cache, &query, Time::ZERO, 0), 0);
        assert_eq!(read(&cache, &query, Time::ZERO), 0);
    }
}
