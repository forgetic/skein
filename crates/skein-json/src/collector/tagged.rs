//! Provisional tagged projections (json.md, section 5.1). Each static node
//! owns a whole candidate and counts for its variants. Shared fields occupy
//! one range in the collector's compact projection; selection compacts it.
use super::{AmbiguousFilter, Error, Filter, Keep, Key, Limits, Tagged, field};
use crate::Token;
use crate::document::{self, Builder};
use skein_lib::{List, Stack};

#[derive(Debug)]
#[expect(clippy::partial_pub_fields, reason = "the parent routes through state; candidate buffers remain private")]
pub(super) struct Pending {
    pub filter: &'static Tagged,
    pub active: bool,
    pub depth: u32,
    pub start: u32,
    pub text_start: u32,
    pub copy_error: Option<Error>,
    pub deferred_error: Option<Error>,
    copy: Builder,
    cap: u32,
    tokens: u32,
    text: u32,
    candidates: List<Candidate>,
    selected: Selection,
    current: Option<&'static [u8]>,
    wire_start: u32,
    tag_seen: bool,
    fields: List<Field>,
    field_start: u32,
}

#[derive(Clone, Copy, Debug)]
struct Field {
    key: &'static [u8],
    start: u32,
    end: u32,
}

#[derive(Clone, Copy, Debug)]
struct Candidate {
    tokens: u32,
    text: u32,
    skipped: u64,
    error: Option<Error>,
}

#[derive(Clone, Copy, Debug)]
enum Selection {
    Pending,
    Known(u32),
    Unknown,
}

impl Pending {
    fn new(filter: &'static Tagged, limits: &Limits, caps: &[u32]) -> Pending {
        let cap = *caps.get(usize::from(filter.unknown.index())).expect("unknown cap supplied");
        let mut candidates = List::with_capacity(u32::try_from(filter.known.len()).expect("static variants fit u32"));
        for _ in filter.known {
            candidates.push(Candidate { tokens: 0, text: 0, skipped: 0, error: None }).expect("variant capacity");
        }
        Pending {
            filter,
            active: false,
            depth: 0,
            start: 0,
            text_start: 0,
            copy_error: None,
            deferred_error: None,
            copy: Builder::new(document::Limits { tokens: limits.tokens, text: cap }),
            cap,
            tokens: limits.tokens,
            text: limits.text,
            candidates,
            selected: Selection::Pending,
            current: None,
            wire_start: 0,
            tag_seen: false,
            fields: List::with_capacity(
                limits
                    .tokens
                    .checked_mul(u32::try_from(filter.known.len()).expect("variants").max(1))
                    .expect("projection capacity"),
            ),
            field_start: 0,
        }
    }

    pub fn reset(&mut self) {
        self.active = false;
        self.copy.clear();
        self.copy_error = None;
        self.deferred_error = None;
        self.selected = Selection::Pending;
        self.current = None;
        self.tag_seen = false;
        self.fields.clear();
        for index in 0..self.candidates.len() {
            *self.candidates.get_mut(index).expect("candidate") =
                Candidate { tokens: 0, text: 0, skipped: 0, error: None };
        }
    }

    pub fn begin(&mut self, depth: u32, start: u32, text_start: u32, tokens: u32, text: u32) {
        self.reset();
        self.active = true;
        self.depth = depth;
        self.start = start;
        self.text_start = text_start;
        self.tokens = tokens;
        self.text = text;
    }

    pub fn copy_room(&self) -> u32 {
        self.cap.checked_sub(self.copy.text_len()).expect("copy is within cap")
    }

    pub fn capture(&mut self, token: &Token) {
        if self.copy_error.is_some() {
            return;
        }
        match self.copy.push(token) {
            Ok(()) => {}
            Err(document::Error::TooManyTokens) => self.copy_error = Some(Error::TooManyTokens),
            Err(document::Error::TooMuchText) => {
                self.copy_error = Some(Error::TooMuchText { cap: Some(self.filter.unknown) });
            }
            Err(document::Error::Range) => unreachable!("builder offsets"),
        }
    }

    pub fn key(&mut self, key: &[u8], wire: u32, start: u32) -> Result<(), Error> {
        self.current = None;
        self.wire_start = wire;
        self.field_start = start;
        for field in &self.fields {
            if field.key == key {
                return Err(Error::Duplicate);
            }
        }
        if key == self.filter.tag {
            if self.tag_seen {
                return Err(Error::Duplicate);
            }
            self.tag_seen = true;
            self.current = Some(self.filter.tag);
        } else {
            for variant in self.filter.known {
                for node in variant.children {
                    match node.key {
                        Key::Field(name) if name == key => {
                            self.current = Some(name);
                            return Ok(());
                        }
                        Key::Field(_) | Key::Each => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn interested(&self, index: u32) -> bool {
        match self.selected {
            Selection::Known(selected) if selected != index => return false,
            Selection::Unknown => return false,
            Selection::Known(_) | Selection::Pending => {}
        }
        match self.current {
            None => false,
            Some(key) if key == self.filter.tag => true,
            Some(key) => {
                field(self.filter.known.get(usize::try_from(index).expect("index")).expect("variant").children, key)
                    .is_some()
            }
        }
    }

    pub fn keep(&self, key: &[u8]) -> Option<Keep> {
        for (index, variant) in self.filter.known.iter().enumerate() {
            let index = u32::try_from(index).expect("variants bounded");
            if self.interested(index)
                && self.candidates.get(index).expect("variant").error.is_none()
                && let Some(keep) = field(variant.children, key)
            {
                return Some(keep);
            }
        }
        None
    }

    pub fn selected_error(&self) -> Option<Error> {
        match self.selected {
            Selection::Known(index) => self.candidates.get(index).expect("candidate").error,
            Selection::Pending | Selection::Unknown => None,
        }
    }

    pub fn selected_skipped(&self) -> Option<u64> {
        match self.selected {
            Selection::Known(index) => Some(self.candidates.get(index).expect("candidate").skipped),
            Selection::Pending | Selection::Unknown => None,
        }
    }

    pub fn remaining(&self) -> (u32, u32) {
        let mut tokens = 0;
        let mut text = 0;
        for index in 0..self.candidates.len() {
            if self.interested(index) {
                let candidate = self.candidates.get(index).expect("candidate");
                if candidate.error.is_none() {
                    tokens = tokens.max(self.tokens.checked_sub(candidate.tokens).expect("admitted tokens"));
                    text = text.max(self.text.checked_sub(candidate.text).expect("admitted text"));
                }
            }
        }
        (tokens, text)
    }

    pub fn retention_room(&self) -> u32 {
        let mut room = 0;
        for index in 0..self.candidates.len() {
            if self.current.is_none() || self.interested(index) {
                let candidate = self.candidates.get(index).expect("candidate");
                if candidate.error.is_none() {
                    room = room.max(self.text.checked_sub(candidate.text).expect("admitted text"));
                }
            }
        }
        room
    }

    pub fn charge(&mut self, tokens: u32, text: u32) -> bool {
        let mut stored = false;
        for index in 0..self.candidates.len() {
            // No current field means the object's start or end.
            let interested = self.current.is_none() || self.interested(index);
            if !interested {
                continue;
            }
            let candidate = self.candidates.get_mut(index).expect("candidate");
            if candidate.error.is_some() {
                continue;
            }
            let count = candidate.tokens.checked_add(tokens);
            match count {
                Some(count) if count <= self.tokens => candidate.tokens = count,
                Some(_) | None => {
                    candidate.error = Some(Error::TooManyTokens);
                    continue;
                }
            }
            let count = candidate.text.checked_add(text);
            match count {
                Some(count) if count <= self.text => candidate.text = count,
                Some(_) | None => {
                    candidate.error = Some(Error::TooMuchText { cap: None });
                    continue;
                }
            }
            stored = true;
        }
        stored
    }

    pub fn charge_result(&mut self, tokens: u32, text: u32, skipped: u64, limit: u64) -> bool {
        let stored = self.charge(tokens, text);
        for index in 0..self.candidates.len() {
            if self.interested(index) {
                let candidate = self.candidates.get_mut(index).expect("candidate");
                candidate.skipped = candidate.skipped.checked_add(skipped).expect("bounded wire bytes");
                if candidate.error.is_none() && candidate.skipped > limit {
                    candidate.error = Some(Error::SkippedTooLong);
                }
            }
        }
        stored
    }

    pub fn skip_value(&mut self, bytes: u64, limit: u64) {
        for index in 0..self.candidates.len() {
            let applies = match self.selected {
                Selection::Pending => true,
                Selection::Known(selected) => selected == index,
                Selection::Unknown => false,
            };
            if applies {
                let candidate = self.candidates.get_mut(index).expect("candidate");
                candidate.skipped = candidate.skipped.checked_add(bytes).expect("bounded wire bytes");
                if candidate.error.is_none() && candidate.skipped > limit {
                    candidate.error = Some(Error::SkippedTooLong);
                }
            }
        }
    }

    pub fn strict_long(&mut self, length: u64, strings: u32) {
        for index in 0..self.candidates.len() {
            if self.interested(index) {
                let candidate = self.candidates.get_mut(index).expect("candidate");
                if candidate.error.is_none() {
                    let room = self.text.checked_sub(candidate.text).expect("admitted text");
                    candidate.error = Some(if candidate.tokens == self.tokens {
                        Error::TooManyTokens
                    } else if strings <= room && length > u64::from(strings) {
                        Error::Tokenizer(super::json::Error::StringTooLong)
                    } else {
                        Error::TooMuchText { cap: None }
                    });
                }
            }
        }
    }

    pub fn fail_field(&mut self, error: Error) {
        for index in 0..self.candidates.len() {
            if self.interested(index) {
                let candidate = self.candidates.get_mut(index).expect("candidate");
                if candidate.error.is_none() {
                    candidate.error = Some(error);
                }
            }
        }
    }

    pub fn skip_finished(&mut self) {
        self.current = None;
    }

    pub fn begin_value(&mut self, wire: u32) {
        self.wire_start = wire;
    }

    pub fn finish_field(&mut self, wire: u32, end: u32) {
        match self.current {
            Some(key) if end > self.field_start => {
                self.fields
                    .push(Field { key, start: self.field_start, end })
                    .expect("each range contains a stored key");
            }
            Some(_) | None => {}
        }
        for index in 0..self.candidates.len() {
            if !self.interested(index) {
                let candidate = self.candidates.get_mut(index).expect("candidate");
                candidate.skipped = candidate
                    .skipped
                    .checked_add(u64::from(wire.checked_sub(self.wire_start).expect("wire advances")))
                    .expect("input length is bounded by u32");
            }
        }
        self.current = None;
    }

    pub fn select(&mut self, value: &[u8]) -> Result<(), Error> {
        self.selected = Selection::Unknown;
        for (index, variant) in self.filter.known.iter().enumerate() {
            if variant.value == value {
                let index = u32::try_from(index).expect("bounded variants");
                self.selected = Selection::Known(index);
                if let Some(error) = self.candidates.get(index).expect("candidate").error {
                    return Err(error);
                }
                // The whole candidate can no longer win.
                self.copy.clear();
                self.copy_error = Some(Error::TooMuchText { cap: Some(self.filter.unknown) });
                break;
            }
        }
        Ok(())
    }

    pub fn finish(&mut self, builder: &mut Builder) -> Result<(u32, u32, u64), Error> {
        if let Some(error) = self.deferred_error {
            return Err(error);
        }
        match self.selected {
            Selection::Known(index) => {
                let candidate = self.candidates.get(index).expect("selected variant");
                if let Some(error) = candidate.error {
                    return Err(error);
                }
                let children = self.filter.known.get(usize::try_from(index).expect("index")).expect("variant").children;
                let mut destination = self.start;
                let mut text = self.text_start;
                (destination, text) = builder.copy_within(
                    self.start,
                    self.start.checked_add(1).expect("object start"),
                    destination,
                    text,
                );
                for range in &self.fields {
                    if range.key == self.filter.tag || field(children, range.key).is_some() {
                        (destination, text) = builder.copy_within(range.start, range.end, destination, text);
                    }
                }
                let end = builder.len();
                (destination, text) =
                    builder.copy_within(end.checked_sub(1).expect("object end"), end, destination, text);
                builder.truncate(destination, text);
                Ok((candidate.tokens, candidate.text, candidate.skipped))
            }
            Selection::Pending | Selection::Unknown => {
                if let Some(error) = self.copy_error {
                    return Err(error);
                }
                if self.copy.len() > self.tokens {
                    return Err(Error::TooManyTokens);
                }
                if self.copy.text_len() > self.text {
                    return Err(Error::TooMuchText { cap: None });
                }
                builder.truncate(self.start, self.text_start);
                for index in 0..self.copy.len() {
                    let record = self.copy.token(index).expect("copy record");
                    builder.push_compact(record, self.copy.text(record)).expect("whole copy fits projection capacity");
                }
                Ok((self.copy.len(), self.copy.text_len(), 0))
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Cursor {
    keep: Keep,
    child: usize,
}

// Static filter descriptions have no peer-controlled size. This bound keeps
// startup traversal finite; every level also uses the configured depth.
const FILTER_STEPS: u32 = 65_536;

fn next_child(cursor: &mut Cursor) -> Option<Keep> {
    match cursor.keep {
        Keep::Value | Keep::Text(_) => None,
        Keep::Into(nodes) => {
            let node = nodes.get(cursor.child)?;
            cursor.child = cursor.child.checked_add(1).expect("static index");
            Some(node.keep)
        }
        Keep::Tagged(tag) => {
            let mut index = cursor.child;
            for variant in tag.known {
                if index < variant.children.len() {
                    cursor.child = cursor.child.checked_add(1).expect("static index");
                    return Some(variant.children.get(index).expect("child").keep);
                }
                index = index.checked_sub(variant.children.len()).expect("remaining index");
            }
            None
        }
    }
}

fn validate(tag: &'static Tagged) -> Result<(), AmbiguousFilter> {
    for left in tag.known {
        for right in tag.known {
            for node in left.children {
                match node.key {
                    Key::Field(name) => match field(right.children, name) {
                        Some(keep) if keep != node.keep => return Err(AmbiguousFilter { field: name }),
                        Some(_) | None => {}
                    },
                    Key::Each => {}
                }
            }
        }
    }
    Ok(())
}

pub(super) fn describe(filter: Filter, limits: &Limits) -> Result<List<&'static Tagged>, AmbiguousFilter> {
    let mut count = 0_u32;
    for pass in 0_u32..2 {
        let mut tags: List<&'static Tagged> = List::with_capacity(if pass == 0 { 0 } else { count });
        let mut walk = Stack::with_capacity(limits.tokenizer.depth.checked_add(1).expect("filter depth"));
        walk.push(Cursor { keep: filter.root, child: 0 }).expect("root frame");
        for _ in 0..FILTER_STEPS {
            let keep = walk.top().expect("unfinished filter").keep;
            if walk.top().expect("frame").child == 0 {
                match keep {
                    Keep::Tagged(tag) => {
                        validate(tag)?;
                        if pass == 0 {
                            count = count.checked_add(1).expect("static tags");
                        } else {
                            let mut exists = false;
                            for existing in &tags {
                                if *existing == tag {
                                    exists = true;
                                    break;
                                }
                            }
                            if !exists {
                                tags.push(tag).expect("counted nodes");
                            }
                        }
                    }
                    Keep::Value | Keep::Text(_) | Keep::Into(_) => {}
                }
            }
            match next_child(walk.top_mut().expect("frame")) {
                Some(child) => walk.push(Cursor { keep: child, child: 0 }).expect("static filter fits tokenizer depth"),
                None => {
                    walk.pop().expect("completed frame");
                    if walk.is_empty() {
                        if pass == 1 {
                            return Ok(tags);
                        }
                        break;
                    }
                }
            }
        }
        assert!(walk.is_empty(), "static filter traversal fits its bound");
    }
    unreachable!("second pass returns the static nodes")
}

pub(super) fn build(filter: Filter, limits: &Limits, caps: &[u32]) -> Result<List<Pending>, AmbiguousFilter> {
    let plan = describe(filter, limits)?;
    let mut tags = List::with_capacity(plan.len());
    for tag in &plan {
        tags.push(Pending::new(tag, limits, caps)).expect("static node capacity");
    }
    Ok(tags)
}

pub(super) fn projection_limits(limits: &Limits, tags: &List<&'static Tagged>) -> Option<document::Limits> {
    let mut copies = 1_u32;
    for tag in tags {
        let variants = u32::try_from(tag.known.len()).ok()?;
        copies = copies.checked_add(match variants {
            0 => 0,
            _ => variants.checked_sub(1)?,
        })?;
    }
    Some(document::Limits { tokens: limits.tokens.checked_mul(copies)?, text: limits.text.checked_mul(copies)? })
}

pub(super) fn candidate_bytes(limits: &Limits, caps: &[u32], tags: &List<&'static Tagged>) -> Option<u64> {
    let mut bytes = List::<Pending>::worst_case(tags.len())?;
    for tag in tags {
        let variants = u32::try_from(tag.known.len()).ok()?;
        let fields = limits.tokens.checked_mul(variants.max(1))?;
        let cap = *caps.get(usize::from(tag.unknown.index()))?;
        bytes = bytes
            .checked_add(List::<Candidate>::worst_case(variants)?)?
            .checked_add(List::<Field>::worst_case(fields)?)?
            .checked_add(document::worst_case(&document::Limits { tokens: limits.tokens, text: cap })?)?;
    }
    Some(bytes)
}
