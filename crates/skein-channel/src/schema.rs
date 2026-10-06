//! Immutable, checked framing vocabulary (channel.md §§2,7). The consumer
//! supplies the union of all known channels/versions, including foreign kinds.
use alloc::boxed::Box;
use core::mem::size_of;
use skein_lib::Queue;

/// Local side of opening and kind direction (channel.md §2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// Sends Open; receives Accept.
    Initiator,
    /// Receives Open; sends Accept.
    Responder,
}

/// Frozen legal kind flow, also checked for newer/foreign kinds (§2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Direction {
    /// Initiator sends, responder receives.
    InitiatorToResponder,
    /// Responder sends, initiator receives.
    ResponderToInitiator,
    /// Both sides may send and receive.
    Both,
}

/// One globally known kind; common kinds 0..=17 are reserved (§2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct KnownKind {
    /// Unique service kind above 17, including newer and foreign kinds.
    pub kind: u16,
    /// Channel owning the kind; never guessed from a kind's bits.
    pub channel: u8,
    /// Legal direction across every version knowing this kind.
    pub direction: Direction,
}

/// One supported-version body bound in original source order (§2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct KindRule {
    /// Supported version owning this row.
    pub version: u16,
    /// A kind present in the all-known union.
    pub kind: u16,
    /// Maximum body bytes; checked before receive/encode allocation.
    pub body_bytes: u32,
}

/// Preinstalled first-record/read gates; service semantics remain above (§2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct First {
    /// Required first received service kind, automatically read when present.
    pub receive: Option<u16>,
    /// Required first outgoing service kind, admitted once.
    pub send: Option<u16>,
    /// Whether Ready starts reading absent a required receive kind.
    pub initial_read: bool,
    /// Whether Ping may arrive before the required first receive kind.
    pub ping_before_receive: bool,
}

/// Complete contiguous supported-version row (§§2,7).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct VersionRule {
    /// Positive version; rows are strictly contiguous ascending.
    pub version: u16,
    /// Exact maximum over all this version's kinds, at least common 512.
    pub unknown_body_bytes: u32,
    /// Gates frozen before selecting this version or emitting Ready.
    pub first: First,
}

/// Mechanical responder selection; authentication belongs above (§2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpeningMode {
    /// Emits Opening and waits for the parent's Accept or Refuse.
    AskParent,
    /// Selects highest intersection without interpreting credentials.
    AcceptHighest,
}

/// Frozen common opening codec and negotiation profile (§§1–2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OpeningProfile {
    /// Exact four-byte wire magic.
    pub magic: [u8; 4],
    /// Exact local channel byte.
    pub channel: u8,
    /// Actual service name maximum, at most 64 bytes.
    pub name_bytes: u32,
    /// Actual service secret maximum, at most 64 bytes.
    pub secret_bytes: u32,
    /// Immutable responder policy.
    pub mode: OpeningMode,
    /// Whether this channel permits Ping after negotiation.
    pub ping_allowed: bool,
}

/// Receiving/storage ceilings, checked before constructor allocations (§7).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    /// Positive exact body/skip delivery chunk; headers remain eight bytes.
    pub chunk_bytes: u32,
    /// Whole-cap native Room and logical admission byte cap C, at least 520.
    pub queued_bytes: u32,
    /// Positive fixed owning frame queue capacity Q, at least two.
    pub queued_frames: u32,
    /// Maximum supplied `KindRule` count.
    pub schema_rows: u32,
    /// Maximum supplied all-known kind count.
    pub known_kinds: u32,
    /// Maximum supplied contiguous version count.
    pub versions: u32,
    /// Maximum terms per version/direction, at most 32.
    pub terms: u32,
    /// Actual opaque refusal text maximum, at most 506.
    pub refuse_bytes: u32,
}

/// Owned immutable configuration; no retained consumer reference (§§2,7).
#[derive(Debug)]
pub struct Schema {
    pub(crate) role: Role,
    pub(crate) profile: OpeningProfile,
    pub(crate) limits: Limits,
    known: Box<[KnownKind]>,
    rules: Box<[KindRule]>,
    versions: Box<[VersionRule]>,
}

impl Schema {
    /// Checks all slices, arithmetic and per-frame fits before allocating
    /// exact owned copies. Rows preserve receive-Terms source order (§§2,7).
    #[must_use]
    pub fn new(
        role: Role,
        profile: OpeningProfile,
        limits: Limits,
        known: &[KnownKind],
        rules: &[KindRule],
        versions: &[VersionRule],
    ) -> Option<Schema> {
        validate_limits(profile, limits)?;
        check_count(known.len(), limits.known_kinds)?;
        check_count(rules.len(), limits.schema_rows)?;
        check_count(versions.len(), limits.versions)?;
        if versions.is_empty() {
            return None;
        }
        validate_known(known)?;
        validate_rules(known, rules, versions, limits)?;
        validate_versions(role, profile, known, rules, versions, limits)?;
        check_layout(known.len(), size_of::<KnownKind>())?;
        check_layout(rules.len(), size_of::<KindRule>())?;
        check_layout(versions.len(), size_of::<VersionRule>())?;
        check_layout(usize::try_from(limits.queued_frames).ok()?, size_of::<crate::Encoded>())?;
        Some(Schema {
            role,
            profile,
            limits,
            known: Box::from(known),
            rules: Box::from(rules),
            versions: Box::from(versions),
        })
    }

    /// Frozen receiving limits, also used for lower preflight (§7).
    #[must_use]
    pub const fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Frozen opening profile; credentials remain opaque (§2).
    #[must_use]
    pub const fn profile(&self) -> &OpeningProfile {
        &self.profile
    }

    /// Configured positive contiguous range, never a peer offer (§2).
    #[must_use]
    pub fn supported(&self) -> (u16, u16) {
        let lowest = self.versions.first().expect("validated nonempty versions").version;
        let highest = self.versions.last().expect("validated nonempty versions").version;
        (lowest, highest)
    }

    pub(crate) fn version(&self, version: u16) -> Option<VersionRule> {
        for row in &self.versions {
            if row.version == version {
                return Some(*row);
            }
        }
        None
    }

    pub(crate) fn known(&self, kind: u16) -> Option<KnownKind> {
        find_known(&self.known, kind)
    }

    pub(crate) fn rule(&self, version: u16, kind: u16) -> Option<KindRule> {
        for row in &self.rules {
            if row.version == version && row.kind == kind {
                return Some(*row);
            }
        }
        None
    }

    pub(crate) fn flows(&self, kind: u16, receiving: bool) -> bool {
        match self.known(kind) {
            Some(row) => row.channel == self.profile.channel && flow(self.role, row.direction, receiving),
            None => false,
        }
    }

    pub(crate) fn rules(&self) -> &[KindRule] {
        &self.rules
    }

    /// Exact shared owned heap plus delivery/common-decode/scratch bound (§7).
    /// Excludes inline Machine, caller-owned Encoded/decoded values and `IO`/`TLS`.
    /// Raw bodies moved to Event remain charged here until wrapper consumption.
    #[must_use]
    pub fn worst_case(&self) -> Option<u64> {
        let known = u64::try_from(self.known.len()).ok()?.checked_mul(u64::try_from(size_of::<KnownKind>()).ok()?)?;
        let rules = u64::try_from(self.rules.len()).ok()?.checked_mul(u64::try_from(size_of::<KindRule>()).ok()?)?;
        let versions =
            u64::try_from(self.versions.len()).ok()?.checked_mul(u64::try_from(size_of::<VersionRule>()).ok()?)?;
        let mut raw = 512;
        for row in &self.versions {
            raw = raw.max(row.unknown_body_bytes);
        }
        let schema = known.checked_add(rules)?.checked_add(versions)?;
        let queue = Queue::<crate::Encoded>::worst_case(self.limits.queued_frames)?;
        let common = u64::from(self.profile.name_bytes)
            .checked_add(u64::from(self.profile.secret_bytes))?
            .max(u64::from(self.limits.refuse_bytes));
        let events = Queue::<crate::Event>::worst_case(crate::MAX_UP)?;
        let lower = Queue::<crate::LowerRequest>::worst_case(crate::MAX_DOWN)?;
        schema
            .checked_add(queue)?
            .checked_add(u64::from(self.limits.queued_bytes))?
            .checked_add(u64::from(raw))?
            .checked_add(u64::from(self.limits.chunk_bytes.max(8)))?
            .checked_add(common)?
            .checked_add(events)?
            .checked_add(lower)
    }
}

fn check_count(length: usize, limit: u32) -> Option<()> {
    if u32::try_from(length).ok()? > limit { None } else { Some(()) }
}

fn validate_limits(profile: OpeningProfile, limits: Limits) -> Option<()> {
    if profile.name_bytes > 64
        || profile.secret_bytes > 64
        || limits.refuse_bytes > 506
        || limits.terms > 32
        || limits.chunk_bytes == 0
        || limits.queued_bytes < 520
        || limits.queued_frames < 2
    {
        return None;
    }
    usize::try_from(limits.chunk_bytes).ok()?;
    usize::try_from(limits.queued_bytes).ok()?;
    isize::try_from(limits.queued_bytes).ok()?;
    isize::try_from(limits.chunk_bytes).ok()?;
    usize::try_from(limits.queued_frames).ok()?;
    Some(())
}

fn find_known(known: &[KnownKind], kind: u16) -> Option<KnownKind> {
    for row in known {
        if row.kind == kind {
            return Some(*row);
        }
    }
    None
}

fn validate_known(known: &[KnownKind]) -> Option<()> {
    for (index, row) in known.iter().enumerate() {
        if row.kind <= 17 {
            return None;
        }
        for previous in known.get(..index)? {
            if previous.kind == row.kind {
                return None;
            }
        }
    }
    Some(())
}

fn validate_rules(known: &[KnownKind], rules: &[KindRule], versions: &[VersionRule], limits: Limits) -> Option<()> {
    for (index, row) in rules.iter().enumerate() {
        find_known(known, row.kind)?;
        let mut supported = false;
        for version in versions {
            if row.version == version.version {
                supported = true;
            }
        }
        if !supported || row.body_bytes.checked_add(8)? > limits.queued_bytes {
            return None;
        }
        for previous in rules.get(..index)? {
            if previous.version == row.version && previous.kind == row.kind {
                return None;
            }
        }
    }
    Some(())
}

fn validate_versions(
    role: Role,
    profile: OpeningProfile,
    known: &[KnownKind],
    rules: &[KindRule],
    versions: &[VersionRule],
    limits: Limits,
) -> Option<()> {
    let mut previous = None;
    for version in versions {
        if version.version == 0 {
            return None;
        }
        if let Some(previous) = previous
            && version.version != u16::checked_add(previous, 1)?
        {
            return None;
        }
        previous = Some(version.version);
        let mut maximum = 512;
        let mut sends = 0_u32;
        let mut receives = 0_u32;
        for row in rules {
            if row.version == version.version {
                maximum = maximum.max(row.body_bytes);
                let kind = find_known(known, row.kind)?;
                if kind.channel == profile.channel {
                    if flow(role, kind.direction, true) {
                        receives = receives.checked_add(1)?;
                    }
                    if flow(role, kind.direction, false) {
                        sends = sends.checked_add(1)?;
                    }
                }
            }
        }
        if maximum != version.unknown_body_bytes || sends > limits.terms || receives > limits.terms {
            return None;
        }
        validate_first(role, profile, known, rules, *version, true, version.first.receive)?;
        validate_first(role, profile, known, rules, *version, false, version.first.send)?;
        if version.first.ping_before_receive && !profile.ping_allowed {
            return None;
        }
    }
    Some(())
}

fn validate_first(
    role: Role,
    profile: OpeningProfile,
    known: &[KnownKind],
    rules: &[KindRule],
    version: VersionRule,
    receiving: bool,
    first: Option<u16>,
) -> Option<()> {
    if let Some(first) = first {
        let known = find_known(known, first)?;
        if known.channel != profile.channel || !flow(role, known.direction, receiving) {
            return None;
        }
        let mut found = false;
        for row in rules {
            if row.version == version.version && row.kind == first {
                found = true;
            }
        }
        if !found {
            return None;
        }
    }
    Some(())
}

fn flow(role: Role, direction: Direction, receiving: bool) -> bool {
    match direction {
        Direction::Both => true,
        Direction::InitiatorToResponder => match role {
            Role::Initiator => !receiving,
            Role::Responder => receiving,
        },
        Direction::ResponderToInitiator => match role {
            Role::Initiator => receiving,
            Role::Responder => !receiving,
        },
    }
}

fn check_layout(count: usize, item_bytes: usize) -> Option<()> {
    let bytes = count.checked_mul(item_bytes)?;
    isize::try_from(bytes).ok()?;
    Some(())
}
