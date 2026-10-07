//! Bounded protocol tables checked before use (channel.md, section 4).
//! These tables keep wire numbers and size limits, but never body semantics.
use skein_lib::List;

/// Which peer owns one end of a channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// The peer that sends Open.
    Initiator,
    /// The peer that receives Open.
    Responder,
}

/// Which peer sends a kind in its version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// A kind sent by the initiator.
    FromInitiator,
    /// A kind sent by the responder.
    FromResponder,
}

/// One application kind and its maximum body in one version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Kind {
    pub kind: u16,
    pub direction: Direction,
    pub largest: u32,
}

/// One version and its bounded application kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub version: u16,
    pub kinds: List<Kind>,
}

/// An application's magic and contiguous supported versions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub magic: [u8; 4],
    pub versions: List<Version>,
}

/// Per-channel storage and wire limits supplied by its owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub chunk: u32,
    pub credential: u32,
    pub skip: u32,
    pub output_bytes: u32,
    pub output_frames: u32,
    pub kinds: u32,
}

/// Why a supplied schema or limit cannot form a bounded channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaError {
    /// At least one count or byte bound is unusable.
    InvalidLimit,
    /// There is no contiguous, ascending version table.
    Versions,
    /// An application kind is reserved, duplicated or exceeds a bound.
    Kind,
}

impl Schema {
    /// Checks the whole table before a machine keeps it (channel.md, section 4).
    pub fn check(&self, limits: &Limits) -> Result<(), SchemaError> {
        if limits.chunk == 0
            || limits.output_frames == 0
            || limits.output_bytes < 8
            || limits.kinds == 0
            || usize::try_from(limits.output_bytes).is_err()
            || isize::try_from(limits.output_bytes).is_err()
            || usize::try_from(limits.chunk).is_err()
            || usize::try_from(limits.credential).is_err()
            || usize::try_from(limits.skip).is_err()
        {
            return Err(SchemaError::InvalidLimit);
        }
        if self.versions.is_empty() {
            return Err(SchemaError::Versions);
        }
        let mut previous: Option<u16> = None;
        for version in &self.versions {
            if previous.is_some() && previous != version.version.checked_sub(1) {
                return Err(SchemaError::Versions);
            }
            previous = Some(version.version);
            if version.kinds.len() > limits.kinds {
                return Err(SchemaError::Kind);
            }
            for (index, kind) in version.kinds.iter().enumerate() {
                let length = kind.largest.checked_add(8).ok_or(SchemaError::Kind)?;
                if kind.kind < 0x0100 || length > limits.output_bytes {
                    return Err(SchemaError::Kind);
                }
                for earlier in version.kinds.iter().take(index) {
                    if earlier.kind == kind.kind {
                        return Err(SchemaError::Kind);
                    }
                }
            }
        }
        Ok(())
    }

    /// Inclusive bounds of the contiguous version offer.
    #[must_use]
    pub fn range(&self) -> Option<(u16, u16)> {
        Some((self.versions.get(0)?.version, self.versions.last()?.version))
    }

    /// Finds a version, or reports that this side does not speak it.
    #[must_use]
    #[expect(clippy::manual_find, reason = "the strict step subset has no closures")]
    pub fn version(&self, number: u16) -> Option<&Version> {
        for version in &self.versions {
            if version.version == number {
                return Some(version);
            }
        }
        None
    }
}

impl Version {
    /// Finds an application kind in this version.
    #[must_use]
    pub fn kind(&self, number: u16) -> Option<Kind> {
        for kind in &self.kinds {
            if kind.kind == number {
                return Some(*kind);
            }
        }
        None
    }
}
