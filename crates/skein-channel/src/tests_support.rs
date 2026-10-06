//! Literal test configuration, never a production alternate machine.
use crate::{Direction, First, KindRule, KnownKind, Limits, OpeningMode, OpeningProfile, Role, Schema, VersionRule};

pub(crate) fn schema() -> Schema {
    Schema::new(
        Role::Responder,
        OpeningProfile {
            magic: *b"tmpr",
            channel: 1,
            name_bytes: 64,
            secret_bytes: 64,
            mode: OpeningMode::AskParent,
            ping_allowed: true,
        },
        Limits {
            chunk_bytes: 7,
            queued_bytes: 520,
            queued_frames: 2,
            schema_rows: 1,
            known_kinds: 1,
            versions: 1,
            terms: 32,
            refuse_bytes: 506,
        },
        &[KnownKind { kind: 257, channel: 1, direction: Direction::InitiatorToResponder }],
        &[KindRule { version: 1, kind: 257, body_bytes: 1 }],
        &[VersionRule {
            version: 1,
            unknown_body_bytes: 512,
            first: First { receive: None, send: None, initial_read: true, ping_before_receive: false },
        }],
    )
    .expect("literal test schema")
}
