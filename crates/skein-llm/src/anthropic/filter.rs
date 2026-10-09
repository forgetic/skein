//! Messages event projections (llm.md, section 4.4). Known content and
//! delta kinds keep their decoder's fields. Thinking and unknown native
//! blocks retain their whole object, including provider extensions.
use crate::filter::{REASONING, STRINGS, field};
use skein_json::collector::{Filter, Keep, Node, Tagged, Variant};

pub(crate) const EVENT: Filter = Filter {
    root: Keep::Into(&[
        field(b"type", Keep::Text(STRINGS)),
        field(b"index", Keep::Value),
        field(
            b"message",
            Keep::Into(&[
                field(b"id", Keep::Text(STRINGS)),
                field(b"type", Keep::Text(STRINGS)),
                field(b"role", Keep::Text(STRINGS)),
                field(b"model", Keep::Text(STRINGS)),
                field(b"content", Keep::Value),
                field(b"usage", Keep::Into(USAGE)),
            ]),
        ),
        field(b"content_block", Keep::Tagged(&BLOCK)),
        field(b"delta", Keep::Tagged(&DELTA)),
        field(b"usage", Keep::Into(USAGE)),
        field(b"error", Keep::Into(&[field(b"type", Keep::Text(STRINGS)), field(b"message", Keep::Text(STRINGS))])),
    ]),
};

const USAGE: &[Node] = &[
    field(b"input_tokens", Keep::Value),
    field(b"output_tokens", Keep::Value),
    field(b"cache_read_input_tokens", Keep::Value),
    field(b"cache_creation_input_tokens", Keep::Value),
];

const BLOCK: Tagged = Tagged {
    tag: b"type",
    known: &[
        Variant { value: b"text", children: &[field(b"text", Keep::Text(STRINGS))] },
        Variant {
            value: b"tool_use",
            children: &[
                field(b"id", Keep::Text(STRINGS)),
                field(b"name", Keep::Text(STRINGS)),
                field(b"input", Keep::Value),
            ],
        },
    ],
    unknown: REASONING,
};

const DELTA: Tagged = Tagged {
    tag: b"type",
    known: &[
        Variant { value: b"text_delta", children: &[field(b"text", Keep::Text(STRINGS))] },
        Variant { value: b"input_json_delta", children: &[field(b"partial_json", Keep::Text(STRINGS))] },
        Variant { value: b"thinking_delta", children: &[field(b"thinking", Keep::Text(REASONING))] },
        Variant { value: b"signature_delta", children: &[field(b"signature", Keep::Text(REASONING))] },
    ],
    unknown: STRINGS,
};
