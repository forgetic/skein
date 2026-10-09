//! Codex event projections (llm.md, section 4.4): known items keep their
//! decoder's fields; unknown items keep bounded native replay. Response
//! output, request echoes and usage attribution are never retained.
use crate::filter::{INPUT, REASONING, STRINGS, each, field};
use skein_json::collector::{Filter, Keep, Tagged, Variant};

pub(crate) const EVENT: Filter = Filter {
    root: Keep::Into(&[
        field(b"type", Keep::Text(STRINGS)),
        field(b"output_index", Keep::Value),
        field(b"content_index", Keep::Value),
        field(b"summary_index", Keep::Value),
        field(b"item_id", Keep::Text(STRINGS)),
        field(b"delta", Keep::Text(STRINGS)),
        field(b"item", Keep::Tagged(&ITEM)),
        field(
            b"response",
            Keep::Into(&[
                field(b"status", Keep::Text(STRINGS)),
                field(
                    b"usage",
                    Keep::Into(&[
                        field(b"input_tokens", Keep::Value),
                        field(b"output_tokens", Keep::Value),
                        field(
                            b"input_tokens_details",
                            Keep::Into(&[
                                field(b"cached_tokens", Keep::Value),
                                field(b"cache_write_tokens", Keep::Value),
                            ]),
                        ),
                        field(b"output_tokens_details", Keep::Into(&[field(b"reasoning_tokens", Keep::Value)])),
                    ]),
                ),
                field(b"error", Keep::Into(ERROR)),
                field(b"incomplete_details", Keep::Into(&[field(b"reason", Keep::Text(STRINGS))])),
            ]),
        ),
        field(b"error", Keep::Into(ERROR)),
        field(b"code", Keep::Text(STRINGS)),
        field(b"message", Keep::Text(STRINGS)),
        field(b"resets_in_seconds", Keep::Value),
        field(b"resets_at", Keep::Value),
    ]),
};

const ERROR: &[skein_json::collector::Node] = &[
    field(b"type", Keep::Text(STRINGS)),
    field(b"code", Keep::Text(STRINGS)),
    field(b"message", Keep::Text(STRINGS)),
    field(b"resets_in_seconds", Keep::Value),
    field(b"resets_at", Keep::Value),
];

const ITEM: Tagged = Tagged {
    tag: b"type",
    known: &[
        Variant {
            value: b"message",
            children: &[
                field(b"id", Keep::Text(STRINGS)),
                field(b"status", Keep::Text(STRINGS)),
                field(b"phase", Keep::Text(STRINGS)),
                field(
                    b"content",
                    Keep::Into(&[each(Keep::Into(&[
                        field(b"type", Keep::Text(STRINGS)),
                        field(b"text", Keep::Text(STRINGS)),
                        field(b"refusal", Keep::Text(STRINGS)),
                    ]))]),
                ),
            ],
        },
        Variant {
            value: b"function_call",
            children: &[
                field(b"id", Keep::Text(STRINGS)),
                field(b"status", Keep::Text(STRINGS)),
                field(b"call_id", Keep::Text(STRINGS)),
                field(b"name", Keep::Text(STRINGS)),
                field(b"arguments", Keep::Text(INPUT)),
            ],
        },
        Variant {
            value: b"reasoning",
            children: &[
                field(b"id", Keep::Text(STRINGS)),
                field(
                    b"summary",
                    Keep::Into(&[each(Keep::Into(&[
                        field(b"type", Keep::Text(STRINGS)),
                        field(b"text", Keep::Text(STRINGS)),
                    ]))]),
                ),
                field(b"encrypted_content", Keep::Text(REASONING)),
            ],
        },
    ],
    unknown: REASONING,
};
