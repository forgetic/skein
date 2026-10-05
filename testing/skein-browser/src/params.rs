//! The CDP parameters the browser sends. One description is written twice:
//! measuring before allocation, then writing exactly the measured length.

use alloc::boxed::Box;

use skein_json::writer::{Encoder, Limits as JsonLimits};

#[derive(Debug)]
pub(crate) enum Params<'a> {
    Empty,
    Context(&'a [u8]),
    Target { url: &'a [u8], context: &'a [u8] },
    TargetId(&'a [u8]),
    AttachTarget(&'a [u8]),
    Url(&'a [u8]),
    Backend(u64),
    Query { root: u64, role: &'a [u8], name: &'a [u8] },
    Quads(u64),
    Point { x: i64, y: i64 },
    Mouse { kind: &'static [u8], x: i64, y: i64, pressed: bool },
    Text(&'a [u8]),
    Composition(&'a [u8]),
    Key { kind: &'static [u8], key: &'a [u8], code: &'a [u8], windows: u64, text: &'a [u8] },
    Viewport { width: u64, height: u64 },
    HistoryEntry(u64),
    Describe { backend: u64, depth: i64 },
}

impl Params<'_> {
    pub(crate) fn encode(&self, limit: u32) -> Option<Box<[u8]>> {
        let limits = JsonLimits { depth: 3, length: limit };
        let mut measure = Encoder::measure(&limits);
        self.write(&mut measure);
        let len = measure.measured().ok()?;
        let mut writer = Encoder::write(len, &limits);
        self.write(&mut writer);
        Some(writer.finish())
    }

    #[expect(clippy::too_many_lines, reason = "one match gives each CDP parameter shape its measured and written form")]
    fn write(&self, json: &mut Encoder) {
        json.object_start();
        match self {
            Params::Empty => {}
            Params::Context(context) => {
                json.key(b"browserContextId");
                json.string(context);
            }
            Params::Target { url, context } => {
                json.key(b"url");
                json.string(url);
                json.key(b"browserContextId");
                json.string(context);
            }
            Params::TargetId(target) => {
                json.key(b"targetId");
                json.string(target);
            }
            Params::AttachTarget(target) => {
                json.key(b"targetId");
                json.string(target);
                json.key(b"flatten");
                json.boolean(true);
            }
            Params::Url(url) => {
                json.key(b"url");
                json.string(url);
            }
            Params::Backend(backend) | Params::Quads(backend) => {
                json.key(b"backendNodeId");
                json.unsigned(*backend);
            }
            Params::Query { root, role, name } => {
                json.key(b"backendNodeId");
                json.unsigned(*root);
                json.key(b"role");
                json.string(role);
                json.key(b"accessibleName");
                json.string(name);
            }
            Params::Point { x, y } => {
                json.key(b"x");
                json.signed(*x);
                json.key(b"y");
                json.signed(*y);
                json.key(b"includeUserAgentShadowDOM");
                json.boolean(true);
            }
            Params::Mouse { kind, x, y, pressed } => {
                json.key(b"type");
                json.string(kind);
                json.key(b"x");
                json.signed(*x);
                json.key(b"y");
                json.signed(*y);
                if *pressed {
                    json.key(b"button");
                    json.string(b"left");
                    json.key(b"buttons");
                    json.unsigned(1);
                    json.key(b"clickCount");
                    json.unsigned(1);
                } else {
                    json.key(b"button");
                    json.string(b"left");
                    json.key(b"buttons");
                    json.unsigned(0);
                    json.key(b"clickCount");
                    json.unsigned(1);
                }
            }
            Params::Text(text) => {
                json.key(b"text");
                json.string(text);
            }
            Params::Composition(text) => {
                json.key(b"text");
                json.string(text);
                json.key(b"selectionStart");
                json.unsigned(u64::try_from(text.len()).expect("text length fits u64"));
                json.key(b"selectionEnd");
                json.unsigned(u64::try_from(text.len()).expect("text length fits u64"));
            }
            Params::Key { kind, key, code, windows, text } => {
                json.key(b"type");
                json.string(kind);
                json.key(b"key");
                json.string(key);
                json.key(b"code");
                json.string(code);
                json.key(b"windowsVirtualKeyCode");
                json.unsigned(*windows);
                json.key(b"text");
                json.string(text);
            }
            Params::Viewport { width, height } => {
                json.key(b"width");
                json.unsigned(*width);
                json.key(b"height");
                json.unsigned(*height);
                json.key(b"deviceScaleFactor");
                json.unsigned(1);
                json.key(b"mobile");
                json.boolean(false);
            }
            Params::HistoryEntry(entry) => {
                json.key(b"entryId");
                json.unsigned(*entry);
            }
            Params::Describe { backend, depth } => {
                json.key(b"backendNodeId");
                json.unsigned(*backend);
                json.key(b"depth");
                json.signed(*depth);
            }
        }
        json.object_end();
    }
}
