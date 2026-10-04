//! What the start lines of both sides name (RFC 9112, 3 and 4): a
//! request's method, and a message's version.

/// A request method (RFC 9110, 9.3): those an API's client sends, and so
/// those the server knows; another is not implemented (http.md, 5.2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Method {
    Get,
    /// A response to it has no body, whatever its head says.
    Head,
    Post,
    Put,
    Patch,
    Delete,
    Options,
}

impl Method {
    /// The method's name, as the request line spells it.
    #[must_use]
    pub fn as_bytes(self) -> &'static [u8] {
        match self {
            Method::Get => b"GET",
            Method::Head => b"HEAD",
            Method::Post => b"POST",
            Method::Put => b"PUT",
            Method::Patch => b"PATCH",
            Method::Delete => b"DELETE",
            Method::Options => b"OPTIONS",
        }
    }

    /// The method a request line names, if it is one of these: methods are
    /// compared with regard to case (RFC 9110, 9.1).
    #[must_use]
    pub fn from_bytes(name: &[u8]) -> Option<Method> {
        match name {
            b"GET" => Some(Method::Get),
            b"HEAD" => Some(Method::Head),
            b"POST" => Some(Method::Post),
            b"PUT" => Some(Method::Put),
            b"PATCH" => Some(Method::Patch),
            b"DELETE" => Some(Method::Delete),
            b"OPTIONS" => Some(Method::Options),
            _ => None,
        }
    }

    /// Whether the method gives a body a meaning, so that a call without
    /// one says `Content-Length: 0` (RFC 9110, 8.6).
    pub(crate) fn expects_body(self) -> bool {
        match self {
            Method::Post | Method::Put | Method::Patch => true,
            Method::Get | Method::Head | Method::Delete | Method::Options => false,
        }
    }
}

/// The version a message gave.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Version {
    Http10,
    /// HTTP/1.1, or a later 1.x, read as 1.1 (RFC 9112, 2.3).
    Http11,
}
