//! HTTP/3 response body stream.

extern crate bytes as bytes_crate;
extern crate http as http_crate;

use bytes_crate::{Buf, Bytes, BytesMut};
use h3::error::Code;

use super::client::H3RequestStream;
use super::error::H3Error;
use crate::cx::Cx;
use http_crate::HeaderMap;

/// Streaming HTTP/3 response body.
pub struct H3Body {
    stream: H3RequestStream,
    done: bool,
}

impl H3Body {
    pub(crate) fn new(stream: H3RequestStream) -> Self {
        Self {
            stream,
            done: false,
        }
    }

    /// Read the next body chunk.
    ///
    /// Returns `None` when the body is fully consumed.
    pub async fn chunk(&mut self, cx: &Cx) -> Result<Option<Bytes>, H3Error> {
        if let Err(err) = cx.checkpoint() {
            self.cancel();
            return Err(err.into());
        }

        if self.done {
            return Ok(None);
        }

        let Some(mut buf) = self.stream.recv_data().await? else {
            self.done = true;
            return Ok(None);
        };

        let bytes = buf.copy_to_bytes(buf.remaining());
        Ok(Some(bytes))
    }

    /// Collect the full body into a single `Bytes` buffer.
    pub async fn collect(mut self, cx: &Cx) -> Result<Bytes, H3Error> {
        let mut out = BytesMut::new();
        while let Some(chunk) = self.chunk(cx).await? {
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }

    /// Receive trailing headers, if any.
    pub async fn trailers(&mut self, cx: &Cx) -> Result<Option<HeaderMap>, H3Error> {
        if let Err(err) = cx.checkpoint() {
            self.cancel();
            return Err(err.into());
        }

        Ok(self.stream.recv_trailers().await?)
    }

    fn cancel(&mut self) {
        let _ = self.stream.stop_sending(Code::H3_REQUEST_CANCELLED);
    }
}
