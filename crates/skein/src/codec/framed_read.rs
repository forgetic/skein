//! Async framed reader combining `AsyncRead` with a `Decoder`.

use crate::bytes::BytesMut;
use crate::codec::Decoder;
use crate::io::{AsyncRead, ReadBuf};
use crate::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Default read buffer capacity.
const DEFAULT_CAPACITY: usize = 8192;

/// Stack buffer size for reads.
const READ_BUF_SIZE: usize = 8192;

/// Async framed reader that applies a `Decoder` to an `AsyncRead` source.
///
/// Implements `Stream` where each item is a decoded frame. Data is read
/// from the inner reader into an internal buffer, then the decoder extracts
/// complete frames.
///
/// # Cancel Safety
///
/// `poll_next` is cancel-safe. Partial data remains in the internal buffer
/// across cancellations. No decoded frame is lost unless it was already yielded.
pub struct FramedRead<R, D> {
    inner: R,
    decoder: D,
    buffer: BytesMut,
    eof: bool,
}

impl<R, D> FramedRead<R, D> {
    /// Creates a new `FramedRead` with the default buffer capacity.
    pub fn new(inner: R, decoder: D) -> Self {
        Self::with_capacity(inner, decoder, DEFAULT_CAPACITY)
    }

    /// Creates a new `FramedRead` with the specified buffer capacity.
    pub fn with_capacity(inner: R, decoder: D, capacity: usize) -> Self {
        Self {
            inner,
            decoder,
            buffer: BytesMut::with_capacity(capacity),
            eof: false,
        }
    }

    /// Returns a reference to the underlying reader.
    #[must_use]
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Returns a mutable reference to the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Returns a reference to the decoder.
    #[must_use]
    pub fn decoder(&self) -> &D {
        &self.decoder
    }

    /// Returns a mutable reference to the decoder.
    pub fn decoder_mut(&mut self) -> &mut D {
        &mut self.decoder
    }

    /// Returns a reference to the read buffer.
    #[must_use]
    pub fn read_buffer(&self) -> &BytesMut {
        &self.buffer
    }

    /// Consumes `self` and returns the inner reader.
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Consumes `self` and returns the inner reader, decoder, and buffer.
    pub fn into_parts(self) -> (R, D, BytesMut) {
        (self.inner, self.decoder, self.buffer)
    }
}

impl<R, D> Stream for FramedRead<R, D>
where
    R: AsyncRead + Unpin,
    D: Decoder + Unpin,
{
    type Item = Result<D::Item, D::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Try to decode a frame from buffered data.
            if !this.eof {
                match this.decoder.decode(&mut this.buffer) {
                    Ok(Some(item)) => return Poll::Ready(Some(Ok(item))),
                    Ok(None) => {} // Need more data
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
            }

            // If we hit EOF, give the decoder one last chance.
            if this.eof {
                return match this.decoder.decode_eof(&mut this.buffer) {
                    Ok(Some(item)) => Poll::Ready(Some(Ok(item))),
                    Ok(None) => Poll::Ready(None),
                    Err(e) => Poll::Ready(Some(Err(e))),
                };
            }

            // Read more data from the underlying reader.
            let mut tmp = [0u8; READ_BUF_SIZE];
            let mut read_buf = ReadBuf::new(&mut tmp);

            match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Ok(())) => {
                    let filled = read_buf.filled();
                    if filled.is_empty() {
                        this.eof = true;
                        // Loop back to handle EOF decoding.
                    } else {
                        this.buffer.put_slice(filled);
                        // Loop back to try decoding.
                    }
                }
            }
        }
    }
}

impl<R: std::fmt::Debug, D: std::fmt::Debug> std::fmt::Debug for FramedRead<R, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedRead")
            .field("inner", &self.inner)
            .field("decoder", &self.decoder)
            .field("buffer_len", &self.buffer.len())
            .field("eof", &self.eof)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::LinesCodec;
    use std::io;
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWaker))
    }

    /// A reader that yields all data immediately.
    struct SliceReader {
        data: Vec<u8>,
        pos: usize,
    }

    impl SliceReader {
        fn new(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                pos: 0,
            }
        }
    }

    impl AsyncRead for SliceReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let remaining = &this.data[this.pos..];
            if remaining.is_empty() {
                return Poll::Ready(Ok(()));
            }
            let to_copy = std::cmp::min(remaining.len(), buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            this.pos += to_copy;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn framed_read_decodes_lines() {
        let reader = SliceReader::new(b"hello\nworld\n");
        let mut framed = FramedRead::new(reader, LinesCodec::new());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(Some(Ok(ref s))) if s == "hello"));

        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(Some(Ok(ref s))) if s == "world"));

        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(None)));
    }

    #[test]
    fn framed_read_handles_partial_data() {
        // Data without trailing newline triggers decode_eof.
        let reader = SliceReader::new(b"partial");
        let mut framed = FramedRead::new(reader, LinesCodec::new());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First poll: reads data, decode returns None, then reads EOF.
        // decode_eof should return an error because there's data but no newline.
        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(Some(Err(_)))));
    }

    #[test]
    fn framed_read_empty_input() {
        let reader = SliceReader::new(b"");
        let mut framed = FramedRead::new(reader, LinesCodec::new());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(None)));
    }

    #[test]
    fn framed_read_accessors() {
        let reader = SliceReader::new(b"");
        let mut framed = FramedRead::new(reader, LinesCodec::new());

        assert!(framed.read_buffer().is_empty());
        let _decoder = framed.decoder();
        let _decoder_mut = framed.decoder_mut();
        let _reader = framed.get_ref();
        let _reader_mut = framed.get_mut();
    }

    #[test]
    fn framed_read_into_parts() {
        let reader = SliceReader::new(b"leftover");
        let framed = FramedRead::new(reader, LinesCodec::new());

        let (_reader, _decoder, _buf) = framed.into_parts();
    }

    /// Reader that yields data in small chunks to test multi-read decoding.
    struct ChunkedReader {
        chunks: Vec<Vec<u8>>,
        index: usize,
    }

    impl ChunkedReader {
        fn new(chunks: Vec<&[u8]>) -> Self {
            Self {
                chunks: chunks.into_iter().map(<[u8]>::to_vec).collect(),
                index: 0,
            }
        }
    }

    impl AsyncRead for ChunkedReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.index >= this.chunks.len() {
                return Poll::Ready(Ok(()));
            }
            let chunk = &this.chunks[this.index];
            let to_copy = std::cmp::min(chunk.len(), buf.remaining());
            buf.put_slice(&chunk[..to_copy]);
            this.index += 1;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn framed_read_multi_chunk() {
        let reader = ChunkedReader::new(vec![b"hel", b"lo\nwo", b"rld\n"]);
        let mut framed = FramedRead::new(reader, LinesCodec::new());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(Some(Ok(ref s))) if s == "hello"));

        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(Some(Ok(ref s))) if s == "world"));

        let poll = Pin::new(&mut framed).poll_next(&mut cx);
        assert!(matches!(poll, Poll::Ready(None)));
    }
}
