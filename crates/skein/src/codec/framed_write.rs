//! Async framed writer combining `AsyncWrite` with an `Encoder`.

use crate::bytes::BytesMut;
use crate::codec::Encoder;
use crate::io::AsyncWrite;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Default write buffer capacity.
const DEFAULT_CAPACITY: usize = 8192;

/// Async framed writer that applies an `Encoder` to an `AsyncWrite` sink.
///
/// Items are encoded into an internal buffer, then flushed to the underlying
/// writer. Call `poll_flush` to ensure all buffered data reaches the writer.
///
/// # Cancel Safety
///
/// `send` (encode) is synchronous and always completes. `poll_flush` is
/// cancel-safe: partial writes are tracked and resumed on the next call.
pub struct FramedWrite<W, E> {
    inner: W,
    encoder: E,
    buffer: BytesMut,
}

impl<W, E> FramedWrite<W, E> {
    /// Creates a new `FramedWrite` with default buffer capacity.
    pub fn new(inner: W, encoder: E) -> Self {
        Self::with_capacity(inner, encoder, DEFAULT_CAPACITY)
    }

    /// Creates a new `FramedWrite` with the specified buffer capacity.
    pub fn with_capacity(inner: W, encoder: E, capacity: usize) -> Self {
        Self {
            inner,
            encoder,
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Returns a reference to the underlying writer.
    #[must_use]
    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    /// Returns a mutable reference to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Returns a reference to the encoder.
    #[must_use]
    pub fn encoder(&self) -> &E {
        &self.encoder
    }

    /// Returns a mutable reference to the encoder.
    pub fn encoder_mut(&mut self) -> &mut E {
        &mut self.encoder
    }

    /// Returns a reference to the write buffer.
    #[must_use]
    pub fn write_buffer(&self) -> &BytesMut {
        &self.buffer
    }

    /// Consumes `self` and returns the inner writer.
    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Consumes `self` and returns the inner writer, encoder, and buffer.
    pub fn into_parts(self) -> (W, E, BytesMut) {
        (self.inner, self.encoder, self.buffer)
    }
}

impl<W, E> FramedWrite<W, E> {
    /// Encode an item into the write buffer.
    ///
    /// The encoded data is buffered internally. Call `poll_flush` to write
    /// it to the underlying writer.
    pub fn send<I>(&mut self, item: I) -> Result<(), <E as Encoder<I>>::Error>
    where
        E: Encoder<I>,
    {
        self.encoder.encode(item, &mut self.buffer)
    }
}

impl<W, E> FramedWrite<W, E>
where
    W: AsyncWrite + Unpin,
{
    /// Flush all buffered data to the underlying writer.
    ///
    /// Returns `Poll::Ready(Ok(()))` when the buffer is empty and the
    /// underlying writer has been flushed.
    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.buffer.is_empty() {
            let n = match Pin::new(&mut self.inner).poll_write(cx, &self.buffer) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(n)) => n,
            };
            if n == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write frame to transport",
                )));
            }
            let _ = self.buffer.split_to(n);
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    /// Flush all buffered data and shut down the underlying writer.
    pub fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_flush(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<W: std::fmt::Debug, E: std::fmt::Debug> std::fmt::Debug for FramedWrite<W, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedWrite")
            .field("inner", &self.inner)
            .field("encoder", &self.encoder)
            .field("buffer_len", &self.buffer.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::LinesCodec;
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWaker))
    }

    #[test]
    fn framed_write_encodes_and_flushes() {
        let output: Vec<u8> = Vec::new();
        let mut framed = FramedWrite::new(output, LinesCodec::new());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        framed.send("hello".to_string()).unwrap();
        framed.send("world".to_string()).unwrap();

        assert_eq!(&framed.write_buffer()[..], b"hello\nworld\n");

        let poll = framed.poll_flush(&mut cx);
        assert!(matches!(poll, Poll::Ready(Ok(()))));

        assert!(framed.write_buffer().is_empty());
        assert_eq!(framed.get_ref(), b"hello\nworld\n");
    }

    #[test]
    fn framed_write_close() {
        let output: Vec<u8> = Vec::new();
        let mut framed = FramedWrite::new(output, LinesCodec::new());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        framed.send("bye".to_string()).unwrap();

        let poll = framed.poll_close(&mut cx);
        assert!(matches!(poll, Poll::Ready(Ok(()))));

        assert!(framed.write_buffer().is_empty());
        assert_eq!(framed.get_ref(), b"bye\n");
    }

    #[test]
    fn framed_write_accessors() {
        let output: Vec<u8> = Vec::new();
        let mut framed = FramedWrite::new(output, LinesCodec::new());

        assert!(framed.write_buffer().is_empty());
        let _encoder = framed.encoder();
        let _encoder_mut = framed.encoder_mut();
        let _writer = framed.get_ref();
        let _writer_mut = framed.get_mut();
    }

    #[test]
    fn framed_write_into_parts() {
        let output: Vec<u8> = Vec::new();
        let framed = FramedWrite::new(output, LinesCodec::new());

        let (_writer, _encoder, _buf) = framed.into_parts();
    }

    /// Writer that accepts only a few bytes at a time.
    struct SlowWriter {
        inner: Vec<u8>,
        max_per_write: usize,
    }

    impl SlowWriter {
        fn new(max_per_write: usize) -> Self {
            Self {
                inner: Vec::new(),
                max_per_write,
            }
        }
    }

    impl AsyncWrite for SlowWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let n = std::cmp::min(buf.len(), this.max_per_write);
            this.inner.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn framed_write_partial_writes() {
        let output = SlowWriter::new(3);
        let mut framed = FramedWrite::new(output, LinesCodec::new());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        framed.send("abcdef".to_string()).unwrap();

        let poll = framed.poll_flush(&mut cx);
        assert!(matches!(poll, Poll::Ready(Ok(()))));

        assert!(framed.write_buffer().is_empty());
        assert_eq!(&framed.get_ref().inner, b"abcdef\n");
    }
}
