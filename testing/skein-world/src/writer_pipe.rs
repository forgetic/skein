//! A shell diagnostic writer bound to a hosted pipe (shell.md, section 12;
//! examples.md, section 6; simulator.md, section 3). It keeps a bounded byte
//! capture and translates host tokens beside its own writes. It knows no
//! service policy. `PipeWriter::new` supplies a `Write` sink at startup;
//! `PipeCapture::host` sends those bytes through the host's actual pipe and
//! delays its close until the tail has settled, in either backend.

use alloc::rc::Rc;
use core::cell::RefCell;
use skein_io::kernel::{Complete, Done, Error, Exit, Fd, Op, Submit};
use skein_lib::{Map, Queue, Time, Token, Wall, bytes};
use std::io::{self, Write};

use crate::Host;

#[derive(Debug)]
struct Captured {
    bytes: Queue<u8>,
    closed: bool,
    pending: u32,
}

/// A bounded diagnostic sink a hosted shell writes before its pipe closes.
#[derive(Clone, Debug)]
pub struct PipeWriter(Rc<RefCell<Captured>>);

/// The receiving half of a diagnostic capture, attached to one host.
#[derive(Debug)]
pub struct PipeCapture {
    fd: Fd,
    captured: Rc<RefCell<Captured>>,
}

impl PipeWriter {
    /// Makes a sink and its bridge for a writable inherited pipe. A full
    /// capture returns `WouldBlock`; the caller decides how to handle it.
    #[must_use]
    pub fn new(fd: Fd, capacity: u32) -> (PipeWriter, PipeCapture) {
        assert!(capacity > 0, "a diagnostic capture has room");
        capacity.checked_mul(2).expect("queued and in-flight diagnostic counts fit");
        let captured =
            Rc::new(RefCell::new(Captured { bytes: Queue::with_capacity(capacity), closed: false, pending: 0 }));
        (PipeWriter(captured.clone()), PipeCapture { fd, captured })
    }
}

impl Write for PipeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut captured = self.0.borrow_mut();
        if captured.closed {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        let count = bytes.len().min(usize::try_from(captured.bytes.room()).expect("capacity fits"));
        if count == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        captured.pending = captured
            .pending
            .checked_add(u32::try_from(count).expect("bounded write"))
            .expect("bounded outstanding diagnostics");
        for byte in bytes.iter().take(count) {
            captured.bytes.push(*byte);
        }
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        let captured = self.0.borrow();
        if captured.pending > 0 { Err(io::ErrorKind::WouldBlock.into()) } else { Ok(()) }
    }
}

#[derive(Clone, Copy, Debug)]
enum Binding {
    Host { original: Token, cancel: Option<Token> },
    Write,
    Close,
}

#[derive(Clone, Copy, Debug)]
enum Closing {
    Open,
    Asked(Option<Token>),
    InFlight,
    Closed,
}

/// A host whose captured diagnostics reach its real or simulated pipe tail
/// before its exit. The inner host may request that pipe's close; otherwise
/// the bridge closes it when the host settles. No destructor does work.
#[derive(Debug)]
pub struct PipeHost<P> {
    host: P,
    capture: PipeCapture,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    bindings: Map<Token, Binding>,
    tokens: Map<Token, Token>,
    next: u64,
    writing: bool,
    closing: Closing,
    failure: Option<Error>,
    operations: u32,
    worst: u64,
}

impl PipeCapture {
    /// Wraps the same adapter the binary ships. Its pipe descriptor belongs
    /// exclusively to this bridge; the inner host may close it, but writes
    /// its diagnostics through the associated `PipeWriter`.
    #[must_use]
    pub fn host<P: Host>(self, host: P) -> PipeHost<P> {
        let operations = host.operations().checked_add(1).expect("room for the diagnostic write");
        let capacity = self.captured.borrow().bytes.capacity();
        let worst = host
            .worst_case()
            .checked_add(Map::<Token, Binding>::worst_case(operations).expect("bounded bindings"))
            .and_then(|bytes| bytes.checked_add(Map::<Token, Token>::worst_case(operations)?))
            .and_then(|bytes| bytes.checked_add(Queue::<Complete>::worst_case(operations)?))
            .and_then(|bytes| bytes.checked_add(Queue::<Submit>::worst_case(operations)?))
            .and_then(|bytes| bytes.checked_add(u64::from(capacity).checked_mul(2)?))
            .and_then(|bytes| bytes.checked_add(u64::try_from(size_of::<Captured>()).ok()?))
            .and_then(|bytes| bytes.checked_add(u64::try_from(size_of::<PipeHost<P>>()).ok()?))
            .and_then(|bytes| bytes.checked_add(u64::try_from(size_of::<usize>().checked_mul(2)?).ok()?))
            .expect("bounded diagnostic bridge memory");
        PipeHost {
            host,
            capture: self,
            completions: Queue::with_capacity(operations),
            submissions: Queue::with_capacity(operations),
            bindings: Map::with_capacity(operations),
            tokens: Map::with_capacity(operations),
            next: 1,
            writing: false,
            closing: Closing::Open,
            failure: None,
            operations,
            worst,
        }
    }
}

impl<P: Host> PipeHost<P> {
    #[must_use]
    pub const fn inner(&self) -> &P {
        &self.host
    }

    pub const fn inner_mut(&mut self) -> &mut P {
        &mut self.host
    }

    /// The first pipe failure, if diagnostics could not reach the reader.
    #[must_use]
    pub const fn failure(&self) -> Option<Error> {
        self.failure
    }

    fn submit(&mut self, kind: Op, binding: Binding) {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).expect("diagnostic bridge tokens fit");
        assert!(self.bindings.insert(token, binding).expect("operation provision").is_none(), "fresh token");
        if let Binding::Host { original, .. } = binding {
            assert!(
                self.tokens.insert(original, token).expect("host operation provision").is_none(),
                "host tokens are unique while in flight"
            );
        }
        self.submissions.push(Submit { op: token, kind });
    }

    fn collect(&mut self) {
        while let Some(mut record) = self.host.submissions().pop() {
            if matches!(record.kind, Op::Close { fd } if fd == self.capture.fd) {
                assert!(matches!(self.closing, Closing::Open), "the diagnostic pipe closes once");
                self.capture.captured.borrow_mut().closed = true;
                self.closing = Closing::Asked(Some(record.op));
            } else {
                assert!(
                    !matches!(record.kind, Op::PipeWrite { fd, .. } if fd == self.capture.fd),
                    "the bridge alone writes its diagnostic pipe"
                );
                let cancel = if let Op::Cancel { target } = &mut record.kind {
                    let original = *target;
                    *target = self.tokens.get(&original).copied().unwrap_or(Token::new(0));
                    Some(original)
                } else {
                    None
                };
                self.submit(record.kind, Binding::Host { original: record.op, cancel });
            }
        }
    }

    fn pump(&mut self) {
        if self.writing {
            return;
        }
        if self.failure.is_none() && !self.capture.captured.borrow().bytes.is_empty() {
            let mut captured = self.capture.captured.borrow_mut();
            let mut bytes = bytes::zeroed(usize::try_from(captured.bytes.len()).expect("capture capacity fits"));
            for byte in &mut bytes {
                *byte = captured.bytes.pop().expect("counted capture bytes");
            }
            drop(captured);
            self.writing = true;
            self.submit(Op::PipeWrite { fd: self.capture.fd, bytes, from: 0 }, Binding::Write);
            return;
        }
        if matches!(self.closing, Closing::Open) && self.host.is_empty() {
            self.capture.captured.borrow_mut().closed = true;
            self.closing = Closing::Asked(None);
        }
        if let Closing::Asked(original) = self.closing {
            let binding = original.map_or(Binding::Close, |original| Binding::Host { original, cancel: None });
            self.submit(Op::Close { fd: self.capture.fd }, binding);
            self.closing = Closing::InFlight;
        }
    }

    fn landed(&mut self, mut complete: Complete) {
        match self.bindings.remove(&complete.op).expect("a bridged completion has its binding") {
            Binding::Host { original, cancel } => {
                assert_eq!(self.tokens.remove(&original), Some(complete.op), "a host token returns once");
                complete.op = original;
                if let Some(target) = cancel {
                    complete.kind = Op::Cancel { target };
                }
                if matches!(complete.kind, Op::Close { fd } if fd == self.capture.fd) {
                    self.closing = Closing::Closed;
                }
                self.host.completions().push(complete);
            }
            Binding::Write => {
                self.writing = false;
                match complete.result {
                    Ok(Done::Count(count)) => {
                        let Op::PipeWrite { fd, bytes, from } = complete.kind else {
                            unreachable!("write returns its record");
                        };
                        {
                            let mut captured = self.capture.captured.borrow_mut();
                            captured.pending = captured.pending.checked_sub(count).expect("written bytes were pending");
                        }
                        let end = from.checked_add(count).expect("bounded pipe progress");
                        assert!(
                            count > 0 && usize::try_from(end).expect("bounded count") <= bytes.len(),
                            "pipe write makes bounded progress"
                        );
                        if usize::try_from(end).expect("bounded count") < bytes.len() {
                            self.writing = true;
                            self.submit(Op::PipeWrite { fd, bytes, from: end }, Binding::Write);
                        }
                    }
                    Err(error) => {
                        self.failure = Some(error);
                        let mut captured = self.capture.captured.borrow_mut();
                        captured.closed = true;
                        captured.pending = 0;
                        while captured.bytes.pop().is_some() {}
                    }
                    Ok(_) => unreachable!("a pipe write counts bytes"),
                }
            }
            Binding::Close => {
                assert!(
                    matches!(complete.kind, Op::Close { fd } if fd == self.capture.fd),
                    "the bridge closes its pipe"
                );
                self.closing = Closing::Closed;
            }
        }
    }
}

impl<P: Host> Host for PipeHost<P> {
    fn iterate(&mut self, now: Time, wall: Wall) {
        while self.host.completions().room() > 0 {
            let Some(complete) = self.completions.pop() else {
                break;
            };
            self.landed(complete);
        }
        self.host.iterate(now, wall);
    }
    fn drain(&mut self) {
        self.host.drain();
        self.collect();
        self.pump();
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.host.work_pending(now)
            || !self.completions.is_empty()
            || !self.submissions.is_empty()
            || (!self.writing && !self.capture.captured.borrow().bytes.is_empty())
    }
    fn next_deadline(&self) -> Option<Time> {
        self.host.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.host.is_empty()
            && matches!(self.closing, Closing::Closed)
            && self.bindings.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.capture.captured.borrow().bytes.is_empty()
    }
    fn exit(&self) -> Option<Exit> {
        if self.is_empty() { self.host.exit() } else { None }
    }
    fn worst_case(&self) -> u64 {
        self.worst
    }
    fn operations(&self) -> u32 {
        self.operations
    }
}
