//! A signalfd adopted at startup (io.md, section 7). It keeps the descriptor,
//! one read and the close/cancel flights. It never knows service policy: the
//! owner decides what a `Shutdown` means. [`resume`] arms the first read,
//! [`landed`] emits one `Shutdown` per signal and rearms, and [`close`]
//! settles the read before releasing the descriptor (programming-model.md,
//! section 5.3).
//!
//! | State | Event | Next state and output |
//! |---|---|---|
//! | Open | resumed | Reading: submit `ReadSignal` |
//! | Reading | signal | Reading: `Shutdown`, submit another read |
//! | Reading | read error | Closing: `Failed`, submit `Close` |
//! | Reading | Close or Abort | Settling: cancel the read |
//! | Settling | read and cancel completed | Closing: submit `Close` |
//! | Closing | close completed | Closed: `Closed` |

use skein_lib::{Id, Queue, Token};

use crate::kernel::{Done, Fd, Op, Submit};
use crate::layer::{Entity, Flight, Landed, Purpose, Tables};
use crate::records::{Error, Event};

#[derive(Debug)]
pub(crate) struct Signals {
    fd: Option<Fd>,
    owner: Option<Token>,
    read: Option<Id<Flight>>,
    close: Option<Id<Flight>>,
    cancels: u32,
    closing: bool,
    closed: bool,
}

impl Signals {
    pub(crate) fn new(fd: Fd, owner: Option<Token>) -> Signals {
        Signals { fd: Some(fd), owner, read: None, close: None, cancels: 0, closing: false, closed: false }
    }

    pub(crate) const fn is_closed(&self) -> bool {
        self.closed
    }
}

pub(crate) fn resume(signals: &mut Signals, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) {
    if !signals.closing && signals.read.is_none() {
        let fd = signals.fd.expect("an open signal source has its descriptor");
        signals.read = Some(tables.submit(subs, id, Purpose::ReadSignal, Op::ReadSignal { fd }));
    }
}

pub(crate) fn landed(
    signals: &mut Signals,
    id: Id<Entity>,
    landed: Landed,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match landed.purpose {
        Purpose::ReadSignal => {
            assert!(signals.read.take() == Some(landed.flight), "the signal read flight matches");
            if !signals.closing {
                match landed.result {
                    Ok(Done::ServiceSignal(signal)) => {
                        up.push(Event::Shutdown { signal });
                        resume(signals, id, tables, subs);
                    }
                    Err(_) => {
                        up.push(Event::Failed { owner: signals.owner.unwrap_or(id.token()), error: Error::Other });
                        signals.closing = true;
                    }
                    Ok(_) => unreachable!("a signal read answers with a service signal"),
                }
            }
        }
        Purpose::Cancel(target) => {
            signals.cancels = signals.cancels.checked_sub(1).expect("one signal cancel in flight");
            // A cancel that never submitted leaves the read waiting. Ask
            // again only while that original flight remains (kernel.md, 5).
            if crate::layer::unsubmitted(landed.result) && signals.read == Some(target) {
                tables.cancel(subs, id, target);
                signals.cancels = signals.cancels.checked_add(1).expect("one signal cancel per target");
            }
        }
        Purpose::Close => {
            assert!(signals.close.take() == Some(landed.flight), "the signal close flight matches");
            signals.closed = true;
            up.push(Event::Closed { owner: signals.owner.unwrap_or(id.token()) });
        }
        Purpose::Socket
        | Purpose::Bind
        | Purpose::Listen
        | Purpose::Accept
        | Purpose::Connect
        | Purpose::Recv
        | Purpose::Send
        | Purpose::Shutdown
        | Purpose::Discard
        | Purpose::Spawn
        | Purpose::Wait
        | Purpose::Signal
        | Purpose::Usage { .. }
        | Purpose::PipeRead
        | Purpose::PipeWrite => unreachable!("a signal source only reads, cancels and closes"),
    }
    maybe_close(signals, id, tables, subs);
}

pub(crate) fn close(signals: &mut Signals, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) {
    if signals.closing || signals.closed {
        return;
    }
    signals.closing = true;
    if let Some(read) = signals.read {
        tables.cancel(subs, id, read);
        signals.cancels = signals.cancels.checked_add(1).expect("one read cancellation");
    }
    maybe_close(signals, id, tables, subs);
}

fn maybe_close(signals: &mut Signals, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) {
    if signals.closing && signals.read.is_none() && signals.cancels == 0 && signals.close.is_none() && !signals.closed {
        let fd = signals.fd.take().expect("the source closes its descriptor once");
        signals.close = Some(tables.submit(subs, id, Purpose::Close, Op::Close { fd }));
    }
}
