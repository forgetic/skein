//! Parent ends of a child's one-way pipes, exposed as streams.

use alloc::boxed::Box;
use skein_lib::stream::{Down, Fault, OutputDown, OutputOutcome, Read, Up};
use skein_lib::{Env, Id, Intake, Queue, bytes};

use crate::kernel::{self, Done, Fd, Op, Submit, Way};
use crate::layer::{Entity, Flight, Landed, Purpose, Tables};
use crate::limits::Limits;
use crate::output::Reservation;
use crate::records::Event;

#[derive(Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent pipe directions, EOF notification and settling flags are tracked separately"
)]
pub(crate) struct Pipe {
    child: Option<Id<Entity>>,
    way: Way,
    fd: Option<Fd>,
    intake: Option<Intake>,
    demand: Option<(Read, u32)>,
    granted: u32,
    classic_grant: bool,
    independent: Reservation,
    queued: Queue<Box<[u8]>>,
    queued_bytes: u32,
    read: Option<Id<Flight>>,
    write: Option<Id<Flight>>,
    close: Option<Id<Flight>>,
    cancels: u32,
    eof: bool,
    ended: bool,
    finish: bool,
    closing: bool,
    closed: bool,
}

impl Pipe {
    pub(crate) fn pending(way: Way) -> Pipe {
        Pipe {
            child: None,
            way,
            fd: None,
            intake: None,
            demand: None,
            granted: 0,
            classic_grant: false,
            independent: Reservation::Idle,
            queued: Queue::with_capacity(0),
            queued_bytes: 0,
            read: None,
            write: None,
            close: None,
            cancels: 0,
            eof: false,
            ended: false,
            finish: false,
            closing: false,
            closed: false,
        }
    }
    pub(crate) fn activate(&mut self, child: Id<Entity>, fd: Fd, way: Way) {
        assert!(self.fd.is_none() && self.way == way, "a pending pipe activates once");
        self.child = Some(child);
        self.fd = Some(fd);
    }
    pub(crate) const fn is_closed(&self) -> bool {
        self.closed
    }
    pub(crate) const fn child(&self) -> Option<Id<Entity>> {
        self.child
    }
}

pub(crate) fn resume(
    pipe: &mut Pipe,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    if pipe.fd.is_none() || pipe.closed {
        return;
    }
    if pipe.intake.is_none() {
        pipe.intake = Some(Intake::with_capacity(env.limits.intake));
        pipe.queued = Queue::with_capacity(env.limits.sends);
    }
    progress(pipe, id, env, tables, up, subs);
}

pub(crate) fn request(
    pipe: &mut Pipe,
    id: Id<Entity>,
    down: Down,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    if pipe.fd.is_none() || pipe.closing || pipe.closed {
        return;
    }
    match down {
        Down::Demand { read, room } => {
            if read == Read::Nothing && room == 0 {
                pipe.demand = None;
                return;
            }
            if pipe.way == Way::In {
                assert!(read == Read::Nothing, "a parent write pipe has no read side");
            }
            if pipe.way == Way::Out {
                assert!(room == 0, "a parent read pipe has no write side");
            }
            assert!(room == 0 || pipe.independent.idle(), "classic room cannot overlap independent output");
            assert!(pipe.demand.is_none(), "one demand at a time");
            assert!(room <= env.limits.output, "demand fits the output cap");
            pipe.demand = Some((read, room));
            tables.ready.mark(id);
        }
        Down::Send(bytes) => {
            assert!(pipe.way == Way::In && !pipe.finish, "only an unfinished write pipe sends");
            assert!(pipe.independent.idle(), "classic Send cannot spend an independent grant");
            pipe.classic_grant = false;
            let size = u32::try_from(bytes.len()).expect("a send fits u32");
            assert!(size <= pipe.granted, "a send stays within granted room");
            pipe.granted = 0;
            if !bytes.is_empty() {
                pipe.queued_bytes = pipe.queued_bytes.checked_add(size).expect("bounded output");
                assert!(pipe.queued_bytes <= env.limits.output, "output fits its cap");
                pipe.queued.push(bytes);
                start_write(pipe, id, tables, subs);
            }
        }
        Down::Finish => {
            assert!(pipe.way == Way::In, "only a write pipe finishes");
            pipe.finish = true;
            pipe.classic_grant = false;
            let pending_output = pipe.independent.wanted().is_some();
            pipe.independent.retire(OutputOutcome::Cancelled);
            if pending_output {
                tables.ready.mark(id);
            }
            maybe_close(pipe, id, tables, subs);
        }
    }
}

pub(crate) fn landed(
    pipe: &mut Pipe,
    landed: Landed,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let id = landed.entity;
    emit_terminal(pipe, id, up);
    match landed.purpose {
        Purpose::PipeRead => {
            assert!(pipe.read.take() == Some(landed.flight), "read flight matches");
            let Op::PipeRead { buf, .. } = landed.kind else { unreachable!("a pipe read returns its buffer") };
            if !pipe.closing {
                match landed.result {
                    Ok(Done::Count(0)) => pipe.eof = true,
                    Ok(Done::Count(n)) => {
                        let n = usize::try_from(n).expect("count fits usize");
                        pipe.intake
                            .as_mut()
                            .expect("active pipe has intake")
                            .append(buf.get(..n).expect("count fits buffer"))
                            .expect("read within room");
                    }
                    Err(error) => {
                        pipe.eof = true;
                        pipe.ended = true;
                        pipe.independent.retire(OutputOutcome::Failed(fault(error)));
                        emit_terminal(pipe, id, up);
                        up.push(Event::Stream { owner: id.token(), up: Up::Failed(fault(error)) });
                    }
                    Ok(
                        Done::Nothing
                        | Done::Fd(_)
                        | Done::Accepted { .. }
                        | Done::Bound(_)
                        | Done::Stat(_)
                        | Done::Spawned { .. }
                        | Done::Exit(_),
                    ) => unreachable!("a pipe read answers with a count"),
                }
            }
        }
        Purpose::PipeWrite => {
            assert!(pipe.write.take() == Some(landed.flight), "write flight matches");
            let Op::PipeWrite { bytes, from, .. } = landed.kind else { unreachable!("a pipe write returns its bytes") };
            let total = u32::try_from(bytes.len()).expect("queued write fits u32");
            match landed.result {
                Ok(Done::Count(n)) if (!pipe.closing || pipe.finish) && from.saturating_add(n) < total => {
                    let fd = pipe.fd.expect("active pipe has descriptor");
                    let from = from.checked_add(n).expect("a short write stays within its buffer");
                    pipe.write = Some(tables.submit(subs, id, Purpose::PipeWrite, Op::PipeWrite { fd, bytes, from }));
                }
                Ok(Done::Count(n)) if n > 0 => {
                    pipe.queued_bytes = pipe.queued_bytes.checked_sub(total).expect("queued bytes include write");
                    start_write(pipe, id, tables, subs);
                }
                Err(error) if !pipe.closing => {
                    pipe.queued_bytes = pipe.queued_bytes.checked_sub(total).expect("queued bytes include write");
                    pipe.independent.retire(OutputOutcome::Failed(fault(error)));
                    emit_terminal(pipe, id, up);
                    up.push(Event::Stream { owner: id.token(), up: Up::Failed(fault(error)) });
                    pipe.closing = true;
                }
                Err(_) => {}
                Ok(
                    Done::Nothing
                    | Done::Fd(_)
                    | Done::Accepted { .. }
                    | Done::Bound(_)
                    | Done::Stat(_)
                    | Done::Spawned { .. }
                    | Done::Exit(_)
                    | Done::Count(_),
                ) => unreachable!("a pipe write counts positive bytes"),
            }
        }
        Purpose::Close => {
            assert!(pipe.close.take() == Some(landed.flight), "close flight matches");
            pipe.closed = true;
            up.push(Event::Closed { owner: id.token() });
        }
        Purpose::Cancel(_) => {
            pipe.cancels = pipe.cancels.checked_sub(1).expect("cancel in flight");
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
        | Purpose::Signal => unreachable!("a pipe only reads, writes, cancels and closes"),
    }
    progress(pipe, id, env, tables, up, subs);
}

pub(crate) fn close(pipe: &mut Pipe, id: Id<Entity>, abort: bool, tables: &mut Tables, subs: &mut Queue<Submit>) {
    if pipe.closed {
        return;
    }
    let pending_output = pipe.independent.wanted().is_some();
    pipe.independent.retire(OutputOutcome::Cancelled);
    if pending_output {
        tables.ready.mark(id);
    }
    if pipe.closing {
        if abort && pipe.finish {
            pipe.finish = false;
            while pipe.queued.pop().is_some() {}
            if let Some(write) = pipe.write {
                tables.cancel(subs, id, write);
                pipe.cancels = pipe.cancels.checked_add(1).expect("one cancel per flight");
            }
            maybe_close(pipe, id, tables, subs);
        }
        return;
    }
    if !abort && pipe.way == Way::In {
        pipe.finish = true;
        pipe.closing = true;
        // The queued output remains to flush.
        maybe_close(pipe, id, tables, subs);
        return;
    }
    pipe.closing = true;
    pipe.finish = false;
    while pipe.queued.pop().is_some() {}
    if let Some(read) = pipe.read {
        tables.cancel(subs, id, read);
        pipe.cancels = pipe.cancels.checked_add(1).expect("one cancel per flight");
    }
    if let Some(write) = pipe.write {
        tables.cancel(subs, id, write);
        pipe.cancels = pipe.cancels.checked_add(1).expect("one cancel per flight");
    }
    maybe_close(pipe, id, tables, subs);
}

fn progress(
    pipe: &mut Pipe,
    id: Id<Entity>,
    env: &Env<Limits>,
    tables: &mut Tables,
    up: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    emit_terminal(pipe, id, up);
    if pipe.closing {
        if pipe.way == Way::In {
            start_write(pipe, id, tables, subs);
        }
        maybe_close(pipe, id, tables, subs);
        return;
    }
    if pipe.way == Way::Out {
        if let Some((read, _)) = pipe.demand
            && let Some(bytes) = pipe.intake.as_mut().expect("active pipe has intake").meet(read)
        {
            pipe.demand = None;
            up.push(Event::Stream { owner: id.token(), up: Up::Bytes(bytes) });
        }
        if pipe.eof
            && !pipe.ended
            && (pipe.intake.as_ref().expect("active pipe has intake").is_empty() || pipe.demand.is_some())
        {
            pipe.ended = true;
            up.push(Event::Stream { owner: id.token(), up: Up::End });
        }
        if !pipe.eof && pipe.read.is_none() {
            let intake = pipe.intake.as_ref().expect("active pipe has intake");
            let room = intake.room().min(env.limits.receive);
            if room > 0 {
                let fd = pipe.fd.expect("active pipe has descriptor");
                pipe.read = Some(tables.submit(
                    subs,
                    id,
                    Purpose::PipeRead,
                    Op::PipeRead { fd, buf: bytes::zeroed(usize::try_from(room).expect("room fits usize")) },
                ));
            }
        }
    } else if let Some((_, room)) = pipe.demand {
        if room > 0 && pipe.queued_bytes.saturating_add(room) <= env.limits.output && pipe.queued.room() > 0 {
            pipe.granted = room;
            pipe.classic_grant = true;
            pipe.demand = None;
            up.push(Event::Stream { owner: id.token(), up: Up::Room });
        } else if room == 0 {
            pipe.demand = None;
        }
    }
    if let Some((_, bytes)) = pipe.independent.wanted()
        && pipe.way == Way::In
        && !pipe.finish
        && output_bytes(pipe, bytes, env.limits.output)
        && pipe.queued.room() > 0
    {
        let terminal = pipe.independent.grant();
        up.push(Event::Output { owner: id.token(), up: terminal });
    }
}

fn start_write(pipe: &mut Pipe, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) {
    if pipe.write.is_some() {
        return;
    }
    if let Some(bytes) = pipe.queued.pop() {
        let fd = pipe.fd.expect("active pipe has descriptor");
        pipe.write = Some(tables.submit(subs, id, Purpose::PipeWrite, Op::PipeWrite { fd, bytes, from: 0 }));
    }
}

fn maybe_close(pipe: &mut Pipe, id: Id<Entity>, tables: &mut Tables, subs: &mut Queue<Submit>) {
    if pipe.close.is_some() || pipe.fd.is_none() {
        return;
    }
    if pipe.finish && pipe.way == Way::In && pipe.write.is_none() && pipe.queued.is_empty() {
        pipe.closing = true;
    }
    if pipe.closing && pipe.read.is_none() && pipe.write.is_none() && pipe.cancels == 0 {
        let fd = pipe.fd.take().expect("close an active pipe once");
        pipe.close = Some(tables.submit(subs, id, Purpose::Close, Op::Close { fd }));
    }
}

fn fault(error: kernel::Error) -> Fault {
    match error {
        kernel::Error::BrokenPipe | kernel::Error::Reset | kernel::Error::NotConnected => Fault::Reset,
        kernel::Error::Refused
        | kernel::Error::AddressInUse
        | kernel::Error::AddressNotAvailable
        | kernel::Error::Unreachable
        | kernel::Error::TimedOut
        | kernel::Error::TooManyOpenFiles
        | kernel::Error::NoBufferSpace
        | kernel::Error::Cancelled
        | kernel::Error::TooLate
        | kernel::Error::NotFound
        | kernel::Error::Exists
        | kernel::Error::NotADirectory
        | kernel::Error::IsADirectory
        | kernel::Error::NotEmpty
        | kernel::Error::Permission
        | kernel::Error::NoSpace
        | kernel::Error::ReadOnly
        | kernel::Error::TooManyLinks
        | kernel::Error::NameTooLong
        | kernel::Error::Escape
        | kernel::Error::NotAFile
        | kernel::Error::InvalidArgument
        | kernel::Error::Other(_) => Fault::Other,
    }
}

fn emit_terminal(pipe: &mut Pipe, id: Id<Entity>, up: &mut Queue<Event>) {
    if let Some(terminal) = pipe.independent.take_terminal() {
        up.push(Event::Output { owner: id.token(), up: terminal });
    }
}

fn output_bytes(pipe: &Pipe, bytes: u32, cap: u32) -> bool {
    match pipe.queued_bytes.checked_add(bytes) {
        Some(held) => held <= cap,
        None => false,
    }
}

pub(crate) fn output_request(
    pipe: &mut Pipe,
    id: Id<Entity>,
    down: OutputDown,
    env: &Env<Limits>,
    tables: &mut Tables,
    subs: &mut Queue<Submit>,
) {
    if pipe.fd.is_none() || pipe.closing || pipe.closed || pipe.finish || pipe.way != Way::In {
        return;
    }
    match down {
        OutputDown::Room { right, bytes } => {
            let classic_room = match pipe.demand {
                Some((_, room)) => room > 0,
                None => false,
            };
            if bytes == 0 || bytes > env.limits.output || !pipe.independent.idle() || classic_room || pipe.classic_grant
            {
                return;
            }
            pipe.granted = 0;
            pipe.independent.admit(right, bytes);
            tables.ready.mark(id);
        }
        OutputDown::Cancel { right } => {
            let cancelled = pipe.independent.cancel(right);
            if cancelled {
                tables.ready.mark(id);
            }
        }
        OutputDown::Send { right, bytes } => {
            let Some(granted) = pipe.independent.granted(right) else { return };
            let length = u32::try_from(bytes.len()).expect("a matching independent pipe Send fits u32");
            assert!(length <= granted, "a matching independent pipe Send fits its grant");
            pipe.independent.release(right);
            pipe.granted = granted;
            request(pipe, id, Down::Send(bytes), env, tables, subs);
        }
        OutputDown::Release { right } => pipe.independent.release(right),
    }
}
