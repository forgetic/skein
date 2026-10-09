//! A `FileIo` child for a world's socket owner (io.md, sections 5.2 and 5.3).
//! Queued requests keep their deadlines; descriptor cleanup remains admitted
//! until it can close the descriptor. Cancellation is routed even while a
//! different request runs. File operations use the upper token bit, leaving
//! the socket driver's namespace intact. The owner still routes file terminals
//! and closes its startup root after its component has settled.

use alloc::collections::VecDeque;
use skein_io::file::{Event, Request};
use skein_io::file_layer::{self, FileIo};
use skein_io::kernel::{Complete, Error, Fd, Op, Submit};
use skein_lib::{Duration, Queue, Time, Token};

const TAG: u64 = 1 << 63;
const ROOM: u32 = 64;

struct Queued {
    request: Request,
    deadline: Time,
}

/// The shared file-driver adapter, retaining file operations until their terminals.
#[expect(missing_debug_implementations, reason = "file requests may contain private bytes")]
pub struct Driver {
    io: FileIo,
    root: Token,
    queue: VecDeque<Queued>,
    events: Queue<Event>,
    submissions: Queue<Submit>,
    max: u32,
    closing: bool,
}

impl Driver {
    /// Adopts the owner's startup root after configuring its effective user.
    #[must_use]
    pub fn new(root: Fd, files: u32, max: u32, stall: Duration, user: u32) -> Driver {
        let mut io = FileIo::with_whole_limit(files, 32, 4, max, stall).with_effective_user(user);
        io.seed_randomness(123);
        let root = io.adopt_root(root).expect("world startup root");
        Driver {
            io,
            root,
            queue: VecDeque::with_capacity(usize::try_from(ROOM).expect("room")),
            events: Queue::with_capacity(ROOM),
            submissions: Queue::with_capacity(ROOM),
            max,
            closing: false,
        }
    }

    /// The adopted startup root, which the component borrows and never closes.
    #[must_use]
    pub const fn root(&self) -> Token {
        self.root
    }

    /// Admits one bounded request, retaining its stated deadline while queued.
    pub fn request(&mut self, request: Request, deadline: Time) {
        assert!(self.queue.len() < usize::try_from(ROOM).expect("bounded queue"), "file request queue bound");
        self.queue.push_back(Queued { request, deadline });
    }

    /// Cancels a queued request once, or asks `FileIo` to cancel the active owner.
    pub fn cancel(&mut self, owner: Token) {
        if let Some(index) = self
            .queue
            .iter()
            .position(|queued| owner_of(&queued.request) == owner && !matches!(queued.request, Request::Close { .. }))
        {
            let removed = self.queue.remove(index).expect("found queued request");
            drop(removed);
            self.events.push(Event::Cancelled { owner });
        } else {
            file_layer::cancel(&mut self.io, owner, &mut self.submissions);
        }
    }

    /// Fires deadlines and starts the next request once `FileIo` has settled.
    pub fn progress(&mut self, now: Time) {
        if self.io.is_due(now) {
            file_layer::expire(&mut self.io, now, &mut self.submissions);
        }
        for index in (0..self.queue.len()).rev() {
            let queued = self.queue.get(index).expect("queued request");
            if queued.deadline <= now && !matches!(queued.request, Request::Close { .. }) {
                let queued = self.queue.remove(index).expect("expired request");
                self.events.push(Event::Failed {
                    owner: owner_of(&queued.request),
                    error: Error::TimedOut,
                    committed: false,
                    residue: None,
                });
            }
        }
        if self.io.takes()
            && let Some(queued) = self.queue.pop_front()
        {
            if now >= queued.deadline && !matches!(queued.request, Request::Close { .. }) {
                self.events.push(Event::Failed {
                    owner: owner_of(&queued.request),
                    error: Error::TimedOut,
                    committed: false,
                    residue: None,
                });
            } else {
                file_layer::down_until(
                    &mut self.io,
                    queued.deadline,
                    queued.request,
                    &mut self.events,
                    &mut self.submissions,
                );
            }
        }
    }

    /// Routes one tagged kernel completion back to the file driver's local namespace.
    pub fn up(&mut self, mut complete: Complete) {
        assert!(Self::owns(complete.op), "completion belongs to the file namespace");
        complete.op = Token::new(complete.op.raw() & !TAG);
        if let Op::Cancel { target } = &mut complete.kind {
            *target = Token::new(target.raw() & !TAG);
        }
        file_layer::up(&mut self.io, complete, &mut self.events, &mut self.submissions);
    }

    /// Whether this kernel owner token belongs to the file driver.
    #[must_use]
    pub const fn owns(token: Token) -> bool {
        token.raw() & TAG != 0
    }

    /// One kernel submission, tagged without changing its cancellation target's identity.
    #[must_use]
    pub fn take_submit(&mut self) -> Option<Submit> {
        let mut submit = self.submissions.pop()?;
        assert!(!Self::owns(submit.op), "finite worlds keep local tokens below the namespace bit");
        submit.op = Token::new(submit.op.raw() | TAG);
        if let Op::Cancel { target } = &mut submit.kind {
            *target = Token::new(target.raw() | TAG);
        }
        Some(submit)
    }

    /// One file terminal for the component owner to route by its request token.
    #[must_use]
    pub fn take_event(&mut self) -> Option<Event> {
        self.events.pop()
    }

    /// Closes the startup root only after the component says Closed.
    pub fn close_root(&mut self, now: Time) {
        assert!(!self.closing, "startup root closes once after component settlement");
        self.closing = true;
        self.request(
            Request::Close { owner: Token::new(u64::MAX), file: self.root },
            now.saturating_add(Duration::from_secs(1)),
        );
    }

    /// The earliest queued or active request deadline.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.queue
            .iter()
            .filter(|queued| !matches!(queued.request, Request::Close { .. }))
            .map(|queued| queued.deadline)
            .chain(self.io.next_deadline())
            .min()
    }

    /// Work the owner can do before sleeping.
    #[must_use]
    pub fn has_work(&self) -> bool {
        !self.events.is_empty() || !self.submissions.is_empty() || (self.io.takes() && !self.queue.is_empty())
    }

    /// Every descriptor and admitted operation has settled after startup-root closing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.closing
            && self.io.takes()
            && self.io.open_files() == 0
            && self.queue.is_empty()
            && self.events.is_empty()
            && self.submissions.is_empty()
    }

    /// The file driver, queued request payloads and output cells at this world's limits.
    #[must_use]
    pub fn worst_case(&self, files: u32) -> u64 {
        let payload = 4095_u64
            .checked_add(u64::from(self.max))
            .and_then(|bytes| bytes.checked_add(u64::try_from(size_of::<Queued>()).expect("queued cell")))
            .and_then(|bytes| bytes.checked_mul(u64::from(ROOM)))
            .expect("bounded queued payload");
        FileIo::worst_case(files, 32, 4, self.max)
            .expect("file driver bound")
            .checked_add(payload)
            .and_then(|bytes| bytes.checked_add(Queue::<Event>::worst_case(ROOM).expect("terminal queue")))
            .and_then(|bytes| bytes.checked_add(Queue::<Submit>::worst_case(ROOM).expect("kernel queue")))
            .expect("bounded file owner")
    }
}

fn owner_of(request: &Request) -> Token {
    match request {
        Request::MakeDirectory { owner, .. }
        | Request::OpenPrivate { owner, .. }
        | Request::Create { owner, .. }
        | Request::CreateNoFollow { owner, .. }
        | Request::OpenRead { owner, .. }
        | Request::OpenReadNoFollow { owner, .. }
        | Request::OpenDirectory { owner, .. }
        | Request::Load { owner, .. }
        | Request::Scan { owner, .. }
        | Request::Store { owner, .. }
        | Request::Stat { owner, .. }
        | Request::WriteAt { owner, .. }
        | Request::ReadAt { owner, .. }
        | Request::Sync { owner, .. }
        | Request::Close { owner, .. }
        | Request::SyncDirectory { owner, .. }
        | Request::Rename { owner, .. }
        | Request::Remove { owner, .. }
        | Request::List { owner, .. } => *owner,
    }
}
