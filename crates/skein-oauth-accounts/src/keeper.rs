//! Private keeper ownership (oauth.md, section 6.4; io.md, sections 5.2 and 5.3).
//! Keeps configuration, one private root and one outstanding file owner token;
//! never reads filesystem metadata itself. The component schedules requests,
//! receives file terminals, and retains cancelled requests until they settle.

use alloc::boxed::Box;
use skein_io::file::Expect;
use skein_lib::{Time, Token};

pub(crate) enum Store {
    Owner,
    Private(Private),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    Open,
    Load,
    Store { generation: u64 },
    Close,
}

pub(crate) struct Pending {
    pub(crate) operation: Operation,
    pub(crate) owner: Token,
    pub(crate) deadline: Option<Time>,
    pub(crate) cancel: bool,
    pub(crate) cancelled: bool,
}

pub(crate) struct Private {
    pub(crate) base: Token,
    pub(crate) directory: Box<[u8]>,
    pub(crate) file: Box<[u8]>,
    pub(crate) root: Option<Token>,
    pub(crate) expected: Expect,
    pub(crate) next: Option<Operation>,
    pub(crate) pending: Option<Pending>,
    pub(crate) waiting_grant: bool,
    pub(crate) closed: bool,
}

impl Private {
    pub(crate) fn new(base: Token, directory: Box<[u8]>, file: Box<[u8]>) -> Private {
        Private {
            base,
            directory,
            file,
            root: None,
            expected: Expect::Absent,
            next: Some(Operation::Open),
            pending: None,
            waiting_grant: false,
            closed: false,
        }
    }

    pub(crate) fn reload(&mut self) {
        if self.next.is_none() && self.pending.is_none() {
            self.next = Some(if self.root.is_some() { Operation::Load } else { Operation::Open });
        }
    }

    pub(crate) fn has_work(&self) -> bool {
        self.next.is_some()
            || match &self.pending {
                Some(pending) => pending.cancel && !pending.cancelled,
                None => false,
            }
    }
}
