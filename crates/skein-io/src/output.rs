//! One native output right and its bounded staged terminal (lib.md, 7.1).

use skein_lib::Token;
use skein_lib::stream::{OutputOutcome, OutputUp};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Reservation {
    Idle,
    Waiting { right: Token, bytes: u32 },
    Granted { right: Token, bytes: u32 },
    Terminal { right: Token, outcome: OutputOutcome },
}

impl Reservation {
    pub(crate) const fn idle(&self) -> bool {
        match self {
            Reservation::Idle => true,
            Reservation::Waiting { .. } | Reservation::Granted { .. } | Reservation::Terminal { .. } => false,
        }
    }

    pub(crate) fn admit(&mut self, right: Token, bytes: u32) {
        assert!(self.idle(), "one independent output right at a time");
        *self = Reservation::Waiting { right, bytes };
    }

    pub(crate) const fn wanted(&self) -> Option<(Token, u32)> {
        match *self {
            Reservation::Waiting { right, bytes } => Some((right, bytes)),
            Reservation::Idle | Reservation::Granted { .. } | Reservation::Terminal { .. } => None,
        }
    }

    pub(crate) fn grant(&mut self) -> OutputUp {
        match *self {
            Reservation::Waiting { right, bytes } => {
                *self = Reservation::Granted { right, bytes };
                OutputUp::Settled { right, outcome: OutputOutcome::Granted }
            }
            Reservation::Idle | Reservation::Granted { .. } | Reservation::Terminal { .. } => {
                unreachable!("only a waiting right can be granted")
            }
        }
    }

    pub(crate) const fn granted(&self, token: Token) -> Option<u32> {
        match *self {
            Reservation::Granted { right, bytes } if right.raw() == token.raw() => Some(bytes),
            Reservation::Idle
            | Reservation::Waiting { .. }
            | Reservation::Granted { .. }
            | Reservation::Terminal { .. } => None,
        }
    }

    pub(crate) fn release(&mut self, right: Token) {
        if self.granted(right).is_some() {
            *self = Reservation::Idle;
        }
    }

    #[must_use]
    pub(crate) fn cancel(&mut self, token: Token) -> bool {
        match *self {
            Reservation::Waiting { right, bytes: _ } => {
                if right == token {
                    *self = Reservation::Terminal { right, outcome: OutputOutcome::Cancelled };
                    true
                } else {
                    false
                }
            }
            Reservation::Idle | Reservation::Granted { .. } | Reservation::Terminal { .. } => false,
        }
    }

    pub(crate) fn retire(&mut self, outcome: OutputOutcome) {
        *self = match *self {
            Reservation::Waiting { right, bytes: _ } => Reservation::Terminal { right, outcome },
            terminal @ Reservation::Terminal { .. } => terminal,
            Reservation::Idle | Reservation::Granted { .. } => Reservation::Idle,
        };
    }

    pub(crate) fn take_terminal(&mut self) -> Option<OutputUp> {
        match *self {
            Reservation::Terminal { right, outcome } => {
                *self = Reservation::Idle;
                Some(OutputUp::Settled { right, outcome })
            }
            Reservation::Idle | Reservation::Waiting { .. } | Reservation::Granted { .. } => None,
        }
    }
}
