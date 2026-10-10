//! Socket owner tokens retain the exchange's slab generation and distinguish
//! its issuer, listener and callback (oauth.md, sections 6.1 and 6.5).

use crate::exchange::Exchange;
use skein_lib::{Id, Token};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Socket {
    Web,
    Listener,
    Callback,
}

pub(crate) fn owner(id: Id<Exchange>, socket: Socket) -> Token {
    let [s0, s1, s2, s3, g0, g1, g2, g3] = id.token().raw().to_be_bytes();
    let tag = match socket {
        Socket::Web => 0,
        Socket::Listener => 1,
        Socket::Callback => 2,
    };
    let base = u32::from_be_bytes([s0, s1, s2, s3])
        .checked_mul(3)
        .expect("startup bounds reserve three socket owners per exchange");
    let slot = base.checked_add(tag).expect("startup bounds reserve each socket tag");
    let [s0, s1, s2, s3] = slot.to_be_bytes();
    Token::new(u64::from_be_bytes([s0, s1, s2, s3, g0, g1, g2, g3]))
}

pub(crate) fn exchange(owner: Token) -> (Id<Exchange>, Socket) {
    let [s0, s1, s2, s3, g0, g1, g2, g3] = owner.raw().to_be_bytes();
    let tagged = u32::from_be_bytes([s0, s1, s2, s3]);
    let tag = tagged.checked_rem(3).expect("nonzero divisor");
    let socket = match tag {
        0 => Socket::Web,
        1 => Socket::Listener,
        2 => Socket::Callback,
        _ => unreachable!("remainder below three"),
    };
    let [s0, s1, s2, s3] = tagged.checked_div(3).expect("nonzero divisor").to_be_bytes();
    (Id::from_token(Token::new(u64::from_be_bytes([s0, s1, s2, s3, g0, g1, g2, g3]))), socket)
}
