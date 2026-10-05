//! The fixed page server's HTTP state flow without sockets or Chromium.

#[path = "../src/serve.rs"]
mod serve;

use std::net::{Ipv4Addr, SocketAddr};

use skein_io::{Event, Request};
use skein_lib::stream::{self, Held, Read};
use skein_lib::{Time, Token, Wall};

fn response(path: &str) -> Vec<u8> {
    let mut pages = serve::Pages::new(Time::ZERO, Wall::EPOCH);
    assert!(pages.pending());
    assert!(matches!(pages.take(), Some(Request::Listen { owner: serve::OWNER, .. })));
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 33333));
    pages.update_time(Time::ZERO, Wall::EPOCH);
    pages.event(Event::Listening { owner: serve::OWNER, listener: Token::new(200), addr });
    assert_eq!(pages.addr, Some(addr));
    let owner = Token::new(101);
    pages.event(Event::Accepted { owner: serve::OWNER, socket: Token::new(300), peer: addr });
    assert!(pages.owns(owner));

    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let mut held = Held::new(request.into_bytes().into_boxed_slice());
    let mut wire = Vec::new();
    for _ in 0..200 {
        match pages.take().expect("page server has the next step") {
            Request::Bind { socket, owner: bound } => {
                assert_eq!(socket, Token::new(300));
                assert_eq!(bound, owner);
            }
            Request::Stream { stream, down } => {
                assert_eq!(stream, owner);
                match down {
                    stream::Down::Demand { read: Read::Nothing, room: 0 } => {}
                    stream::Down::Demand { room: 1.., .. } => {
                        pages.event(Event::Stream { owner, up: stream::Up::Room });
                    }
                    stream::Down::Demand { read, room: 0 } => {
                        pages.event(Event::Stream { owner, up: held.answer(read).expect("request input") });
                    }
                    stream::Down::Send(bytes) => wire.extend_from_slice(&bytes),
                    stream::Down::Finish => panic!("HTTP server closes through io"),
                }
            }
            Request::Close { entity } if entity == owner => {
                pages.event(Event::Closed { owner });
                break;
            }
            other => panic!("unexpected page request: {other:?}"),
        }
    }
    assert!(!pages.owns(owner));
    pages.stop();
    assert_eq!(pages.take(), Some(Request::Close { entity: Token::new(200) }));
    pages.event(Event::Closed { owner: serve::OWNER });
    assert!(pages.is_closed());
    wire
}

#[test]
fn serves_button_and_csp_pages_through_http_machine() {
    let button = response("/button");
    assert!(button.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(button.windows(b"Press</button>".len()).any(|window| window == b"Press</button>"));
    let csp = response("/csp");
    assert!(
        csp.windows(b"Content-Security-Policy: default-src 'none'\r\n".len())
            .any(|window| window == b"Content-Security-Policy: default-src 'none'\r\n")
    );
    assert!(csp.windows(b"CSP page".len()).any(|window| window == b"CSP page"));
}
