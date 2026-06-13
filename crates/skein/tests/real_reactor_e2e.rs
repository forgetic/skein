//! End-to-end tests for the production I/O path: a runtime built with the
//! real platform reactor (epoll on Linux), exactly as downstream consumers
//! wire it (`RuntimeBuilder ... .with_reactor(create_reactor()?)`).
//!
//! Without an attached reactor the runtime has no I/O driver and interest
//! registration silently degrades to a busy-poll fallback (the waker is
//! re-woken immediately), which masks lost-wakeup and rearm bugs in the
//! readiness path. These tests therefore wrap the reactor in a counting
//! decorator and assert that registrations and readiness events actually
//! flowed through it — a test here cannot quietly pass on the fallback.
//!
//! I/O must run inside spawned tasks: the scheduler installs the ambient
//! `Cx` (and with it the I/O driver handle) only while polling tasks, not
//! inside `block_on`.

use skein::http::h1::http_client::HttpClient;
use skein::http::h1::listener::Http1Listener;
use skein::http::h1::types::{Request, Response};
use skein::io::{AsyncReadExt, AsyncWriteExt};
use skein::net::tcp::listener::TcpListener;
use skein::net::tcp::stream::TcpStream;
use skein::process::{Command, Stdio};
use skein::runtime::reactor::{Events, Interest, Reactor, Source, Token, create_reactor};
use skein::runtime::{Runtime, RuntimeBuilder};
use skein::test_utils::init_test_logging;
use skein::time::{timeout, wall_now};
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

/// Reactor decorator that counts operations, proving I/O readiness flowed
/// through the real reactor rather than the no-driver busy-poll fallback.
struct CountingReactor {
    inner: Arc<dyn Reactor>,
    registers: AtomicUsize,
    events: AtomicUsize,
}

impl CountingReactor {
    fn new(inner: Arc<dyn Reactor>) -> Self {
        Self {
            inner,
            registers: AtomicUsize::new(0),
            events: AtomicUsize::new(0),
        }
    }
}

impl Reactor for CountingReactor {
    fn register(&self, source: &dyn Source, token: Token, interest: Interest) -> io::Result<()> {
        self.registers.fetch_add(1, Ordering::Relaxed);
        self.inner.register(source, token, interest)
    }

    fn modify(&self, token: Token, interest: Interest) -> io::Result<()> {
        self.inner.modify(token, interest)
    }

    fn deregister(&self, token: Token) -> io::Result<()> {
        self.inner.deregister(token)
    }

    fn poll(&self, events: &mut Events, poll_timeout: Option<Duration>) -> io::Result<usize> {
        let n = self.inner.poll(events, poll_timeout)?;
        self.events.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }

    fn wake(&self) -> io::Result<()> {
        self.inner.wake()
    }

    fn registration_count(&self) -> usize {
        self.inner.registration_count()
    }
}

/// Builds a runtime wired to the real platform reactor, as production
/// consumers do, with counters interposed.
fn reactor_runtime(workers: usize) -> (Runtime, Arc<CountingReactor>) {
    init_test_logging();
    let reactor = Arc::new(CountingReactor::new(
        create_reactor().expect("create platform reactor"),
    ));
    let runtime = RuntimeBuilder::new()
        .worker_threads(workers)
        .with_reactor(reactor.clone())
        .build()
        .expect("build runtime with real reactor");
    (runtime, reactor)
}

/// Asserts that the test's I/O went through the real reactor.
fn assert_reactor_used(reactor: &CountingReactor) {
    let registers = reactor.registers.load(Ordering::Relaxed);
    let events = reactor.events.load(Ordering::Relaxed);
    assert!(
        registers > 0,
        "no interest was registered with the real reactor — \
         the runtime fell back to busy-polling"
    );
    assert!(
        events > 0,
        "no readiness events were dispatched by the real reactor — \
         wakeups did not flow through the production path"
    );
}

/// Runs a future on the runtime with a watchdog so a lost wakeup fails the
/// test instead of hanging it.
fn block_on_guarded<F: Future>(runtime: &Runtime, fut: F) -> F::Output {
    runtime
        .block_on(timeout(wall_now(), Duration::from_secs(30), Box::pin(fut)))
        .unwrap_or_else(|_| {
            panic!("e2e test timed out — possible lost wakeup in the reactor path")
        })
}

#[test]
fn tcp_echo_round_trip_via_real_reactor() {
    let (runtime, reactor) = reactor_runtime(1);
    let handle = runtime.handle();

    let (addr_tx, addr_rx) = mpsc::channel();
    let server = handle.spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        addr_tx
            .send(listener.local_addr().expect("local addr"))
            .expect("send addr");
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 4];
        sock.read_exact(&mut buf).await.expect("server read");
        sock.write_all(&buf).await.expect("server write");
    });

    let addr = addr_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server never reported its address");

    let client = handle.spawn(async move {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(b"ping").await.expect("client write");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("client read");
        buf
    });

    let echoed = block_on_guarded(&runtime, client);
    assert_eq!(&echoed, b"ping");
    block_on_guarded(&runtime, server);

    assert_reactor_used(&reactor);
}

#[test]
fn http_client_server_round_trip_via_real_reactor() {
    let (runtime, reactor) = reactor_runtime(1);
    let handle = runtime.handle();

    let (addr_tx, addr_rx) = mpsc::channel();
    let run_handle = handle.clone();
    let server = handle.spawn(async move {
        let listener = Http1Listener::bind("127.0.0.1:0", |req: Request| async move {
            Response::new(200, "OK", req.uri.into_bytes())
        })
        .await
        .expect("bind http listener");
        addr_tx
            .send((
                listener.local_addr().expect("local addr"),
                listener.shutdown_signal(),
            ))
            .expect("send addr");
        listener.run(&run_handle).await.expect("listener run");
    });

    let (addr, shutdown) = addr_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server never reported its address");

    let client = handle.spawn(async move {
        let client = HttpClient::new();
        client
            .get(&format!("http://{addr}/e2e"))
            .await
            .expect("http get")
    });

    let resp = block_on_guarded(&runtime, client);
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"/e2e");

    let _ = shutdown.begin_drain(Duration::from_millis(500));
    block_on_guarded(&runtime, server);

    assert_reactor_used(&reactor);
}

#[test]
fn process_pipe_drain_via_real_reactor() {
    let (runtime, reactor) = reactor_runtime(1);
    let handle = runtime.handle();

    // Both streams exceed the 64 KiB pipe buffer, so the reads must go
    // Pending and resume on reactor readiness events.
    let (out_tx, out_rx) = mpsc::channel();
    handle.spawn_with_cx(move |cx| async move {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(
                "head -c 200000 /dev/zero | tr '\\0' a; \
                 head -c 200000 /dev/zero | tr '\\0' b >&2",
            )
            .stdin(Stdio::Null)
            .stdout(Stdio::Pipe)
            .stderr(Stdio::Pipe)
            .spawn()
            .expect("spawn child");
        let output = child
            .wait_with_output_async(&cx)
            .await
            .expect("wait_with_output_async");
        out_tx.send(output).expect("send output");
    });

    let output = out_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("child output never arrived — possible lost wakeup");
    assert!(output.status.success());
    assert_eq!(output.stdout.len(), 200_000);
    assert_eq!(output.stderr.len(), 200_000);
    assert!(output.stdout.iter().all(|&b| b == b'a'));
    assert!(output.stderr.iter().all(|&b| b == b'b'));

    assert_reactor_used(&reactor);
    drop(runtime);
}

#[test]
fn wall_clock_sleep_fires_on_reactor_runtime() {
    let (runtime, _reactor) = reactor_runtime(1);
    let handle = runtime.handle();

    let (elapsed_tx, elapsed_rx) = mpsc::channel();
    handle.spawn_with_cx(move |cx| async move {
        let now = cx
            .timer_driver()
            .map_or_else(wall_now, |driver| driver.now());
        let start = std::time::Instant::now();
        skein::time::sleep(now, Duration::from_millis(100)).await;
        elapsed_tx.send(start.elapsed()).expect("send elapsed");
    });

    let elapsed = elapsed_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("sleep never completed — timer wakeup lost");
    assert!(
        elapsed >= Duration::from_millis(80),
        "sleep(100ms) returned after only {elapsed:?}"
    );
    drop(runtime);
}

#[test]
fn multi_worker_concurrent_connects_via_real_reactor() {
    const TASKS: usize = 8;
    const ITERS: usize = 25;

    let (runtime, reactor) = reactor_runtime(2);
    let handle = runtime.handle();

    // Synchronous echo server on a plain OS thread: accepts one connection
    // at a time, echoes one 8-byte message, and moves on. Concurrent client
    // connects queue in the listen backlog.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind sync listener");
    let addr = listener.local_addr().expect("local addr");
    let server = std::thread::spawn(move || {
        for _ in 0..TASKS * ITERS {
            let (mut sock, _) = listener.accept().expect("sync accept");
            let mut buf = [0u8; 8];
            std::io::Read::read_exact(&mut sock, &mut buf).expect("sync read");
            std::io::Write::write_all(&mut sock, &buf).expect("sync write");
        }
    });

    let mut joins = Vec::new();
    for task in 0..TASKS {
        joins.push(handle.spawn(async move {
            for iter in 0..ITERS {
                let mut stream = TcpStream::connect(addr).await.expect("connect");
                let msg = [task as u8, iter as u8, 0, 1, 2, 3, 4, 5];
                stream.write_all(&msg).await.expect("write");
                let mut echoed = [0u8; 8];
                stream.read_exact(&mut echoed).await.expect("read");
                assert_eq!(echoed, msg, "echo mismatch on task {task} iter {iter}");
            }
            ITERS
        }));
    }

    let total = block_on_guarded(&runtime, async move {
        let mut total = 0;
        for join in joins {
            total += join.await;
        }
        total
    });
    assert_eq!(total, TASKS * ITERS);
    server.join().expect("sync echo server panicked");

    assert_reactor_used(&reactor);
}
