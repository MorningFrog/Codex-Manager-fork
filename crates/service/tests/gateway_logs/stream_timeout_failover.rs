use super::*;
use codexmanager_core::storage::UsageSnapshotRecord;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Condvar, Mutex};

const STREAM_TIMEOUT_MS: u64 = 200;
const PRIMARY: &str = "acc_header_timeout_primary";
const SECONDARY: &str = "acc_header_timeout_secondary";
const PLATFORM_KEY: &str = "pk_header_timeout";
const KEY_ID: &str = "gk_header_timeout";
const FIXTURE_WAIT: Duration = Duration::from_secs(5);
const FIXTURE_FINISH_WAIT: Duration = Duration::from_secs(10);
const FINISH_WAKE: &str = "R05-FIXTURE-FINISH\r\n";

#[derive(Debug, Clone, PartialEq, Eq)]
enum UpstreamObservation {
    Ready,
    Accepted(usize),
    HeadersRead(usize),
    BodyRead(usize),
    CaptureDelivered(usize),
    ControlWake(usize),
    ClosedBeforeHeaders(usize),
    ClosedAfterBody(usize),
    Incomplete(usize, String),
    AcceptClosed,
    ReaderPanicked(usize),
}

struct CaptureGate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl CaptureGate {
    fn wait(&self) {
        let (open, _) = self
            .changed
            .wait_timeout_while(
                self.open
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                FIXTURE_WAIT,
                |open| !*open,
            )
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let released = *open;
        drop(open);
        assert!(released, "capture observer was not released by its test");
    }

    fn release(&self) {
        *self
            .open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        self.changed.notify_all();
    }
}

// Releases a paused observer even if the deterministic regression panics.
struct ReleaseCaptureOnDrop(Arc<CaptureGate>);

impl Drop for ReleaseCaptureOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Clone)]
struct HeaderTimeoutBehavior {
    healthy_token: Option<String>,
    close_after_body: bool,
    force_healthy: Arc<std::sync::atomic::AtomicBool>,
}

struct HeaderTimeoutUpstream {
    addr: String,
    requests: Receiver<(usize, CapturedUpstreamRequest)>,
    events: Option<Receiver<UpstreamObservation>>,
    stop: mpsc::Sender<()>,
    finished: Receiver<Result<(), String>>,
    capture_gate: Arc<CaptureGate>,
    force_healthy: Arc<std::sync::atomic::AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

impl HeaderTimeoutUpstream {
    fn start(healthy_account: Option<&str>) -> Self {
        Self::start_with_capture_gate(healthy_account, true)
    }

    fn start_with_capture_gate(healthy_account: Option<&str>, capture_open: bool) -> Self {
        Self::start_with_reader_panic(healthy_account, capture_open, None)
    }

    fn start_with_reader_panic(
        healthy_account: Option<&str>,
        capture_open: bool,
        panic_reader: Option<usize>,
    ) -> Self {
        Self::start_with_behavior(healthy_account, capture_open, panic_reader, false)
    }

    fn start_closing_after_body(healthy_account: Option<&str>) -> Self {
        Self::start_with_behavior(healthy_account, true, None, true)
    }

    fn respond_healthy_to_all(&self) {
        self.force_healthy.store(true, Ordering::Release);
    }

    fn start_with_behavior(
        healthy_account: Option<&str>,
        capture_open: bool,
        panic_reader: Option<usize>,
        close_after_body: bool,
    ) -> Self {
        let listener = bind_test_listener("silent-header upstream");
        let addr = listener.local_addr().expect("upstream address").to_string();
        let force_healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let behavior = HeaderTimeoutBehavior {
            healthy_token: healthy_account.map(|id| format!("Bearer access_{id}")),
            close_after_body,
            force_healthy: Arc::clone(&force_healthy),
        };
        let (request_tx, requests) = mpsc::channel();
        let (event_tx, events) = mpsc::channel();
        let (ready_tx, ready) = mpsc::channel();
        let (stop, stop_rx) = mpsc::channel();
        let (finished_tx, finished) = mpsc::channel();
        let capture_gate = Arc::new(CaptureGate {
            open: Mutex::new(capture_open),
            changed: Condvar::new(),
        });
        let worker_gate = Arc::clone(&capture_gate);
        let join = thread::spawn(move || {
            let mut readers = Vec::new();
            let mut sequence = 0;
            let _ = event_tx.send(UpstreamObservation::Ready);
            ready_tx.send(()).expect("fixture readiness");
            loop {
                // finish() wakes this blocking accept with a marked local socket.
                // Every accepted product socket gets its own bounded reader, so
                // a cancelled or paused request cannot hide another attempt.
                let (stream, _) = listener.accept().expect("accept upstream connection");
                sequence += 1;
                readers.push(spawn_header_timeout_reader(
                    stream,
                    sequence,
                    behavior.clone(),
                    request_tx.clone(),
                    event_tx.clone(),
                    Arc::clone(&worker_gate),
                    panic_reader == Some(sequence),
                ));
                if stop_rx.try_recv().is_ok() {
                    break;
                }
            }

            // Callers finish only after the gateway and its timed-out send
            // futures have ended. Drain established connections already queued
            // in the listener before closing it; no quiet-period sleep or
            // guessed equality with persisted attempt counts is needed.
            listener
                .set_nonblocking(true)
                .expect("drain upstream listener");
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        sequence += 1;
                        readers.push(spawn_header_timeout_reader(
                            stream,
                            sequence,
                            behavior.clone(),
                            request_tx.clone(),
                            event_tx.clone(),
                            Arc::clone(&worker_gate),
                            panic_reader == Some(sequence),
                        ));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(err) => panic!("drain upstream connection: {err}"),
                }
            }
            drop(listener);
            let _ = event_tx.send(UpstreamObservation::AcceptClosed);
            // A completed reader's JoinHandle retains its silent TcpStream.
            // Keep every such socket alive until collection has fully settled.
            let mut silent_connections = Vec::new();
            let mut failures = Vec::new();
            for (index, reader) in readers.into_iter().enumerate() {
                match reader.join() {
                    Ok(Some(stream)) => silent_connections.push(stream),
                    Ok(None) => {}
                    Err(_) => {
                        let sequence = index + 1;
                        failures.push(format!("upstream reader {sequence} panicked"));
                        // Reporting a reader failure must not skip the remaining
                        // joins when a test has already dropped its observer.
                        let _ = event_tx.send(UpstreamObservation::ReaderPanicked(sequence));
                    }
                }
            }
            // Closing the last producer is the exact capture-completion event.
            drop(request_tx);
            drop(silent_connections);
            drop(event_tx);
            let result = if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("; "))
            };
            let _ = finished_tx.send(result);
        });
        ready
            .recv_timeout(FIXTURE_WAIT)
            .expect("upstream fixture ready");
        Self {
            addr,
            requests,
            events: Some(events),
            stop,
            finished,
            capture_gate,
            force_healthy,
            join: Some(join),
        }
    }

    fn finish_receiving(&mut self) -> Result<(), String> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let stop = self.stop.send(());
        // The actor may already have seen stop while handling a queued product
        // connection and closed the listener. Connection refusal is then fine.
        let wake = TcpStream::connect_timeout(
            &self.addr.parse().expect("upstream socket address"),
            FIXTURE_WAIT,
        )
        .and_then(|mut wake| {
            wake.set_write_timeout(Some(FIXTURE_WAIT))?;
            wake.write_all(FINISH_WAKE.as_bytes())
        });
        // A failed wake must never lead to an unbounded join while accept is
        // still blocked. Only join after the actor confirms all readers ended.
        match self.finished.recv_timeout(FIXTURE_FINISH_WAIT) {
            Ok(result) => {
                // Both successful and failed acknowledgements mean every
                // reader was joined and every capture producer was dropped.
                join.join()
                    .map_err(|_| "silent-header upstream panicked after completion".to_string())?;
                result
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // The sole completion sender belonged to this actor. A panic
                // drops it during unwind, so reap the actor instead of silently
                // detaching its handle when reporting the missing acknowledgement.
                match join.join() {
                    Ok(()) => Err("silent-header upstream exited without completion".to_string()),
                    Err(_) => Err("silent-header upstream panicked before completion".to_string()),
                }
            }
            Err(error @ mpsc::RecvTimeoutError::Timeout) => {
                // Never join an actor still waiting on a reader. Keep the
                // handle so Drop can release the gate and retry bounded cleanup.
                self.join = Some(join);
                Err(format!(
                    "upstream fixture did not finish: {error}; stop={stop:?}; wake={wake:?}"
                ))
            }
        }
    }

    fn finish_and_collect(mut self) -> Vec<CapturedUpstreamRequest> {
        self.finish_receiving()
            .expect("finish silent-header upstream");
        let mut requests = self.requests.iter().collect::<Vec<_>>();
        // Reader scheduling must not reorder the primary/secondary chronology.
        requests.sort_by_key(|(sequence, _)| *sequence);
        if let Some(events) = self.events.take() {
            eprintln!(
                "silent-header upstream observations: {:?}",
                events.iter().collect::<Vec<_>>()
            );
        }
        requests.into_iter().map(|(_, request)| request).collect()
    }
}

impl Drop for HeaderTimeoutUpstream {
    fn drop(&mut self) {
        self.capture_gate.release();
        if self.join.is_some() {
            if let Err(error) = self.finish_receiving() {
                // Cleanup must never cause a second panic while a failed test
                // unwinds. Ignore a stderr write failure for the same reason.
                let _ = writeln!(std::io::stderr().lock(), "{error}");
            }
        }
    }
}

fn spawn_header_timeout_reader(
    mut stream: TcpStream,
    sequence: usize,
    behavior: HeaderTimeoutBehavior,
    request_tx: mpsc::Sender<(usize, CapturedUpstreamRequest)>,
    events: mpsc::Sender<UpstreamObservation>,
    capture_gate: Arc<CaptureGate>,
    panic_on_capture: bool,
) -> thread::JoinHandle<Option<TcpStream>> {
    let _ = events.send(UpstreamObservation::Accepted(sequence));
    thread::spawn(move || {
        stream
            .set_nonblocking(false)
            .expect("blocking upstream reader");
        stream
            .set_read_timeout(Some(FIXTURE_WAIT))
            .expect("fixture read timeout");
        stream
            .set_write_timeout(Some(FIXTURE_WAIT))
            .expect("fixture write timeout");
        let captured = match read_header_timeout_request(&mut stream, sequence, &events) {
            Ok(Some(captured)) => captured,
            Ok(None) => return None,
            Err(error) => {
                let _ = events.send(UpstreamObservation::Incomplete(sequence, error.to_string()));
                return None;
            }
        };
        let healthy = behavior.force_healthy.load(Ordering::Acquire)
            || behavior
                .healthy_token
                .as_ref()
                .is_some_and(|token| captured.headers.get("authorization") == Some(token));
        assert!(!panic_on_capture, "injected upstream reader panic");
        capture_gate.wait();
        request_tx
            .send((sequence, captured))
            .expect("capture upstream request");
        let _ = events.send(UpstreamObservation::CaptureDelivered(sequence));
        if healthy {
            let body = concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"healthy secondary answer\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_header_timeout_ok\",\"status\":\"completed\",\"model\":\"gpt-5.3-codex\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"healthy secondary answer\"}]}],\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n",
                "data: [DONE]\n\n"
            );
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(header.as_bytes())
                .expect("write upstream status");
            stream
                .write_all(body.as_bytes())
                .expect("write upstream SSE");
            stream.flush().expect("flush healthy upstream");
            None
        } else if behavior.close_after_body {
            // The entire POST has reached the provider. Closing without any
            // response headers leaves its inference/billing outcome unknown.
            let _ = events.send(UpstreamObservation::ClosedAfterBody(sequence));
            None
        } else {
            Some(stream)
        }
    })
}

// The shared raw HTTP helper is private and does not expose parsing stages.
// This small Content-Length reader follows the same contract, adding events
// needed to tell cancellation before headers/body apart from delayed capture.
fn read_header_timeout_request(
    stream: &mut TcpStream,
    sequence: usize,
    events: &mpsc::Sender<UpstreamObservation>,
) -> std::io::Result<Option<CapturedUpstreamRequest>> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        let _ = events.send(UpstreamObservation::ClosedBeforeHeaders(sequence));
        return Ok(None);
    }
    if line == FINISH_WAKE {
        let _ = events.send(UpstreamObservation::ControlWake(sequence));
        return Ok(None);
    }
    let path = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request path")
        })?
        .to_string();
    let mut headers = HashMap::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "cancelled before complete headers",
            ));
        }
        if line == "\r\n" {
            break;
        }
        let (name, value) = line.split_once(':').ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid request header")
        })?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let _ = events.send(UpstreamObservation::HeadersRead(sequence));
    let content_length = headers
        .get("content-length")
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "fixture expects Content-Length",
            )
        })?
        .parse::<usize>()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).map_err(|error| {
        std::io::Error::new(error.kind(), format!("request body incomplete: {error}"))
    })?;
    let _ = events.send(UpstreamObservation::BodyRead(sequence));
    Ok(Some(CapturedUpstreamRequest {
        path,
        headers,
        body,
    }))
}

#[test]
fn header_timeout_fixture_collects_every_request_after_delayed_observation() {
    let mut upstream = HeaderTimeoutUpstream::start_with_capture_gate(None, false);
    let events = upstream.events.take().expect("fixture events");
    let release = ReleaseCaptureOnDrop(Arc::clone(&upstream.capture_gate));
    assert_eq!(
        events.recv_timeout(FIXTURE_WAIT).expect("ready event"),
        UpstreamObservation::Ready
    );
    let mut clients = Vec::new();
    for index in 0..2 {
        let mut stream = TcpStream::connect(&upstream.addr).expect("fixture regression connection");
        let body = format!("request-{index}");
        let request = format!(
            "POST /fixture-{index} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer fixture-{index}\r\nContent-Length: {}\r\n\r\n{body}",
            upstream.addr, body.len()
        );
        stream
            .write_all(request.as_bytes())
            .expect("write fixture request");
        clients.push(stream);
    }
    let mut body_connections = Vec::new();
    let mut observations = Vec::new();
    while body_connections.len() < 2 {
        let event = events
            .recv_timeout(FIXTURE_WAIT)
            .expect("request body read event");
        if let UpstreamObservation::BodyRead(connection) = event {
            body_connections.push(connection);
        }
        observations.push(event);
    }
    for connection in &body_connections {
        assert!(observations.contains(&UpstreamObservation::Accepted(*connection)));
        assert!(observations.contains(&UpstreamObservation::HeadersRead(*connection)));
    }
    // Both requests have reached the fixture in full. The original try_iter
    // collection nevertheless reports an empty set while observation is paused.
    assert_eq!(upstream.requests.try_iter().count(), 0);
    let (collected_tx, collected) = mpsc::channel();
    let collector = thread::spawn(move || {
        collected_tx
            .send(upstream.finish_and_collect())
            .expect("collection result");
    });
    loop {
        if events
            .recv_timeout(FIXTURE_WAIT)
            .expect("accept boundary event")
            == UpstreamObservation::AcceptClosed
        {
            break;
        }
    }
    // The listener has closed, but collection cannot finish before the two
    // pending captures are delivered. This check follows an event, not a sleep.
    assert!(matches!(
        collected.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.0.release();
    let requests = collected
        .recv_timeout(FIXTURE_WAIT)
        .expect("complete capture collection");
    collector.join().expect("join capture collector");
    assert_eq!(requests.len(), 2);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.path, format!("/fixture-{index}"));
        assert_eq!(
            request.headers.get("authorization"),
            Some(&format!("Bearer fixture-{index}"))
        );
        assert_eq!(request.body, format!("request-{index}").as_bytes());
    }
}

#[test]
fn header_timeout_fixture_distinguishes_cancelled_and_complete_requests() {
    let mut upstream = HeaderTimeoutUpstream::start(None);
    let events = upstream.events.take().expect("fixture events");
    assert_eq!(
        events.recv_timeout(FIXTURE_WAIT).expect("ready event"),
        UpstreamObservation::Ready
    );
    let requests = [
        "",
        "POST /partial-headers HTTP/1.1\r\nHost: fixture",
        "POST /short-body HTTP/1.1\r\nContent-Length: 8\r\n\r\nab",
        "POST /complete HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc",
    ];
    for (index, request) in requests.iter().enumerate() {
        let connection = index + 1;
        let mut client = TcpStream::connect(&upstream.addr).expect("cancellation case connection");
        client
            .write_all(request.as_bytes())
            .expect("send cancellation case");
        drop(client);
        let mut observations = Vec::new();
        let terminal = loop {
            let event = events
                .recv_timeout(FIXTURE_WAIT)
                .expect("cancellation case event");
            let terminal = matches!(&event,
                UpstreamObservation::ClosedBeforeHeaders(id)
                | UpstreamObservation::Incomplete(id, _)
                | UpstreamObservation::CaptureDelivered(id) if *id == connection);
            observations.push(event.clone());
            if terminal {
                break event;
            }
        };
        assert!(observations.contains(&UpstreamObservation::Accepted(connection)));
        assert_eq!(
            observations.contains(&UpstreamObservation::HeadersRead(connection)),
            index >= 2
        );
        assert_eq!(
            observations.contains(&UpstreamObservation::BodyRead(connection)),
            index == 3
        );
        match index {
            0 => assert_eq!(
                terminal,
                UpstreamObservation::ClosedBeforeHeaders(connection)
            ),
            1 => assert!(
                matches!(terminal, UpstreamObservation::Incomplete(_, detail) if detail.contains("headers"))
            ),
            2 => assert!(
                matches!(terminal, UpstreamObservation::Incomplete(_, detail) if detail.contains("body incomplete"))
            ),
            3 => assert_eq!(terminal, UpstreamObservation::CaptureDelivered(connection)),
            _ => unreachable!(),
        }
    }
    let captures = upstream.finish_and_collect();
    assert_eq!(
        captures.len(),
        1,
        "cancelled requests must not become full captures"
    );
    assert_eq!(captures[0].path, "/complete");
    assert_eq!(captures[0].body, b"abc");
    assert!(events
        .iter()
        .any(|event| event == UpstreamObservation::AcceptClosed));
}

#[test]
fn header_timeout_fixture_finishes_without_product_connections() {
    let mut upstream = HeaderTimeoutUpstream::start(None);
    let events = upstream.events.take().expect("fixture events");
    assert!(upstream.finish_and_collect().is_empty());
    let observations = events.iter().collect::<Vec<_>>();
    assert!(observations.contains(&UpstreamObservation::Ready));
    assert!(observations.contains(&UpstreamObservation::AcceptClosed));
    assert!(observations
        .iter()
        .any(|event| matches!(event, UpstreamObservation::ControlWake(_))));
    assert!(!observations.iter().any(|event| matches!(
        event,
        UpstreamObservation::HeadersRead(_)
            | UpstreamObservation::BodyRead(_)
            | UpstreamObservation::CaptureDelivered(_)
    )));
}

#[test]
fn header_timeout_fixture_release_and_drop_recover_from_poison() {
    let upstream = HeaderTimeoutUpstream::start(None);
    let gate = Arc::clone(&upstream.capture_gate);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _held = gate.open.lock().unwrap();
        panic!("injected capture gate poison");
    }));
    assert!(poisoned.is_err());
    assert!(gate.open.is_poisoned());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| gate.release())).is_ok());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| gate.wait())).is_ok());
    // release intentionally does not clear poison: Drop must independently
    // recover the same mutex instead of introducing a cleanup panic.
    assert!(gate.open.is_poisoned());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(upstream))).is_ok());
}

#[test]
fn header_timeout_fixture_joins_surviving_readers_before_reporting_a_panic() {
    let mut upstream = HeaderTimeoutUpstream::start_with_reader_panic(None, false, Some(1));
    let events = upstream.events.take().expect("fixture events");
    let release = ReleaseCaptureOnDrop(Arc::clone(&upstream.capture_gate));
    assert_eq!(
        events.recv_timeout(FIXTURE_WAIT).expect("ready event"),
        UpstreamObservation::Ready
    );
    let mut clients = Vec::new();
    for path in ["/injected-panic", "/surviving-reader"] {
        let mut client = TcpStream::connect(&upstream.addr).expect("panic case connection");
        client
            .write_all(format!("POST {path} HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc").as_bytes())
            .expect("send panic case request");
        clients.push(client);
    }
    let mut body_connections = Vec::new();
    while body_connections.len() < 2 {
        if let UpstreamObservation::BodyRead(sequence) = events
            .recv_timeout(FIXTURE_WAIT)
            .expect("panic case body read event")
        {
            body_connections.push(sequence);
        }
    }
    let (finished_tx, finished) = mpsc::channel();
    let (captures_tx, captures) = mpsc::channel();
    let collector = thread::spawn(move || {
        // Report completion before draining captures, so the test cannot hide
        // an early actor return behind a still-live capture sender.
        let result = upstream.finish_receiving();
        finished_tx.send(result).expect("panic case finish result");
        captures_tx
            .send(upstream.requests.iter().collect::<Vec<_>>())
            .expect("panic case captures");
    });
    loop {
        if events
            .recv_timeout(FIXTURE_WAIT)
            .expect("reader panic observation")
            == UpstreamObservation::ReaderPanicked(1)
        {
            break;
        }
    }
    assert!(matches!(
        finished.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.0.release();
    assert_eq!(
        finished
            .recv_timeout(FIXTURE_WAIT)
            .expect("all readers finished")
            .expect_err("injected reader panic must remain a fixture failure"),
        "upstream reader 1 panicked"
    );
    let captures = captures
        .recv_timeout(FIXTURE_WAIT)
        .expect("surviving reader capture");
    collector.join().expect("join panic case collector");
    assert_eq!(captures.len(), 1);
    assert_eq!(captures[0].0, 2);
    assert_eq!(captures[0].1.path, "/surviving-reader");
    assert_eq!(captures[0].1.body, b"abc");
    assert!(events
        .iter()
        .any(|event| event == UpstreamObservation::CaptureDelivered(2)));
}

// The proxy matcher checks presence, so assigning REQUEST_METHOD="" would
// still disable proxies. Preserve the original OS string while removing it.
struct RemovedProxyCgiEnvironment(Option<std::ffi::OsString>);

impl RemovedProxyCgiEnvironment {
    fn enter() -> Self {
        let original = std::env::var_os("REQUEST_METHOD");
        std::env::remove_var("REQUEST_METHOD");
        Self(original)
    }
}

impl Drop for RemovedProxyCgiEnvironment {
    fn drop(&mut self) {
        match &self.0 {
            Some(value) => std::env::set_var("REQUEST_METHOD", value),
            None => std::env::remove_var("REQUEST_METHOD"),
        }
    }
}

// Reverse restoration also handles uppercase/lowercase aliases on Windows.
struct ControlledProxyEnvironment(Vec<EnvGuard>);

impl Drop for ControlledProxyEnvironment {
    fn drop(&mut self) {
        while let Some(value) = self.0.pop() {
            drop(value);
        }
    }
}

#[test]
fn gateway_fixture_proxy_guard_bypasses_host_proxy_but_preserves_explicit_proxy() {
    let _lock = test_env_guard();
    let original_request_method = std::env::var_os("REQUEST_METHOD");
    let cgi = RemovedProxyCgiEnvironment::enter();
    assert!(std::env::var_os("REQUEST_METHOD").is_none());
    let names = [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ];
    let originals = names.map(|name| (name, std::env::var_os(name)));
    let proxy = bind_test_listener("controlled silent host proxy");
    proxy
        .set_nonblocking(true)
        .expect("nonblocking proxy probe");
    let proxy_url = format!("http://{}", proxy.local_addr().expect("proxy address"));
    let mut host_proxy = ControlledProxyEnvironment(Vec::new());
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        host_proxy.0.push(EnvGuard::set(name, &proxy_url));
    }
    // A nonmatching value also prevents Windows ProxyOverride from filling an
    // empty bypass list, so this control does not depend on the host settings.
    host_proxy
        .0
        .push(EnvGuard::set("NO_PROXY", "unrelated.invalid"));
    host_proxy
        .0
        .push(EnvGuard::set("no_proxy", "unrelated.invalid"));

    let destination = "http://127.0.0.1:1/".parse().expect("loopback URI");
    assert!(hyper_util::client::proxy::matcher::Matcher::from_system()
        .intercept(&destination)
        .is_some());
    let upstream = HeaderTimeoutUpstream::start(Some("proxy_guard_fixture"));
    let upstream_url = format!("http://{}/backend-api/codex/responses", upstream.addr);
    let send = |explicit_proxy: Option<&str>| {
        codexmanager_service::process_runtime()
            .expect("service runtime")
            .block_on(async {
                let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(2));
                if let Some(proxy_url) = explicit_proxy {
                    builder = builder
                        .proxy(reqwest::Proxy::all(proxy_url).expect("local explicit proxy"));
                }
                let response = builder
                    .build()?
                    .post(&upstream_url)
                    .header("Authorization", "Bearer access_proxy_guard_fixture")
                    .body(r#"{"model":"gpt-5.3-codex","input":"proxy isolation","stream":true}"#)
                    .send()
                    .await?;
                let status = response.status();
                response.text().await.map(|body| (status, body))
            })
    };

    // Negative control: a default client really reaches our silent local proxy.
    let proxied =
        send(None).expect_err("silent local proxy must reach the bounded request timeout");
    assert!(proxied.is_timeout(), "{proxied}");
    let (proxied_connection, _) = proxy.accept().expect("default client reached local proxy");
    drop(proxied_connection);

    // Re-enter the same guard used by test_env_guard, under its existing lock.
    let loopback = super::support::LoopbackProxyGuard::enter();
    assert!(!hyper_util::client::proxy::matcher::Matcher::from_system()
        .intercept(&destination)
        .is_some());
    let (status, body) = send(None).expect("loopback bypasses the silent host proxy");
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(body.contains("healthy secondary answer"), "{body}");
    assert!(matches!(proxy.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));

    // Explicit Proxy::all remains authoritative even while NO_PROXY lists loopback.
    let explicit = send(Some(&proxy_url)).expect_err("explicit local proxy must still be used");
    assert!(explicit.is_timeout(), "{explicit}");
    let (explicit_connection, _) = proxy.accept().expect("explicit proxy was not bypassed");
    drop(explicit_connection);
    let requests = upstream.finish_and_collect();
    assert_eq!(
        requests.len(),
        1,
        "only the direct request reaches the upstream"
    );

    drop(loopback);
    assert!(std::env::var_os("NO_PROXY") == Some("unrelated.invalid".into()));
    assert!(std::env::var_os("no_proxy") == Some("unrelated.invalid".into()));
    drop(host_proxy);
    drop(cgi);
    assert!(std::env::var_os("REQUEST_METHOD") == original_request_method);
    for (name, original) in originals {
        assert!(
            std::env::var_os(name) == original,
            "proxy environment {name} restored"
        );
    }
}

fn seed_accounts(storage: &Storage, account_ids: &[&str], key_id: &str) {
    let now = now_ts();
    seed_model_catalog_models(storage, &["gpt-5.3-codex"]);
    for (sort, id) in account_ids.iter().enumerate() {
        storage
            .insert_account(&Account {
                id: (*id).to_string(),
                label: (*id).to_string(),
                issuer: "https://auth.openai.com".to_string(),
                chatgpt_account_id: Some(format!("chatgpt_{id}")),
                workspace_id: None,
                group_name: None,
                sort: sort as i64,
                status: "active".to_string(),
                created_at: now + sort as i64,
                updated_at: now + sort as i64,
            })
            .expect("insert account");
        storage
            .insert_token(&Token {
                account_id: (*id).to_string(),
                id_token: String::new(),
                access_token: format!("access_{id}"),
                refresh_token: String::new(),
                api_key_access_token: Some(format!("api_access_{id}")),
                last_refresh: now,
            })
            .expect("insert token");
        storage
            .insert_usage_snapshot(&UsageSnapshotRecord {
                account_id: (*id).to_string(),
                used_percent: Some(10.0),
                window_minutes: Some(300),
                resets_at: None,
                secondary_used_percent: None,
                secondary_window_minutes: None,
                secondary_resets_at: None,
                credits_json: None,
                captured_at: now,
            })
            .expect("insert usage snapshot");
    }
    storage
        .insert_api_key(&ApiKey {
            id: key_id.to_string(),
            name: Some(key_id.to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: None,
            service_tier: None,
            rotation_strategy: "account_rotation".to_string(),
            aggregate_api_id: None,
            account_plan_filter: None,
            aggregate_api_url: None,
            client_type: "codex".to_string(),
            protocol_type: "openai_compat".to_string(),
            auth_scheme: "authorization_bearer".to_string(),
            upstream_base_url: None,
            static_headers_json: None,
            key_hash: hash_platform_key_for_test(PLATFORM_KEY),
            status: "active".to_string(),
            created_at: now,
            last_used_at: None,
        })
        .expect("insert API key");
}

fn post_stream(addr: &str) -> (u16, String) {
    post_http_raw_with_read_timeout(
        addr,
        "/v1/responses",
        r#"{"model":"gpt-5.3-codex","input":"hello","stream":true}"#,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {PLATFORM_KEY}")),
        ],
        Duration::from_secs(5),
    )
}

fn report_header_timeout_context(
    storage: &Storage,
    db_path: &std::path::Path,
    upstream_addr: &str,
    key_id: &str,
    elapsed: Duration,
) {
    fn safe_url(raw: &str) -> String {
        let Ok(mut parsed) = url::Url::parse(raw) else {
            return "<invalid URL>".to_string();
        };
        let _ = parsed.set_username("");
        let _ = parsed.set_password(None);
        parsed.set_query(None);
        parsed.set_fragment(None);
        parsed.to_string()
    }
    let proxy_environment = [
        "CODEXMANAGER_UPSTREAM_PROXY_URL",
        "CODEXMANAGER_PROXY_LIST",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ]
    .map(|name| {
        (
            name,
            std::env::var_os(name).is_some_and(|value| !value.is_empty()),
        )
    });
    eprintln!(
        "header-timeout context: db_path={} expected_upstream=http://{}/backend-api/codex/responses elapsed={elapsed:?} stream_timeout_ms={} total_timeout_ms={} proxy_environment={proxy_environment:?}",
        db_path.display(), upstream_addr,
        codexmanager_service::current_gateway_upstream_stream_timeout_ms(),
        codexmanager_service::current_gateway_upstream_total_timeout_ms(),
    );
    for log in storage
        .list_request_logs(Some(&format!("key:={key_id}")), 10)
        .expect("diagnostic request logs")
    {
        eprintln!(
            "header-timeout request: trace={:?} account={:?} initial_account={:?} attempted_accounts={:?} status={:?} duration_ms={:?} upstream={:?} header_timeout={} downstream_cancelled={}",
            log.trace_id, log.account_id, log.initial_account_id, log.attempted_account_ids_json,
            log.status_code, log.duration_ms, log.upstream_url.as_deref().map(safe_url),
            log.error.as_deref().is_some_and(|error| error.contains("response headers timed out")),
            log.error.as_deref().is_some_and(|error| error.contains("downstream request cancelled")),
        );
        if let Some(trace_id) = log.trace_id.as_deref() {
            let stages = storage
                .list_request_log_upstream_attempt_stages(trace_id)
                .expect("diagnostic attempt stages");
            eprintln!("header-timeout attempt stages: {stages:?}");
            for stage in stages {
                if let Some(attempt) = storage
                    .find_request_log_upstream_attempt(trace_id, &stage)
                    .expect("diagnostic upstream attempt")
                {
                    eprintln!(
                        "header-timeout attempt: stage={} method={} url={} transport={} encoding={:?} wire_sha256={} created_at={}",
                        attempt.stage, attempt.method, safe_url(&attempt.url), attempt.transport,
                        attempt.content_encoding, attempt.wire_sha256, attempt.created_at,
                    );
                }
            }
        }
    }
}

fn assert_recorded_attempts_match_requests(
    storage: &Storage,
    trace_id: &str,
    requests: &[CapturedUpstreamRequest],
    max_attempts: usize,
) {
    assert!(!trace_id.is_empty());
    let attempts = wait_for_upstream_attempts(storage, trace_id, requests.len());
    // A send can reach its deadline before the mock HTTP parser presents it.
    // Every observed request must be captured, while unobserved timed-out sends
    // can legitimately add records to the same trace.
    assert!(attempts.len() >= requests.len());
    assert!(attempts.len() <= max_attempts, "{attempts:?}");
    for attempt in &attempts {
        assert_eq!(attempt.trace_id, trace_id);
        assert_eq!(attempt.method, "POST");
    }
    for request in requests {
        let wire_sha256 = format!("{:x}", Sha256::digest(&request.body));
        assert!(
            attempts
                .iter()
                .any(|attempt| attempt.wire_sha256 == wire_sha256),
            "persisted attempts must include the observed HTTP request bytes"
        );
    }
}

#[test]
fn gateway_silent_response_headers_do_not_replay_to_a_healthy_account() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-header-timeout-failover");
    let db_path = dir.join("codexmanager.db");
    let _db = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _route = EnvGuard::set("CODEXMANAGER_ROUTE_STRATEGY", "ordered");
    // Ordered routing still permits health-based selection within its leading
    // window. Disable that choice so this test always reaches the silent primary.
    let _health = EnvGuard::set("CODEXMANAGER_ROUTE_HEALTH_P2C_ENABLED", "0");
    let _total = EnvGuard::set("CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS", "0");
    let _stream = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_STREAM_TIMEOUT_MS",
        &STREAM_TIMEOUT_MS.to_string(),
    );
    let upstream = HeaderTimeoutUpstream::start(Some(SECONDARY));
    let _base = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_BASE_URL",
        &format!("http://{}/backend-api/codex", upstream.addr),
    );
    let storage = Storage::open(&db_path).expect("open db");
    seed_accounts(&storage, &[PRIMARY, SECONDARY], KEY_ID);
    let server = codexmanager_service::start_one_shot_server().expect("start gateway");
    let started = Instant::now();
    let (status, body) = post_stream(&server.addr);
    let elapsed = started.elapsed();
    server.join();

    report_header_timeout_context(&storage, &db_path, &upstream.addr, KEY_ID, elapsed);
    assert_eq!(status, 502, "gateway response: {body}");
    assert!(body.contains("automatic replay stopped"), "{body}");
    assert!(elapsed < Duration::from_secs(5), "elapsed: {elapsed:?}");
    let requests = upstream.finish_and_collect();
    // Both accounts are available and the secondary would answer immediately.
    // A timeout after a complete POST cannot prove the primary did not start
    // inference, so neither a fresh-client retry nor account failover is safe.
    assert_eq!(
        requests.len(),
        1,
        "ambiguous request was replayed: {requests:?}"
    );
    assert_eq!(
        requests[0].headers.get("authorization"),
        Some(&format!("Bearer access_{PRIMARY}"))
    );
    assert_eq!(requests[0].path, "/backend-api/codex/responses");
    assert!(elapsed >= Duration::from_millis(STREAM_TIMEOUT_MS));
    let logs = storage
        .list_request_logs(Some(&format!("key:={KEY_ID}")), 10)
        .expect("read final request log");
    assert_eq!(logs.len(), 1, "{logs:?}");
    let log = &logs[0];
    assert_eq!(log.status_code, Some(502));
    assert_eq!(log.initial_account_id.as_deref(), Some(PRIMARY));
    assert_eq!(log.account_id.as_deref(), Some(PRIMARY));
    assert_eq!(
        serde_json::from_str::<Vec<String>>(
            log.attempted_account_ids_json
                .as_deref()
                .expect("attempted accounts")
        )
        .expect("decode attempted accounts"),
        vec![PRIMARY]
    );
    let trace_id = log.trace_id.as_deref().expect("request trace id");
    assert_recorded_attempts_match_requests(&storage, trace_id, &requests, 1);
}

#[test]
fn gateway_single_silent_account_still_obeys_stream_header_timeout() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-single-header-timeout");
    let db_path = dir.join("codexmanager.db");
    let _db = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _route = EnvGuard::set("CODEXMANAGER_ROUTE_STRATEGY", "ordered");
    let _health = EnvGuard::set("CODEXMANAGER_ROUTE_HEALTH_P2C_ENABLED", "0");
    let _total = EnvGuard::set("CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS", "0");
    let _stream = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_STREAM_TIMEOUT_MS",
        &STREAM_TIMEOUT_MS.to_string(),
    );
    let upstream = HeaderTimeoutUpstream::start(None);
    let _base = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_BASE_URL",
        &format!("http://{}/backend-api/codex", upstream.addr),
    );
    let storage = Storage::open(&db_path).expect("open db");
    seed_accounts(
        &storage,
        &["acc_header_timeout_only"],
        "gk_single_header_timeout",
    );
    let server = codexmanager_service::start_one_shot_server().expect("start gateway");
    let started = Instant::now();
    let (status, body) = post_stream(&server.addr);
    let elapsed = started.elapsed();
    server.join();

    report_header_timeout_context(
        &storage,
        &db_path,
        &upstream.addr,
        "gk_single_header_timeout",
        elapsed,
    );
    assert_eq!(status, 502, "gateway response: {body}");
    assert!(body.contains("automatic replay stopped"), "{body}");
    assert!(elapsed < Duration::from_secs(5), "elapsed: {elapsed:?}");
    let requests = upstream.finish_and_collect();
    assert_eq!(
        requests.len(),
        1,
        "ambiguous request was replayed: {requests:?}"
    );
    assert!(elapsed >= Duration::from_millis(STREAM_TIMEOUT_MS));
    assert!(requests.iter().all(|request| {
        request.headers.get("authorization").map(String::as_str)
            == Some("Bearer access_acc_header_timeout_only")
    }));
    let logs = storage
        .list_request_logs(Some("key:=gk_single_header_timeout"), 10)
        .expect("read timeout request log");
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_eq!(logs[0].status_code, Some(502));
    let trace_id = logs[0]
        .trace_id
        .as_deref()
        .expect("timeout request trace id");
    assert_recorded_attempts_match_requests(&storage, trace_id, &requests, 1);
}

fn post_long_inference(addr: &str, path: &str, stream: bool) -> (u16, String) {
    let context = format!(
        "{}REPLAY_GUARD_CONTEXT_END",
        "long conversation context ".repeat(2048)
    );
    let mut request = serde_json::json!({
        "model": "gpt-5.3-codex",
        "stream": stream,
    });
    if path == "/v1/chat/completions" {
        request["messages"] = serde_json::json!([{"role": "user", "content": context}]);
        request["tools"] = serde_json::json!([{
            "type": "function",
            "function": {
                "name": "capture_probe",
                "parameters": {"type": "object", "properties": {}}
            }
        }]);
    } else {
        request["input"] = serde_json::json!([{"role": "user", "content": context}]);
        request["tools"] = serde_json::json!([{
            "type": "function",
            "name": "capture_probe",
            "parameters": {"type": "object", "properties": {}}
        }]);
    }
    post_http_raw_with_read_timeout(
        addr,
        path,
        &request.to_string(),
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {PLATFORM_KEY}")),
        ],
        FIXTURE_WAIT,
    )
}

fn assert_captured_account(request: &CapturedUpstreamRequest, account: &str) {
    assert_eq!(
        request.headers.get("authorization"),
        Some(&format!("Bearer access_{account}"))
    );
    assert_eq!(
        request.headers.get("chatgpt-account-id"),
        Some(&format!("chatgpt_{account}"))
    );
    assert_eq!(request.path, "/backend-api/codex/responses");
}

fn assert_complete_post_without_headers_is_not_replayed(path: &str, stream: bool, case: &str) {
    let _lock = test_env_guard();
    let dir = new_test_dir(&format!("codexmanager-no-header-replay-{case}"));
    let db_path = dir.join("codexmanager.db");
    let _db = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _route = EnvGuard::set("CODEXMANAGER_ROUTE_STRATEGY", "ordered");
    let _health = EnvGuard::set("CODEXMANAGER_ROUTE_HEALTH_P2C_ENABLED", "0");
    let _total = EnvGuard::set("CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS", "0");
    let _stream = EnvGuard::set("CODEXMANAGER_UPSTREAM_STREAM_TIMEOUT_MS", "2000");
    let primary = format!("acc_no_headers_{case}_primary");
    let secondary = format!("acc_no_headers_{case}_secondary");
    let key_id = format!("gk_no_headers_{case}");
    let mut upstream = HeaderTimeoutUpstream::start_closing_after_body(Some(&secondary));
    let events = upstream.events.take().expect("no-header upstream events");
    let _base = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_BASE_URL",
        &format!("http://{}/backend-api/codex", upstream.addr),
    );
    let storage = Storage::open(&db_path).expect("open db");
    seed_accounts(&storage, &[&primary, &secondary], &key_id);
    let server = codexmanager_service::start_one_shot_server().expect("start gateway");
    let (status, body) = post_long_inference(&server.addr, path, stream);
    server.join();
    let requests = upstream.finish_and_collect();
    let observations = events.iter().collect::<Vec<_>>();

    assert_eq!(status, 502, "{case} gateway response: {body}");
    assert!(
        body.contains("automatic replay stopped"),
        "{case}: ambiguous delivery must explain the replay stop: {body}"
    );
    assert!(observations.contains(&UpstreamObservation::BodyRead(1)));
    assert!(observations.contains(&UpstreamObservation::ClosedAfterBody(1)));
    assert_eq!(
        requests.len(),
        1,
        "{case}: complete POST must not be resent to either account: {requests:?}"
    );
    assert_captured_account(&requests[0], &primary);
    let decoded = decode_upstream_request_body(&requests[0]);
    let decoded = std::str::from_utf8(&decoded).expect("decoded JSON request");
    assert!(decoded.contains("REPLAY_GUARD_CONTEXT_END"));
    assert!(decoded.contains("capture_probe"));
    assert!(
        decoded.len() > 48_000,
        "fixture must receive the full long context"
    );
    let logs = storage
        .list_request_logs(Some(&format!("key:={key_id}")), 10)
        .expect("read no-header request log");
    assert_eq!(logs.len(), 1, "{logs:?}");
    let log = &logs[0];
    assert_eq!(log.status_code, Some(502));
    assert_eq!(log.initial_account_id.as_deref(), Some(primary.as_str()));
    assert_eq!(log.account_id.as_deref(), Some(primary.as_str()));
    assert_eq!(
        serde_json::from_str::<Vec<String>>(
            log.attempted_account_ids_json
                .as_deref()
                .expect("attempted accounts")
        )
        .expect("decode attempted accounts"),
        vec![primary]
    );
    assert_recorded_attempts_match_requests(
        &storage,
        log.trace_id.as_deref().expect("no-header trace id"),
        &requests,
        1,
    );
}

#[test]
fn gateway_complete_post_without_headers_is_not_replayed_for_responses_stream() {
    assert_complete_post_without_headers_is_not_replayed("/v1/responses", true, "responses_stream");
}

#[test]
fn gateway_complete_post_without_headers_is_not_replayed_for_responses_json() {
    assert_complete_post_without_headers_is_not_replayed("/v1/responses", false, "responses_json");
}

#[test]
fn gateway_complete_post_without_headers_is_not_replayed_for_chat_stream() {
    assert_complete_post_without_headers_is_not_replayed(
        "/v1/chat/completions",
        true,
        "chat_stream",
    );
}

#[test]
fn gateway_complete_post_without_headers_is_not_replayed_for_chat_json() {
    assert_complete_post_without_headers_is_not_replayed(
        "/v1/chat/completions",
        false,
        "chat_json",
    );
}

#[test]
fn gateway_no_header_replay_guard_does_not_leak_into_next_inbound_request() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-no-header-next-inbound");
    let db_path = dir.join("codexmanager.db");
    let _db = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _route = EnvGuard::set("CODEXMANAGER_ROUTE_STRATEGY", "ordered");
    let _health = EnvGuard::set("CODEXMANAGER_ROUTE_HEALTH_P2C_ENABLED", "0");
    let _total = EnvGuard::set("CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS", "0");
    let _stream = EnvGuard::set("CODEXMANAGER_UPSTREAM_STREAM_TIMEOUT_MS", "2000");
    let upstream = HeaderTimeoutUpstream::start_closing_after_body(None);
    let _base = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_BASE_URL",
        &format!("http://{}/backend-api/codex", upstream.addr),
    );
    let storage = Storage::open(&db_path).expect("open db");
    let primary = "acc_no_headers_next_primary";
    let secondary = "acc_no_headers_next_secondary";
    let key_id = "gk_no_headers_next_inbound";
    seed_accounts(&storage, &[primary, secondary], key_id);
    let server = TestServer::start();
    let first = post_long_inference(&server.addr, "/v1/responses", true);
    assert_eq!(first.0, 502, "first inbound: {}", first.1);

    // The client explicitly starts a second request on the same live service.
    // Only this new inbound may submit the body again after the ambiguous EOF.
    upstream.respond_healthy_to_all();
    let second = post_long_inference(&server.addr, "/v1/responses", true);
    assert_eq!(second.0, 200, "second inbound: {}", second.1);
    assert!(
        second.1.contains("healthy secondary answer"),
        "{}",
        second.1
    );
    drop(server);
    let requests = upstream.finish_and_collect();
    assert_eq!(
        requests.len(),
        2,
        "exactly one POST per inbound: {requests:?}"
    );
    assert_captured_account(&requests[0], primary);
    let logs = storage
        .list_request_logs(Some(&format!("key:={key_id}")), 10)
        .expect("read both inbound request logs");
    assert_eq!(logs.len(), 2, "{logs:?}");
    let failed = logs
        .iter()
        .find(|log| log.status_code == Some(502))
        .expect("failed inbound log");
    let success = logs
        .iter()
        .find(|log| log.status_code == Some(200))
        .expect("successful inbound log");
    assert_ne!(failed.trace_id, success.trace_id);
    for (log, request) in [(failed, &requests[0]), (success, &requests[1])] {
        assert_recorded_attempts_match_requests(
            &storage,
            log.trace_id.as_deref().expect("inbound trace id"),
            std::slice::from_ref(request),
            1,
        );
    }
}

#[test]
fn gateway_connection_refused_before_post_can_fail_over_to_a_healthy_account() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-connect-refused-safe-failover");
    let db_path = dir.join("codexmanager.db");
    let _db = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _route = EnvGuard::set("CODEXMANAGER_ROUTE_STRATEGY", "ordered");
    let _health = EnvGuard::set("CODEXMANAGER_ROUTE_HEALTH_P2C_ENABLED", "0");
    let _total = EnvGuard::set("CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS", "0");
    // Windows may wait before reporting a refused loopback proxy connection.
    // The connect deadline must expire before the response-header deadline so
    // this fixture exercises a typed, known-before-send connection failure.
    let _connect = EnvGuard::set("CODEXMANAGER_UPSTREAM_CONNECT_TIMEOUT_SECS", "1");
    let _stream = EnvGuard::set("CODEXMANAGER_UPSTREAM_STREAM_TIMEOUT_MS", "3000");
    let primary = "acc_connect_refused_primary";
    let secondary = "acc_connect_refused_secondary";
    let key_id = "gk_connect_refused";
    let upstream = HeaderTimeoutUpstream::start(Some(secondary));
    let _base = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_BASE_URL",
        &format!("http://{}/backend-api/codex", upstream.addr),
    );
    let storage = Storage::open(&db_path).expect("open db");
    seed_accounts(&storage, &[primary, secondary], key_id);
    let refused_listener = bind_test_listener("refused account proxy port");
    let refused_proxy = format!(
        "http://{}",
        refused_listener.local_addr().expect("proxy address")
    );
    drop(refused_listener);
    storage
        .upsert_account_proxy_settings(
            primary,
            true,
            Some("custom"),
            None,
            Some(&refused_proxy),
            "unchecked",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("route only the primary through a refused proxy connection");
    let server = codexmanager_service::start_one_shot_server().expect("start gateway");
    let (status, body) = post_http_raw_with_read_timeout(
        &server.addr,
        "/v1/responses",
        r#"{"model":"gpt-5.3-codex","input":"hello","stream":true}"#,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {PLATFORM_KEY}")),
        ],
        Duration::from_secs(10),
    );
    server.join();
    let requests = upstream.finish_and_collect();
    assert_eq!(status, 200, "safe connect-refused failover: {body}");
    assert!(body.contains("healthy secondary answer"), "{body}");
    assert_eq!(
        requests.len(),
        1,
        "only the connected account can receive a POST: {requests:?}"
    );
    assert_captured_account(&requests[0], secondary);
    let logs = storage
        .list_request_logs(Some(&format!("key:={key_id}")), 10)
        .expect("read safe failover log");
    assert_eq!(logs.len(), 1, "{logs:?}");
    let log = &logs[0];
    assert_eq!(log.status_code, Some(200));
    assert_eq!(log.initial_account_id.as_deref(), Some(primary));
    assert_eq!(log.account_id.as_deref(), Some(secondary));
    assert_eq!(
        serde_json::from_str::<Vec<String>>(
            log.attempted_account_ids_json
                .as_deref()
                .expect("attempted accounts")
        )
        .expect("decode attempted accounts"),
        vec![primary, secondary]
    );
}
