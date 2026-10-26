use super::{send_stream_request_with_capture, AsyncStreamRequestError, TransportReplaySafety};
use crate::http::gateway_replay_guard::{scope_upstream_replay_guard, UpstreamReplayGuard};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::TcpListener;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

async fn read_complete_post(
    socket: &mut tokio::net::TcpStream,
) -> (String, HashMap<String, String>, Vec<u8>) {
    let mut received = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = socket.read(&mut buffer).await.expect("read POST headers");
        assert!(
            read > 0,
            "provider closed before receiving complete headers"
        );
        received.extend_from_slice(&buffer[..read]);
        if let Some(end) = received.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
        assert!(received.len() < 64 * 1024, "unbounded fixture headers");
    };
    let header_text = std::str::from_utf8(&received[..header_end]).expect("HTTP headers");
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().expect("request line").to_string();
    assert!(request_line.starts_with("POST "), "{request_line}");
    let headers = lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once(':').expect("HTTP header separator");
            (name.to_ascii_lowercase(), value.trim().to_string())
        })
        .collect::<HashMap<_, _>>();
    let length = headers
        .get("content-length")
        .expect("fixture requires a known body length")
        .parse::<usize>()
        .expect("Content-Length");
    assert!(length > 48_000, "regression must submit a full long input");
    while received.len() < header_end + length {
        let read = socket.read(&mut buffer).await.expect("read POST body");
        assert!(read > 0, "provider did not receive the entire POST body");
        received.extend_from_slice(&buffer[..read]);
    }
    assert_eq!(received.len(), header_end + length);
    (request_line, headers, received[header_end..].to_vec())
}

fn assert_no_accepted_connection(listener: &TcpListener, label: &str) {
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    match listener.accept() {
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(error) => panic!("{label} listener failed: {error}"),
        Ok(_) => panic!("{label} unexpectedly received a replay connection"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn direct_transport_task_scope_blocks_replay_to_another_provider_after_complete_post() {
    for path in ["/v1/responses", "/v1/chat/completions"] {
        for is_stream in [false, true] {
            let failed_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind first provider");
            let failed_url = format!("http://{}{path}", failed_listener.local_addr().unwrap());
            let healthy_listener = TcpListener::bind("127.0.0.1:0").expect("bind healthy provider");
            let healthy_url = format!("http://{}{path}", healthy_listener.local_addr().unwrap());
            let provider = tokio::spawn(async move {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(3), failed_listener.accept())
                        .await
                        .expect("first provider accept deadline")
                        .expect("accept first POST");
                let captured =
                    tokio::time::timeout(Duration::from_secs(3), read_complete_post(&mut socket))
                        .await
                        .expect("complete POST read deadline");
                // The complete body could already have started inference. An
                // EOF before any HTTP response does not make replay safe.
                drop(socket);
                (captured, failed_listener)
            });
            let mut payload = serde_json::json!({
                "model": "gpt-5.3-codex",
                "stream": is_stream,
            });
            let context = "long context ".repeat(4096);
            if path == "/v1/chat/completions" {
                payload["messages"] = serde_json::json!([{"role": "user", "content": context}]);
            } else {
                payload["input"] = serde_json::json!([{"role": "user", "content": context}]);
            }
            let body = Bytes::from(payload.to_string());
            let headers = [
                ("Content-Type".to_string(), "application/json".to_string()),
                (
                    "Authorization".to_string(),
                    "Bearer first-provider".to_string(),
                ),
                (
                    "ChatGPT-Account-ID".to_string(),
                    "first-account".to_string(),
                ),
            ];
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let guard = UpstreamReplayGuard::default();
            // Intentionally bypass UpstreamRequestContext and the primary
            // candidate executor, as fallback and aggregate send paths do.
            tokio::time::timeout(
                Duration::from_secs(3),
                scope_upstream_replay_guard(guard.clone(), async {
                    let first = send_stream_request_with_capture(
                        &client,
                        &reqwest::Method::POST,
                        &failed_url,
                        path,
                        Some(Instant::now() + Duration::from_secs(3)),
                        &headers,
                        &body,
                        is_stream,
                        None,
                        None,
                    )
                    .await;
                    let first_error = match first {
                        Err(error) => error,
                        Ok(_) => panic!("provider closed without sending HTTP response headers"),
                    };
                    assert_eq!(
                        first_error.replay_safety(),
                        TransportReplaySafety::Ambiguous
                    );
                    assert!(
                        guard.is_blocked(),
                        "the direct send must block its request scope"
                    );

                    let second = send_stream_request_with_capture(
                        &client,
                        &reqwest::Method::POST,
                        &healthy_url,
                        path,
                        Some(Instant::now() + Duration::from_secs(3)),
                        &headers,
                        &body,
                        is_stream,
                        None,
                        None,
                    )
                    .await;
                    assert!(
                        matches!(second, Err(AsyncStreamRequestError::ReplayBlocked)),
                        "changing the URL cannot replay an ambiguously delivered POST"
                    );
                }),
            )
            .await
            .expect("direct sends must finish after provider EOF");
            let ((request_line, captured_headers, captured_body), failed_listener) =
                provider.await.expect("join first provider");
            assert_eq!(request_line, format!("POST {path} HTTP/1.1"));
            assert_eq!(
                captured_headers.get("authorization").map(String::as_str),
                Some("Bearer first-provider")
            );
            assert_eq!(
                captured_headers
                    .get("chatgpt-account-id")
                    .map(String::as_str),
                Some("first-account")
            );
            assert_eq!(captured_body.as_slice(), body.as_ref());
            assert_eq!(Sha256::digest(&captured_body), Sha256::digest(&body));
            let failed_listener = failed_listener
                .into_std()
                .expect("retain first provider listener");
            assert_no_accepted_connection(&failed_listener, "first provider");
            assert_no_accepted_connection(&healthy_listener, "healthy provider");
        }
    }
}
