use super::*;
use serde_json::{json, Value};

const INTERRUPT: &str =
    " { \"type\" : \"response.interrupt\", \"response_id\" : \"resp_interrupt\", \"mode\" : \"discard_partial_items\" } ";

async fn next_text<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> String
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => return text.to_string(),
                Some(Ok(Message::Ping(payload))) => {
                    socket.send(Message::Pong(payload)).await.unwrap();
                }
                other => panic!("expected websocket text, got {other:?}"),
            }
        }
    })
    .await
    .expect("websocket text timeout")
}

async fn send_json<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>, value: Value)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("send websocket fixture");
}

fn interrupted_terminal() -> Value {
    json!({
        "type": "response.incomplete",
        "response": {
            "id": "resp_interrupt", "status": "incomplete",
            "incomplete_details": {"reason": "interrupted"},
            "output": [{
                "type": "function_call", "id": "fc_interrupt", "call_id": "call_interrupt",
                "name": "echo", "arguments": "{}", "status": "completed"
            }],
            "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}
        }
    })
}

fn tool_follow_up() -> Value {
    json!({
        "type": "response.create", "model": "gpt-6-luna",
        "previous_response_id": "resp_interrupt", "store": false,
        "input": [{"type": "function_call_output", "call_id": "call_interrupt", "output": "ok"}]
    })
}

async fn finish_mock<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(Ok(message)) = socket.next().await {
        if matches!(message, Message::Close(_)) {
            let _ = socket.flush().await;
            break;
        }
    }
}

async fn seed_proxy(
    upstream_addr: &str,
    label: &str,
) -> (
    Storage,
    EnvGuard,
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let db_path = new_test_db_path(label);
    let storage = init_test_storage(&db_path);
    let db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    insert_api_key_record(
        &storage,
        "platform_key_interrupt",
        crate::apikey_profile::ROTATION_ACCOUNT,
        Some(format!(
            "http://{upstream_addr}/chatgpt.com/backend-api/codex"
        )),
    );
    insert_account_and_token(&storage);
    tokio::task::spawn_blocking(crate::gateway::reload_runtime_config_from_env)
        .await
        .unwrap();
    let (addr, shutdown, handle) = start_front_proxy_test_server(ProxyState {
        backend_base_url: "http://127.0.0.1:1".into(),
        client: Client::new(),
    })
    .await;
    (storage, db_guard, addr, shutdown, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn official_responses_websocket_interrupt_preserves_active_session_and_tool_follow_up() {
    let _guard = crate::test_env_guard();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap().to_string();
    let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = oneshot::channel();
    let upstream = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |_: &Request, response: Response| Ok(response))
            .await
            .unwrap();
        frames_tx.send(next_text(&mut socket).await).unwrap();
        send_json(
            &mut socket,
            json!({"type": "response.created", "response": {"id": "resp_interrupt"}}),
        )
        .await;
        frames_tx.send(next_text(&mut socket).await).unwrap();
        send_json(&mut socket, json!({"type": "response.interrupt.accepted", "response_id": "resp_interrupt", "sequence_number": 3})).await;
        release_rx.await.unwrap();
        send_json(&mut socket, interrupted_terminal()).await;
        frames_tx.send(next_text(&mut socket).await).unwrap();
        send_json(&mut socket, json!({"type": "response.completed", "response": {"id": "resp_tool_follow_up", "output": []}})).await;
        frames_tx.send(next_text(&mut socket).await).unwrap();
        send_json(
            &mut socket,
            json!({"type": "response.interrupt.failed", "response_id": "resp_tool_follow_up"}),
        )
        .await;
        frames_tx.send(next_text(&mut socket).await).unwrap();
        send_json(&mut socket, json!({"type": "response.completed", "response": {"id": "resp_after_idle", "output": []}})).await;
        finish_mock(&mut socket).await;
    });
    let (storage, _db_guard, addr, shutdown, server) =
        seed_proxy(&upstream_addr, "ws-interrupt-active").await;
    let (mut client, _) = connect_async(build_ws_request(
        &format!("ws://{addr}/v1/responses"),
        "platform_key_interrupt",
        &[],
    ))
    .await
    .unwrap();
    send_json(
        &mut client,
        json!({"type": "response.create", "model": "gpt-6-luna", "input": "initial prompt"}),
    )
    .await;
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["type"],
        "response.created"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&frames_rx.recv().await.unwrap()).unwrap()["type"],
        "response.create"
    );
    client.send(Message::Text(INTERRUPT.into())).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["type"],
        "response.interrupt.accepted"
    );
    assert_eq!(
        frames_rx.recv().await.unwrap(),
        INTERRUPT,
        "control frame must pass through byte-for-byte without request rewriting"
    );
    // An acknowledgement is not the terminal event. An overlapping create
    // must still be rejected until the interrupted response finishes.
    send_json(
        &mut client,
        json!({"type": "response.create", "model": "gpt-6-luna", "input": "too early"}),
    )
    .await;
    let error: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
    assert_eq!(error["error"]["code"], "response_in_flight");
    assert!(frames_rx.try_recv().is_err());
    release_tx.send(()).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap(),
        interrupted_terminal()
    );
    send_json(&mut client, tool_follow_up()).await;
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["type"],
        "response.completed"
    );
    let follow_up: Value = serde_json::from_str(&frames_rx.recv().await.unwrap()).unwrap();
    assert_eq!(follow_up["previous_response_id"], "resp_interrupt");
    assert_eq!(follow_up["input"][0]["call_id"], "call_interrupt");
    let idle_interrupt = r#"{"type":"response.interrupt","response_id":"resp_tool_follow_up","mode":"discard_partial_items"}"#;
    client
        .send(Message::Text(idle_interrupt.into()))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["type"],
        "response.interrupt.failed"
    );
    assert_eq!(frames_rx.recv().await.unwrap(), idle_interrupt);
    send_json(
        &mut client,
        json!({"type": "response.create", "model": "gpt-6-luna", "input": "after idle interrupt"}),
    )
    .await;
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["response"]["id"],
        "resp_after_idle"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&frames_rx.recv().await.unwrap()).unwrap()["type"],
        "response.create"
    );
    let logs = storage.list_request_logs(None, 20).unwrap();
    assert_eq!(
        logs.len(),
        3,
        "interrupt controls must not create generation logs"
    );
    assert!(logs.iter().all(|log| log.status_code == Some(200)));
    let interrupted = logs
        .iter()
        .find(|log| log.total_tokens == Some(10))
        .expect("interrupted usage remains recorded");
    assert!(interrupted.error.is_none());
    assert_normal_client_close(&mut client).await;
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), upstream)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn official_responses_websocket_interrupt_ignores_unconnected_idle_and_recovers_tool_context()
{
    let _guard = crate::test_env_guard();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap().to_string();
    let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let upstream = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |_: &Request, response: Response| Ok(response))
            .await
            .unwrap();
        let initial: Value = serde_json::from_str(&next_text(&mut socket).await).unwrap();
        assert_eq!(
            initial["type"], "response.create",
            "pre-create interrupt must not establish upstream or become create"
        );
        send_json(
            &mut socket,
            json!({"type": "response.created", "response": {"id": "resp_interrupt"}}),
        )
        .await;
        assert_eq!(next_text(&mut socket).await, INTERRUPT);
        send_json(
            &mut socket,
            json!({"type": "response.interrupt.accepted", "response_id": "resp_interrupt"}),
        )
        .await;
        send_json(&mut socket, interrupted_terminal()).await;
        socket.close(None).await.unwrap();
        drop(socket);
        closed_tx.send(()).unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |_: &Request, response: Response| Ok(response))
            .await
            .unwrap();
        frames_tx.send(next_text(&mut socket).await).unwrap();
        send_json(&mut socket, json!({"type": "response.completed", "response": {"id": "resp_reconnected", "output": []}})).await;
        finish_mock(&mut socket).await;
    });
    let (_storage, _db_guard, addr, shutdown, server) =
        seed_proxy(&upstream_addr, "ws-interrupt-idle").await;
    let (mut client, _) = connect_async(build_ws_request(
        &format!("ws://{addr}/v1/responses"),
        "platform_key_interrupt",
        &[],
    ))
    .await
    .unwrap();
    client
        .send(Message::Text(r#"{"type":"response.interrupt"}"#.into()))
        .await
        .unwrap();
    send_json(&mut client, json!({"type": "response.create", "model": "gpt-6-luna", "input": "initial prompt", "store": false})).await;
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["type"],
        "response.created"
    );
    client.send(Message::Text(INTERRUPT.into())).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["type"],
        "response.interrupt.accepted"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap(),
        interrupted_terminal()
    );
    closed_rx.await.unwrap();
    client
        .send(Message::Text(r#"{"type":"response.interrupt"}"#.into()))
        .await
        .unwrap();
    send_json(&mut client, tool_follow_up()).await;
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["response"]["id"],
        "resp_reconnected"
    );
    let replay: Value = serde_json::from_str(&frames_rx.recv().await.unwrap()).unwrap();
    assert_eq!(replay["type"], "response.create");
    assert!(
        replay.get("previous_response_id").is_none(),
        "new socket must expand interrupted store=false response history"
    );
    let input = replay["input"].as_array().unwrap();
    assert!(input
        .iter()
        .any(|item| item["type"] == "function_call" && item["call_id"] == "call_interrupt"));
    assert!(input
        .iter()
        .any(|item| item["type"] == "function_call_output" && item["call_id"] == "call_interrupt"));
    assert!(replay.to_string().contains("initial prompt"));
    assert_normal_client_close(&mut client).await;
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), upstream)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn official_responses_websocket_interrupt_never_replays_cancelled_generation_after_disconnect(
) {
    let _guard = crate::test_env_guard();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap().to_string();
    let upstream = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |_: &Request, response: Response| Ok(response))
            .await
            .unwrap();
        let first: Value = serde_json::from_str(&next_text(&mut socket).await).unwrap();
        assert_eq!(first["type"], "response.create");
        send_json(
            &mut socket,
            json!({"type": "response.created", "response": {"id": "resp_interrupt"}}),
        )
        .await;
        assert_eq!(next_text(&mut socket).await, INTERRUPT);
        drop(socket);
        assert!(
            tokio::time::timeout(Duration::from_millis(350), listener.accept())
                .await
                .is_err(),
            "interrupted request must never reconnect and regenerate"
        );
    });
    let (storage, _db_guard, addr, shutdown, server) =
        seed_proxy(&upstream_addr, "ws-interrupt-no-replay").await;
    let (mut client, _) = connect_async(build_ws_request(
        &format!("ws://{addr}/v1/responses"),
        "platform_key_interrupt",
        &[],
    ))
    .await
    .unwrap();
    send_json(
        &mut client,
        json!({"type": "response.create", "model": "gpt-6-luna", "input": "cancel me"}),
    )
    .await;
    assert_eq!(
        serde_json::from_str::<Value>(&next_text(&mut client).await).unwrap()["type"],
        "response.created"
    );
    client.send(Message::Text(INTERRUPT.into())).await.unwrap();
    let error: Value = serde_json::from_str(&next_text(&mut client).await).unwrap();
    assert_eq!(error["type"], "error");
    assert_eq!(error["status"], 502);
    tokio::time::timeout(Duration::from_secs(5), upstream)
        .await
        .unwrap()
        .unwrap();
    let logs = storage.list_request_logs(None, 20).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].status_code, Some(502));
    let _ = client.close(None).await;
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}
