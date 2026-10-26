use super::*;

#[test]
fn aggregate_first_unknown_delivery_keeps_502_with_an_empty_account_pool() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-aggregate-first-unknown-delivery-empty-pool");
    let db_path = dir.join("codexmanager.db");
    let _db = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _total = EnvGuard::set("CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS", "5000");
    let listener = bind_test_listener("aggregate unknown-delivery upstream");
    let upstream_addr = listener.local_addr().expect("mock address").to_string();
    let (captured_tx, captured_rx) = mpsc::channel();
    let storage = Storage::open(&db_path).expect("open fixture database");
    storage.init().expect("initialize fixture database");
    let aggregate_id = "agg_unknown_delivery_empty_pool";
    let key_id = "gk_unknown_delivery_empty_pool";
    let platform_key = "pk_unknown_delivery_empty_pool";
    let now = now_ts();
    insert_aggregate_api(&storage, aggregate_id, &upstream_addr, "/responses", now);
    seed_dual_routes(&storage, aggregate_id);
    insert_hybrid_aggregate_first_key(&storage, key_id, platform_key, now);
    assert!(storage
        .list_accounts()
        .expect("read account pool")
        .is_empty());
    let request = serde_json::json!({
        "model": MODEL,
        "input": "do not replay this submitted request",
        "stream": false,
        "tools": [{
            "type": "function",
            "name": "inspect_fixture",
            "parameters": {"type": "object", "properties": {}}
        }]
    });
    let upstream = thread::spawn(move || {
        let mut count = 0;
        loop {
            let timeout = if count == 0 {
                Duration::from_secs(5)
            } else {
                Duration::from_millis(300)
            };
            let Some((stream, captured)) = accept_http_request(&listener, timeout) else {
                break;
            };
            // The shared reader returns only after every Content-Length byte.
            // Close without writing even an HTTP status line or response header.
            captured_tx.send(captured).expect("capture complete POST");
            stream
                .shutdown(std::net::Shutdown::Both)
                .expect("close aggregate socket without response headers");
            count += 1;
        }
    });

    let server = codexmanager_service::start_one_shot_server().expect("start real gateway");
    let (status, response_body) = post_http_raw_with_read_timeout(
        &server.addr,
        "/v1/responses",
        &request.to_string(),
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
        Duration::from_secs(10),
    );
    server.join();
    upstream.join().expect("join aggregate mock");

    assert_eq!(
        status, 502,
        "must retain the aggregate outcome: {response_body}"
    );
    assert!(
        response_body.contains("automatic replay stopped"),
        "{response_body}"
    );
    let requests = captured_rx.try_iter().collect::<Vec<_>>();
    assert_eq!(
        requests.len(),
        1,
        "must submit only one complete POST: {requests:?}"
    );
    let captured = &requests[0];
    assert_eq!(captured.path, "/backend-api/codex/responses");
    assert_eq!(
        captured.headers.get("authorization").map(String::as_str),
        Some("Bearer aggregate-secret")
    );
    assert_eq!(
        captured
            .headers
            .get("content-length")
            .expect("Content-Length")
            .parse::<usize>()
            .expect("numeric Content-Length"),
        captured.body.len()
    );
    let body: serde_json::Value = serde_json::from_slice(&decode_upstream_request_body(captured))
        .expect("complete aggregate request JSON");
    assert_eq!(body["model"], UPSTREAM_MODEL);
    assert_eq!(body["tools"][0]["name"], "inspect_fixture");

    let logs = storage
        .list_request_logs(Some(&format!("key:={key_id}")), 10)
        .expect("read final request log");
    assert_eq!(logs.len(), 1, "must persist one final outcome");
    let log = &logs[0];
    assert_eq!(log.status_code, Some(502));
    assert_eq!(log.actual_source_kind.as_deref(), Some("aggregate_api"));
    assert_eq!(log.actual_source_id.as_deref(), Some(aggregate_id));
    let trace_id = log.trace_id.as_deref().expect("request trace");
    let attempts = wait_for_upstream_attempts(&storage, trace_id, 1);
    assert_eq!(
        attempts.len(),
        1,
        "must not retry or release into the account pool"
    );
    assert_eq!(attempts[0].method, "POST");
    assert_eq!(
        attempts[0].wire_sha256,
        format!("{:x}", Sha256::digest(&captured.body))
    );
}
