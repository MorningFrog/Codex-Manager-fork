use super::*;

struct ModelOrderEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl ModelOrderEnvironment {
    fn new(database: &std::path::Path, codex_home: &std::path::Path) -> Self {
        let mut environment = Self(Vec::new());
        for (name, value) in [
            ("CODEXMANAGER_DB_PATH", Some(database.as_os_str())),
            ("CODEX_HOME", Some(codex_home.as_os_str())),
            (
                "CODEXMANAGER_STORAGE_BACKEND",
                Some(std::ffi::OsStr::new("sqlite")),
            ),
            ("CODEXMANAGER_DATABASE_URL", None),
        ] {
            environment.0.push((name, std::env::var_os(name)));
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        environment
    }
}

impl Drop for ModelOrderEnvironment {
    fn drop(&mut self) {
        while let Some((name, value)) = self.0.pop() {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

#[test]
fn admin_model_reorder_rpc_persists_and_rejects_a_stale_catalog() {
    let _lock = test_env_guard();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "codexmanager-model-order-rpc-{}-{nonce}",
        std::process::id()
    ));
    let database = root.join("catalog.sqlite");
    let codex_home = root.join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create isolated Codex home");
    let config_path = codex_home.join("config.toml");
    let config = "model = \"isolated-model-order-rpc\"\n";
    std::fs::write(&config_path, config).expect("write isolated config sentinel");
    let environment = ModelOrderEnvironment::new(&database, &codex_home);
    storage_helpers::initialize_storage().expect("initialize isolated model catalog");

    let admin_rpc = |method, params| {
        response_result(handle_request_with_actor(
            rpc_request(method, params),
            RpcActor::system_admin(),
        ))
    };
    let list = || {
        admin_rpc(
            "apikey/managedModelListV2",
            serde_json::json!({"includeHidden": true}),
        )
    };
    let initial = list();
    assert!(rpc_error(&initial).is_empty(), "{:?}", initial.result);
    let original = initial.result["items"]
        .as_array()
        .expect("complete model catalog");
    assert!(original.len() > 2);
    assert!(original.iter().any(|model| model["visibility"] == "hide"));
    let expected_slugs = original
        .iter()
        .map(|model| {
            model["slug"]
                .as_str()
                .expect("canonical model slug")
                .to_string()
        })
        .collect::<Vec<_>>();
    let mut reversed = expected_slugs.clone();
    reversed.reverse();

    let reordered = admin_rpc(
        "apikey/managedModelReorderV2",
        serde_json::json!({
            "slugs": reversed,
            "expectedSlugs": expected_slugs,
        }),
    );
    assert!(rpc_error(&reordered).is_empty(), "{:?}", reordered.result);
    assert_eq!(
        reordered.result.as_array().expect("reordered full catalog"),
        &original.iter().rev().cloned().collect::<Vec<_>>()
    );
    let persisted = list();
    assert!(rpc_error(&persisted).is_empty(), "{:?}", persisted.result);
    assert_eq!(persisted.result["items"], reordered.result);
    assert_eq!(persisted.result["stats"], initial.result["stats"]);

    let stale = admin_rpc(
        "apikey/managedModelReorderV2",
        serde_json::json!({
            "slugs": expected_slugs,
            "expectedSlugs": expected_slugs,
        }),
    );
    assert!(
        rpc_error(&stale).contains("model_order_conflict"),
        "{:?}",
        stale.result
    );
    let after_conflict = list();
    assert!(
        rpc_error(&after_conflict).is_empty(),
        "{:?}",
        after_conflict.result
    );
    assert_eq!(after_conflict.result, persisted.result);
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), config);
    assert_eq!(std::fs::read_dir(&codex_home).unwrap().count(), 1);

    drop(environment);
    let _ = std::fs::remove_file(config_path);
    let _ = std::fs::remove_dir(codex_home);
    for name in [
        "catalog.sqlite",
        "catalog.sqlite-wal",
        "catalog.sqlite-shm",
        "catalog.sqlite.model-catalog.lock",
    ] {
        let _ = std::fs::remove_file(root.join(name));
    }
    let _ = std::fs::remove_dir(root);
}
