use serde_json::Value;

use super::WsSessionError;

pub(super) enum ClientMessage {
    Create(Value),
    Interrupt,
}

pub(super) fn is_interrupted_response(event: &Value) -> bool {
    event.get("type").and_then(Value::as_str) == Some("response.incomplete")
        && event.get("error").is_none_or(Value::is_null)
        && event.pointer("/response/error").is_none_or(Value::is_null)
        && event
            .pointer("/response/status_details/error")
            .is_none_or(Value::is_null)
        && event
            .pointer("/response/status")
            .is_none_or(|status| status.as_str() == Some("incomplete"))
        && event
            .pointer("/response/incomplete_details/reason")
            .and_then(Value::as_str)
            == Some("interrupted")
}

pub(super) fn is_retained_interrupted_output(item: &Value) -> bool {
    // Terminal output is authoritative when older servers omit item status.
    // Explicitly unfinished/discarded items must never be replayed on a fresh
    // socket, even if they were present in an earlier output_item.added event.
    item.get("status")
        .filter(|status| !status.is_null())
        .is_none_or(|status| status.as_str() == Some("completed"))
}

pub(super) fn parse_client_message(text: &str) -> Result<ClientMessage, WsSessionError> {
    let payload = serde_json::from_str::<Value>(text).map_err(|err| {
        WsSessionError::bad_request_bilingual(
            "WebSocket JSON 载荷无效",
            format!("invalid websocket json payload: {err}"),
        )
    })?;
    let object = payload.as_object().ok_or_else(|| {
        WsSessionError::bad_request_bilingual(
            "WebSocket 载荷必须是 JSON 对象",
            "websocket payload must be a JSON object",
        )
    })?;
    let message_type = object.get("type").and_then(Value::as_str).ok_or_else(|| {
        WsSessionError::bad_request_bilingual(
            "WebSocket 载荷缺少消息类型",
            "websocket payload missing message type",
        )
    })?;
    match message_type {
        "response.create" => Ok(ClientMessage::Create(payload)),
        "response.interrupt" => Ok(ClientMessage::Interrupt),
        other => Err(WsSessionError::bad_request_bilingual(
            "不支持的 WebSocket 消息类型",
            format!("unsupported websocket message type: {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_control_parser_separates_interrupt_from_create() {
        assert!(matches!(
            parse_client_message(r#"{"type":"response.interrupt"}"#),
            Ok(ClientMessage::Interrupt)
        ));
        assert!(matches!(
            parse_client_message(r#"{"type":"response.create","model":"test","input":[]}"#),
            Ok(ClientMessage::Create(_))
        ));
        for text in ["{}", "[]", "invalid", r#"{"type":"response.cancel"}"#] {
            assert!(parse_client_message(text).is_err(), "accepted {text}");
        }
    }

    #[test]
    fn websocket_control_interrupt_acknowledgements_are_not_terminal_responses() {
        for ack in ["response.interrupt.accepted", "response.interrupt.failed"] {
            let event = serde_json::json!({
                "type": ack, "response_id": "resp_interrupt",
                "error": {"code": "interrupt_not_supported", "message": "Interruption is disabled"}
            });
            assert!(super::super::inspect_ws_terminal_event(&event.to_string()).is_none());
        }
    }

    #[test]
    fn websocket_control_only_successful_interruption_has_success_terminal_status() {
        let event = serde_json::json!({
            "type": "response.incomplete",
            "response": {"id": "resp_interrupt", "status": "incomplete", "incomplete_details": {"reason": "interrupted"},
                "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}}
        });
        let terminal = super::super::inspect_ws_terminal_event(&event.to_string()).unwrap();
        assert_eq!(terminal.status_code, 200);
        assert_eq!(terminal.usage.total_tokens, Some(10));
        assert!(terminal.error.is_none());
        for (pointer, replacement) in [
            ("/response/status", serde_json::json!("failed")),
            (
                "/response/error",
                serde_json::json!({"message": "upstream failure"}),
            ),
            (
                "/error",
                serde_json::json!({"message": "top-level failure"}),
            ),
            (
                "/response/status_details/error",
                serde_json::json!({"message": "nested failure"}),
            ),
            (
                "/response/incomplete_details/reason",
                serde_json::json!("max_output_tokens"),
            ),
        ] {
            let mut failed = event.clone();
            // Use object indexing for optional fields that do not exist yet.
            match pointer {
                "/response/error" => failed["response"]["error"] = replacement,
                "/response/status_details/error" => {
                    failed["response"]["status_details"]["error"] = replacement
                }
                "/error" => failed["error"] = replacement,
                _ => *failed.pointer_mut(pointer).unwrap() = replacement,
            }
            assert!(!is_interrupted_response(&failed));
            let mut history = super::super::CompletedWsResponseCache::default();
            assert!(!history
                .observe_completed_response(&failed.to_string(), None, &serde_json::json!([]))
                .unwrap());
            assert_eq!(
                super::super::inspect_ws_terminal_event(&failed.to_string())
                    .unwrap()
                    .status_code,
                502
            );
        }
    }
}
