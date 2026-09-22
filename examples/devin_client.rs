//! Minimal generated-client request/stream example.
//! Run against a local Connect server with `DEVIN_TOKEN=test cargo run --example devin_client -- http://127.0.0.1:PORT`.

use devin_proto::generated::exa::api_server_pb as pb;
use devin2api::upstream::transport::{TransportConfig, static_token, upstream_clients};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let base_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:48090".into());
    let token = std::env::var("DEVIN_TOKEN").unwrap_or_else(|_| "local-example".into());
    let clients = upstream_clients(
        &TransportConfig {
            base_url,
            proxy: String::new(),
            force_http1: true,
            extra_root_pems: Vec::new(),
        },
        static_token(token),
    )?;
    let request = pb::GetChatMessageRequest {
        prompt: Some("Reply exactly: pong".into()),
        tools: vec![pb::ExaChatPb_ChatToolDefinition {
            name: Some("read_file".into()),
            description: Some("Read a file from the workspace".into()),
            json_schema_string: Some(
                r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#
                    .into(),
            ),
            read_only_hint: Some(true),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut stream = clients.stream.get_chat_message(request).await?;
    while let Some(frame) = stream.message().await? {
        let view = frame.view();
        if let Some(text) = view.delta_text {
            print!("{text}");
        }
    }
    println!();
    Ok(())
}
