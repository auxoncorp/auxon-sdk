// A basic example using the plugin_utils::ingest module

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Config {
    // This can be set in the config file, or with the environment variable "EXAMPLE_SETTING"
    setting: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    auxon_sdk::init_tracing!();
    let cfg = auxon_sdk::plugin_utils::ingest::Config::<Config>::load("EXAMPLE_")?;
    let mut client = cfg.connect_and_authenticate_ingest().await?;

    let tl = auxon_sdk::api::TimelineId::allocate();
    client.switch_timeline(tl).await?;
    client
        .send_timeline_attrs("tl", [("tl_attr", 1.into())])
        .await?;

    for i in 0..10 {
        client
            .send_event("ev", i as u128, [("ev_attr", "hello".into())])
            .await?;
    }

    Ok(())
}
