//! Synthetic CPU profiles for UI testing, not measurements of this process.
//! Run: RUSH_PROFILE_API_KEY=... cargo run --example profiles_demo
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    resource::v1::Resource,
};
use prost::Message;
use rush_api::models::profile::wire::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint =
        std::env::var("RUSH_PROFILE_ENDPOINT").unwrap_or_else(|_| "http://localhost:8080".into());
    let tenant = std::env::var("RUSH_PROFILE_TENANT").unwrap_or_else(|_| "default".into());
    let key = std::env::var("RUSH_PROFILE_API_KEY")?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;
    eprintln!(
        "Sending synthetic profiles for rush-profiles-demo every 10s. Ctrl-C stops the sender."
    );
    loop {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap() as u64;
        let d = Dictionary {
            string_table: [
                "",
                "cpu",
                "nanoseconds",
                "main",
                "handle_request",
                "serialize_json",
                "compress_response",
            ]
            .map(String::from)
            .to_vec(),
            function_table: std::iter::once(Function::default())
                .chain((3..7).map(|i| Function {
                    name_strindex: i,
                    ..Default::default()
                }))
                .collect(),
            location_table: std::iter::once(Location::default())
                .chain((1..5).map(|i| Location {
                    lines: vec![Line {
                        function_index: i,
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
                .collect(),
            stack_table: vec![
                Stack::default(),
                Stack {
                    location_indices: vec![3, 2, 1],
                },
                Stack {
                    location_indices: vec![4, 2, 1],
                },
            ],
            ..Default::default()
        };
        let mut resources = Vec::new();
        for (version, values) in [
            ("v1", [20_000_000, 80_000_000]),
            ("v2", [70_000_000, 30_000_000]),
        ] {
            let attributes = [
                ("service.name", "rush-profiles-demo"),
                ("service.version", version),
                ("k8s.pod.name", "synthetic-demo"),
            ]
            .into_iter()
            .map(|(key, value)| KeyValue {
                key: key.into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(value.into())),
                }),
            })
            .collect();
            resources.push(ResourceProfiles {
                resource: Some(Resource {
                    attributes,
                    ..Default::default()
                }),
                scope_profiles: vec![ScopeProfiles {
                    profiles: vec![Profile {
                        sample_type: Some(ValueType {
                            type_strindex: 1,
                            unit_strindex: 2,
                        }),
                        time_unix_nano: now - 10_000_000_000,
                        duration_nano: 10_000_000_000,
                        profile_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                        samples: values
                            .into_iter()
                            .enumerate()
                            .map(|(i, value)| Sample {
                                stack_index: i as i32 + 1,
                                values: vec![value],
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                }],
            });
        }
        let payload = ExportProfilesServiceRequest {
            dictionary: Some(d),
            resource_profiles: resources,
        }
        .encode_to_vec();
        let response = client
            .post(format!(
                "{}/v1development/profiles",
                endpoint.trim_end_matches('/')
            ))
            .header("content-type", "application/x-protobuf")
            .header("X-Rush-Tenant", &tenant)
            .bearer_auth(&key)
            .body(payload)
            .send()
            .await?;
        if !response.status().is_success() {
            anyhow::bail!("profile upload failed with HTTP {}", response.status());
        }
        tokio::select! { _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {}, _ = tokio::signal::ctrl_c() => break }
    }
    Ok(())
}
