//! Requires a disposable ClickHouse with Keeper and keeper_map_path_prefix.
//!
//!   RUSH_TEST_CLICKHOUSE_URL=http://localhost:18123 RUSH_LEADER_ELECTION=keeper \
//!   RUSH_LEADER_LEASE_TTL_SECS=6 cargo test --test engine_leader_lease -- --ignored
use std::sync::Arc;
use std::time::{Duration, Instant};

use rush_api::leader::{EngineLease, LeaderElection};
use rush_api::self_metrics::SelfMetrics;
use rush_api::shutdown::ShutdownController;

struct Replica {
    shutdown: ShutdownController,
    lease: Arc<EngineLease>,
}

async fn replica(url: &str, name: &'static str, id: usize) -> anyhow::Result<Replica> {
    let client = clickhouse::Client::default()
        .with_url(url)
        .with_user("default");
    let shutdown = ShutdownController::new();
    let election = LeaderElection::from_env(
        client,
        &format!("replica-{id}"),
        Arc::new(SelfMetrics::new()),
        Some(shutdown.clone()),
    )
    .await?;
    Ok(Replica {
        shutdown,
        lease: election.lease(name),
    })
}

fn leaders(replicas: &[Replica]) -> Vec<usize> {
    replicas
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.shutdown.is_requested() && r.lease.is_leader())
        .map(|(i, _)| i)
        .collect()
}

async fn wait_for_one_leader(replicas: &[Replica], within: Duration) -> usize {
    let deadline = Instant::now() + within;
    loop {
        let current = leaders(replicas);
        assert!(
            current.len() <= 1,
            "two replicas held the lease at once: {current:?}"
        );
        if current.len() == 1 {
            return current[0];
        }
        assert!(
            Instant::now() < deadline,
            "no replica became leader in {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable ClickHouse with Keeper"]
async fn one_replica_leads_and_hands_over_on_shutdown() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let name: &'static str = Box::leak(format!("test-{}", uuid::Uuid::new_v4()).into_boxed_str());

    let mut replicas = Vec::new();
    for id in 0..5 {
        replicas.push(replica(&url, name, id).await?);
    }

    // Exactly one leader, and it stays the only one across several renewals.
    let first = wait_for_one_leader(&replicas, Duration::from_secs(10)).await;
    let first_epoch = replicas[first].lease.epoch();
    let watch_until = Instant::now() + Duration::from_secs(8);
    while Instant::now() < watch_until {
        assert_eq!(
            leaders(&replicas),
            vec![first],
            "leadership moved without a failure"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        replicas[first].lease.epoch(),
        first_epoch,
        "renewals keep the epoch"
    );

    // Shutting the leader down releases the lease; another replica takes it
    // well before the TTL would have expired.
    let released_at = Instant::now();
    replicas[first].shutdown.request();
    let second = wait_for_one_leader(&replicas, Duration::from_secs(5)).await;
    assert_ne!(second, first);
    assert!(released_at.elapsed() < Duration::from_secs(5));
    assert!(
        replicas[second].lease.epoch() > first_epoch,
        "a new holder bumps the epoch"
    );

    for replica in &replicas {
        replica.shutdown.request();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable ClickHouse with Keeper"]
async fn a_live_lease_blocks_takeover_until_it_expires() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let name: &'static str = Box::leak(format!("test-{}", uuid::Uuid::new_v4()).into_boxed_str());
    let client = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default");

    // Creates the table.
    let first = replica(&url, name, 0).await?;
    wait_for_one_leader(std::slice::from_ref(&first), Duration::from_secs(10)).await;
    first.shutdown.request();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A replica that vanished without releasing: its lease is still live for 4s.
    client
        .query(
            "ALTER TABLE config_engine_leases UPDATE holder = 'vanished', token = 'x', \
             expires_at = now64(3) + toIntervalSecond(4) WHERE name = ?",
        )
        .with_option("keeper_map_strict_mode", "1")
        .bind(name)
        .execute()
        .await?;
    let started = Instant::now();
    let replicas = vec![replica(&url, name, 1).await?, replica(&url, name, 2).await?];
    wait_for_one_leader(&replicas, Duration::from_secs(10)).await;
    assert!(
        started.elapsed() >= Duration::from_secs(3),
        "took over a live lease after {:?}",
        started.elapsed()
    );

    for replica in &replicas {
        replica.shutdown.request();
    }
    Ok(())
}
