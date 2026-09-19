// Actual insertion + access-listener path. No transport or cryptographic validation.
include!("endpoint_lab.rs");
use kitsune2_core::factories::{
    CorePeerAccessState, MemBlocks, MemPeerStore, MemPeerStoreConfig,
};

fn valid_info(
    i: usize,
    t: i64,
    endpoint: Url,
    epoch: i64,
) -> Arc<AgentInfoSigned> {
    AgentBuilder {
        agent: Some(AgentId::from(bytes::Bytes::copy_from_slice(
            &(i as u64).to_le_bytes(),
        ))),
        created_at: Some(Timestamp::from_micros(epoch + t)),
        expires_at: Some(Timestamp::from_micros(epoch + 86_400_000_000)),
        url: Some(Some(endpoint)),
        is_tombstone: Some(false),
        storage_arc: Some(DhtArc::Empty),
        ..Default::default()
    }
    .build(TestLocalAgent::default())
}

async fn listener_run(
    n: usize,
    case: &str,
    epoch: i64,
    iterations: usize,
) -> serde_json::Value {
    let setup = Instant::now();
    let known = Arc::new(CoreKnownPeers::default());
    let blocks = Arc::new(MemBlocks::default());
    let store: DynPeerStore = Arc::new(MemPeerStore::new(
        MemPeerStoreConfig {
            prune_interval_s: 3600,
        },
        blocks.clone(),
        known.clone(),
    ));
    let shared = case == "listener_shared_blocked";
    let initial: Vec<_> = (0..n)
        .map(|i| {
            valid_info(
                i,
                10,
                if shared && i <= 16 { url(n) } else { url(i) },
                epoch,
            )
        })
        .collect();
    assert!(initial[0].created_at < Timestamp::now());
    assert!(initial[0].expires_at > Timestamp::now());
    if shared {
        blocks
            .block(BlockTarget::Agent(initial[0].agent.clone()))
            .await
            .unwrap();
    }
    // Prefill through the actual store, but attach listener afterward: timing
    // is steady update handling with N retained identities, not bulk startup.
    store.insert(initial.clone()).await.unwrap();
    let access =
        CorePeerAccessState::new(known.clone(), blocks, &store).unwrap();
    let batches: Vec<Vec<_>> = (0..iterations + 20)
        .map(|round| {
            (0..16)
                .map(|i| {
                    let endpoint = match case {
                        "listener_rebind" => url(n + 2 + i + n * (round % 2)),
                        "listener_shared_blocked" => url(n),
                        "listener_unique" => url(i),
                        _ => panic!("unknown listener case"),
                    };
                    valid_info(i, 100 + round as i64, endpoint, epoch)
                })
                .collect()
        })
        .collect();
    let setup_ns = setup.elapsed().as_nanos();
    let warmup = Instant::now();
    for batch in &batches[..20] {
        store.insert(black_box(batch.clone())).await.unwrap();
    }
    let warmup_ns = warmup.elapsed().as_nanos();
    let start = Instant::now();
    for batch in &batches[20..] {
        store.insert(black_box(batch.clone())).await.unwrap();
        black_box(());
    }
    let elapsed_ns = start.elapsed().as_nanos();
    // Assertions are outside timing. Block filtering must not erase the
    // retained identity; all unblocked records must reach the real peer store.
    for a in batches.last().unwrap() {
        let decision = access
            .get_access_decision(a.url.clone().unwrap())
            .unwrap()
            .unwrap()
            .decision;
        assert_eq!(
            decision,
            if shared {
                AccessDecision::Blocked
            } else {
                AccessDecision::Granted
            }
        );
        if shared && a.agent == initial[0].agent {
            assert!(store.get(a.agent.clone()).await.unwrap().is_none());
        } else {
            assert_eq!(
                store
                    .get(a.agent.clone())
                    .await
                    .unwrap()
                    .unwrap()
                    .created_at,
                a.created_at
            );
        }
    }
    if shared {
        let agents = known.get_by_url(url(n)).await.unwrap();
        assert_eq!(agents.len(), 17);
        assert!(agents.contains(&initial[0].agent));
    }
    if case == "listener_rebind" {
        assert!(known.get_by_url(url(0)).await.unwrap().is_empty());
    }
    serde_json::json!({"mode":"bench", "n":n,"case":case,"iterations":iterations,
        "records_per_call":16,"listeners_per_call":if shared {15} else {16},
        "elapsed_ns":elapsed_ns,"setup_ns":setup_ns,"warmup_ns":warmup_ns,"epoch_micros":epoch})
}

#[test]
fn listener_semantics() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            let epoch = Timestamp::now().as_micros() - 60_000_000;
            for case in [
                "listener_unique",
                "listener_rebind",
                "listener_shared_blocked",
            ] {
                listener_run(100, case, epoch, 3).await;
            }
        });
}

#[test]
#[ignore = "run by paired lab driver"]
fn listener_measurement() {
    let n: usize = std::env::var("LAB_N").unwrap().parse().unwrap();
    let case = std::env::var("LAB_CASE").unwrap();
    let epoch: i64 =
        std::env::var("LAB_EPOCH_MICROS").unwrap().parse().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            println!("LAB_JSON {}", listener_run(n, &case, epoch, 200).await);
        });
}
