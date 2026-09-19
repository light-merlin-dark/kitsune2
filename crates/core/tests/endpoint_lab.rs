// Local instrumented component experiment against Kitsune2 (Apache-2.0).
// Uses actual public CoreKnownPeers API, not a substitute implementation.
use kitsune2_api::*;
use kitsune2_core::factories::CoreKnownPeers;
use kitsune2_test_utils::agent::{AgentBuilder, TestLocalAgent};
use std::{collections::BTreeMap, hint::black_box, sync::Arc, time::Instant};

/// Construct a distinct in-memory peer endpoint for this fixture.
fn url(i: usize) -> Url {
    Url::from_str(format!("ws://lab.invalid:80/{i}")).unwrap()
}
/// Build a signed identity advertisement with controlled timestamp and endpoint.
fn info(
    i: usize,
    t: i64,
    endpoint: Option<Url>,
    tombstone: bool,
) -> Arc<AgentInfoSigned> {
    AgentBuilder {
        agent: Some(AgentId::from(bytes::Bytes::copy_from_slice(
            &(i as u64).to_le_bytes(),
        ))),
        created_at: Some(Timestamp::from_micros(t)),
        expires_at: Some(Timestamp::from_micros(1_000_000_000)),
        url: Some(endpoint),
        is_tombstone: Some(tombstone),
        storage_arc: Some(DhtArc::Empty),
        ..Default::default()
    }
    .build(TestLocalAgent::default())
}
/// Create the single-thread runtime used by deterministic component tests.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

/// Compare endpoint results with a timestamp-ordered oracle over deterministic updates.
#[test]
fn equivalence() {
    runtime().block_on(async {
        let known = CoreKnownPeers::default();
        assert!(Url::from_str("").is_err()); // empty strings are not valid API URLs
        assert!(known.get_by_url(url(0)).await.unwrap().is_empty());
        known.record(vec![]).await.unwrap();
        // New/equal/older, rebind, shared URL, None, tombstone and tombstone
        // carrying Some(URL): preserve upstream behavior even for odd input.
        let edges = [
            (0, 10, Some(0), false), (1, 10, Some(0), false),
            (0, 9, Some(1), false), (0, 10, Some(1), false),
            (0, 11, Some(1), false), (0, 12, None, true),
            (0, 11, Some(0), false), (0, 12, Some(0), false),
            (0, 13, Some(0), false), (1, 11, None, false),
            (2, 1, None, false), (2, 2, Some(2), true),
            (2, 3, Some(2), false), (0, 14, Some(1), false),
        ];
        let mut seq = edges.to_vec();
        let mut seed = 0x1909_2026_u64;
        for _ in 0..5000 {
            seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
            seq.push(((seed % 64) as usize, ((seed >> 8) % 100) as i64,
                      if seed.is_multiple_of(5) { None } else { Some(((seed >> 24) % 8) as usize) },
                      seed.is_multiple_of(7)));
        }
        let mut oracle: BTreeMap<AgentId, (i64, Option<Url>)> = BTreeMap::new();
        let mut digest = 0xcbf29ce484222325_u64;
        let mut checks = 0;
        // Batch length varies deterministically; duplicates in a batch matter.
        let mut cursor = 0;
        while cursor < seq.len() {
            let count = if cursor < edges.len() { 1 } else { 1 + cursor % 7 };
            let end = (cursor + count).min(seq.len());
            let batch: Vec<_> = seq[cursor..end].iter().map(|&(i,t,u,b)| info(i,t,u.map(url),b)).collect();
            for a in &batch {
                if oracle.get(&a.agent).is_none_or(|(t,_)| *t < a.created_at.as_micros()) {
                    oracle.insert(a.agent.clone(), (a.created_at.as_micros(), a.url.clone()));
                }
            }
            known.record(batch).await.unwrap();
            for q in 0..=8 {
                let query = url(q);
                let mut actual = known.get_by_url(query.clone()).await.unwrap();
                actual.sort();
                let expected: Vec<_> = oracle.iter().filter(|(_,(_,u))| u.as_ref() == Some(&query)).map(|(id,_)| id.clone()).collect();
                assert_eq!(actual, expected, "batch ending {end}, query {q}");
                for byte in serde_json::to_vec(&(end,q,&actual)).unwrap() {
                    digest = (digest ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                }
                checks += 1;
            }
            cursor = end;
        }
        println!("LAB_JSON {}", serde_json::json!({"mode":"verify", "records":seq.len(), "checks":checks, "digest":format!("{digest:016x}")}));
    });
}

/// Populate retained identities with shared, unique, and absent endpoints.
fn fixture(n: usize) -> Vec<Arc<AgentInfoSigned>> {
    (0..n)
        .map(|i| {
            info(
                i,
                10,
                match i % 10 {
                    0 => Some(url(n)),
                    1 => None,
                    _ => Some(url(i)),
                },
                false,
            )
        })
        .collect()
}

/// Measure an explicitly configured endpoint lookup or update workload.
#[test]
#[ignore = "run by paired lab driver"]
fn measurement() {
    let n: usize = std::env::var("LAB_N").unwrap().parse().unwrap();
    let case = std::env::var("LAB_CASE").unwrap();
    runtime().block_on(async {
        let setup = Instant::now();
        let known = CoreKnownPeers::default();
        let initial = fixture(n);
        known.record(initial.clone()).await.unwrap();
        let iterations = match case.as_str() {
            "shared" => 2000,
            "hit" | "miss" => 10000,
            _ => 2000,
        };
        let query = match case.as_str() { "hit" => url(2), "shared" => url(n), _ => url(n+1) };
        if matches!(case.as_str(), "hit" | "miss" | "shared") {
            let expected = match case.as_str() { "hit" => 1, "shared" => n/10, _ => 0 };
            assert_eq!(known.get_by_url(query.clone()).await.unwrap().len(), expected);
            let setup_ns = setup.elapsed().as_nanos();
            let warmup = Instant::now();
            for _ in 0..1000 { black_box(known.get_by_url(black_box(query.clone())).await.unwrap()); }
            let warmup_ns = warmup.elapsed().as_nanos();
            let start = Instant::now();
            for _ in 0..iterations { black_box(known.get_by_url(black_box(query.clone())).await.unwrap()); }
            println!("LAB_JSON {}", serde_json::json!({"mode":"bench", "n":n,"case":case,"iterations":iterations,"elapsed_ns":start.elapsed().as_nanos(),"setup_ns":setup_ns,"warmup_ns":warmup_ns}));
        } else {
            // Prepare signed advertisements before timing. 32 records per call.
            // Same-URL update, alternating rebind, and rejected stale batches.
            let batches: Vec<Vec<_>> = (0..iterations+100).map(|round| (0..32).map(|j| {
                let i = (round*32+j) % n;
                if case == "stale" { return initial[i].clone(); }
                let endpoint = if case == "same_url" { initial[i].url.clone() }
                    else { Some(url(n+2+i+n*((round*32+j)/n%2))) };
                info(i, 100+round as i64, endpoint, false)
            }).collect()).collect();
            let setup_ns = setup.elapsed().as_nanos();
            let warmup = Instant::now();
            for batch in &batches[..100] { known.record(black_box(batch.clone())).await.unwrap(); }
            let warmup_ns = warmup.elapsed().as_nanos();
            let start = Instant::now();
            for batch in &batches[100..] { known.record(black_box(batch.clone())).await.unwrap();
                black_box(()); }
            println!("LAB_JSON {}", serde_json::json!({"mode":"bench", "n":n,"case":case,"iterations":iterations,"records_per_call":32,"elapsed_ns":start.elapsed().as_nanos(),"setup_ns":setup_ns,"warmup_ns":warmup_ns}));
        }
    });
}
