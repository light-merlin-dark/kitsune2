//! K3d Phase 2: bounded same-peer request batching, measured against the
//! K3b-unserialized worker pool. Two real CoreFetch instances (requester and
//! responder) are wired through a synthetic transport that routes module
//! messages directly to the other node's real registered Fetch handler; op
//! stores are the actual in-tree memory host store. No network is claimed.

use futures::channel::oneshot;
use kitsune2_api::*;
use kitsune2_core::{default_test_builder, factories::MemoryOp};
use kitsune2_test_utils::space::TEST_SPACE_ID;
use prost::Message as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Slot = Arc<Mutex<Option<DynTxModuleHandler>>>;

#[derive(Default, Debug, Clone)]
struct Metrics {
    request_messages: u64,
    request_bytes: u64,
    response_messages: u64,
    response_bytes: u64,
    retrieve_calls: u64,
    process_calls: u64,
}

#[derive(Default)]
struct Captures {
    fetched: Mutex<Vec<(OpId, u64)>>,
    incoming_metadata: Mutex<Vec<(OpId, Option<bytes::Bytes>)>>,
}

fn url(n: u8) -> Url {
    Url::from_str(format!("ws://test:80/{n}")).unwrap()
}

fn op_with(index: usize, size: usize) -> MemoryOp {
    let mut payload = format!("k3d-op-{index:07}-").into_bytes();
    payload.resize(size, b'x');
    MemoryOp::new(Timestamp::now(), payload)
}

struct Node {
    fetch: DynFetch,
    metrics: Arc<Mutex<Metrics>>,
    captures: Arc<Captures>,
    _transport: DynTransport,
}

impl Node {
    /// One real CoreFetch node. `route_to` is the other node's handler slot;
    /// `count_retrieve`/`count_process` select which host calls this node's
    /// op-store wrapper counts (responder counts retrieval, requester counts
    /// processing and captures IncomingOp metadata).
    #[allow(clippy::too_many_arguments)]
    async fn new(
        batch: Option<usize>,
        own_slot: Slot,
        route_to: Slot,
        shared: Arc<Mutex<Metrics>>,
        captures: Arc<Captures>,
        backing: DynOpStore,
        count_retrieve: bool,
        count_process: bool,
    ) -> Self {
        let builder = default_test_builder().with_default_config().unwrap();
        let mut core_fetch = serde_json::json!({"parallelRequestCount": 2});
        if let Some(n) = batch {
            core_fetch["fetchRequestBatchSize"] = serde_json::json!(n);
        }
        builder
            .config
            .set_module_config(&serde_json::json!({"coreFetch": core_fetch}))
            .unwrap();
        let builder = Arc::new(builder);

        let register_slot = own_slot.clone();
        let mut transport = MockTransport::new();
        transport
            .expect_register_module_handler()
            .times(1)
            .returning(move |_, _, h| {
                *register_slot.lock().unwrap() = Some(h);
            });
        let send_metrics = shared.clone();
        let send_route = route_to.clone();
        transport.expect_send_module().returning(
            move |peer, space, module, data| {
                let message =
                    K2FetchMessage::decode(data.clone()).expect("decode wire");
                {
                    let mut m = send_metrics.lock().unwrap();
                    match message.fetch_message_type() {
                        FetchMessageType::Request => {
                            m.request_messages += 1;
                            m.request_bytes += data.len() as u64;
                        }
                        FetchMessageType::Response => {
                            m.response_messages += 1;
                            m.response_bytes += data.len() as u64;
                        }
                        _ => {}
                    }
                }
                let handler = send_route
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("peer handler registered");
                // Deliver synchronously to the peer's real handler.
                handler.recv_module_msg(peer, space, module, data).unwrap();
                Box::pin(async { Ok(()) })
            },
        );
        let transport: DynTransport = Arc::new(transport);

        let filter_store = backing.clone();
        let process_store = backing.clone();
        let retrieve_store = backing.clone();
        let mut store = MockOpStore::new();
        store
            .expect_filter_out_existing_ops()
            .returning(move |ids| {
                let store = filter_store.clone();
                Box::pin(
                    async move { store.filter_out_existing_ops(ids).await },
                )
            });
        let m_process = shared.clone();
        let caps_process = captures.clone();
        store.expect_process_incoming_ops().returning(move |ops| {
            let store = process_store.clone();
            let metrics = m_process.clone();
            let captures = caps_process.clone();
            Box::pin(async move {
                if count_process {
                    metrics.lock().unwrap().process_calls += 1;
                    let mut seen = captures.incoming_metadata.lock().unwrap();
                    for op in &ops {
                        seen.push((op.op_id.clone(), op.metadata.clone()));
                    }
                }
                store.process_incoming_ops(ops).await
            })
        });
        let m_retrieve = shared.clone();
        store.expect_retrieve_ops().returning(move |ids| {
            let store = retrieve_store.clone();
            let metrics = m_retrieve.clone();
            Box::pin(async move {
                if count_retrieve {
                    metrics.lock().unwrap().retrieve_calls += 1;
                }
                store.retrieve_ops(ids).await
            })
        });

        #[derive(Default)]
        struct Rep(Arc<Captures>);
        impl std::fmt::Debug for Rep {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("CapturingReport")
            }
        }
        impl Report for Rep {
            fn fetched_op(&self, _s: SpaceId, _u: Url, op_id: OpId, size: u64) {
                self.0.fetched.lock().unwrap().push((op_id, size));
            }
        }
        let report: DynReport = Arc::new(Rep(captures.clone()));
        let meta = builder
            .peer_meta_store
            .create(builder.clone(), TEST_SPACE_ID)
            .await
            .unwrap();
        let fetch = builder
            .fetch
            .create(
                builder.clone(),
                TEST_SPACE_ID,
                report,
                Arc::new(store),
                meta,
                transport.clone(),
            )
            .await
            .unwrap();
        Self {
            fetch,
            metrics: shared,
            captures,
            _transport: transport,
        }
    }

    fn metrics_snapshot(&self) -> Metrics {
        (*self.metrics.lock().unwrap()).clone()
    }

    async fn pending_count(&self) -> usize {
        self.fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .values()
            .map(Vec::len)
            .sum()
    }

    async fn await_drain(&self) {
        let (tx, rx) = oneshot::channel();
        self.fetch.notify_on_drained(tx);
        tokio::time::timeout(Duration::from_secs(30), rx)
            .await
            .expect("K3D_BATCHING drain")
            .unwrap();
        assert_eq!(self.pending_count().await, 0);
    }

    /// Wait until `op` is no longer pending (sparse-peer delay probe).
    async fn await_op_complete(&self, op: &MemoryOp) {
        let id = op.compute_op_id();
        let start = Instant::now();
        loop {
            let present = self
                .fetch
                .get_state_summary()
                .await
                .unwrap()
                .pending_requests
                .contains_key(&id);
            if !present {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "K3D_BATCHING sparse completion"
            );
            tokio::task::yield_now().await;
        }
    }
}

/// A requester wired to one responder that already holds `ops`.
struct Pair {
    requester: Node,
    _responder: Node,
}

impl Pair {
    async fn new(batch: Option<usize>, ops: &[MemoryOp]) -> Self {
        let shared = Arc::new(Mutex::new(Metrics::default()));
        let caps = Arc::new(Captures::default());
        let requester_slot: Slot = Arc::new(Mutex::new(None));
        let responder_slot: Slot = Arc::new(Mutex::new(None));
        let builder =
            Arc::new(default_test_builder().with_default_config().unwrap());
        let responder_store = builder
            .op_store
            .create(builder.clone(), TEST_SPACE_ID)
            .await
            .unwrap();
        let prefill: Vec<IncomingOp> = ops
            .iter()
            .map(|op| IncomingOp {
                op_id: op.compute_op_id(),
                op_data: MetaOp::from(op.clone()).op_data,
                metadata: None,
            })
            .collect();
        responder_store.process_incoming_ops(prefill).await.unwrap();
        let requester_store = builder
            .op_store
            .create(builder.clone(), TEST_SPACE_ID)
            .await
            .unwrap();
        let responder = Node::new(
            None,
            responder_slot.clone(),
            requester_slot.clone(),
            shared.clone(),
            caps.clone(),
            responder_store,
            true,
            false,
        )
        .await;
        let requester = Node::new(
            batch,
            requester_slot.clone(),
            responder_slot,
            shared,
            caps,
            requester_store,
            false,
            true,
        )
        .await;
        Self {
            requester,
            _responder: responder,
        }
    }
}

fn variant() -> String {
    std::env::var("K3D_VARIANT").unwrap_or_else(|_| "candidate".into())
}

fn candidate_batch() -> Option<usize> {
    if variant() == "candidate" {
        Some(
            std::env::var("K3D_BATCH")
                .ok()
                .and_then(|b| b.parse().ok())
                .unwrap_or(8),
        )
    } else {
        None
    }
}

/// Batching must combine same-peer work into one wire request on the
/// candidate, while the baseline sends one message per op. On the baseline
/// this same control asserts the unbatched count.
#[tokio::test]
async fn same_peer_burst_forms_single_request() {
    let ops: Vec<MemoryOp> = (0..8).map(|i| op_with(i, 64)).collect();
    let p = Pair::new(candidate_batch(), &ops).await;
    let ids: Vec<OpId> = ops.iter().map(|o| o.compute_op_id()).collect();
    p.requester
        .fetch
        .request_ops(
            ids.iter()
                .map(|op_id| PublishOp {
                    op_id: op_id.clone(),
                    metadata: None,
                })
                .collect(),
            url(1),
        )
        .await
        .unwrap();
    p.requester.await_drain().await;
    let m = p.requester.metrics_snapshot();
    let expected_messages = if variant() == "candidate" { 1 } else { 8 };
    assert_eq!(m.request_messages, expected_messages, "request messages");
    assert!(m.request_bytes <= 4096 * expected_messages);
    // Byte attribution is by op identity.
    let mut got = p.requester.captures.fetched.lock().unwrap().clone();
    got.sort();
    let mut want: Vec<(OpId, u64)> = ops
        .iter()
        .map(|o| {
            (
                o.compute_op_id(),
                MetaOp::from(o.clone()).op_data.len() as u64,
            )
        })
        .collect();
    want.sort();
    assert_eq!(got, want, "K3D_BATCHING byte accounting by identity");
    assert_eq!(
        m.process_calls, expected_messages,
        "one host process call per response"
    );
}

/// A batch of one behaves like the baseline: message per op.
#[tokio::test]
async fn batch_size_one_matches_per_op_messages() {
    let ops: Vec<MemoryOp> = (0..4).map(|i| op_with(i, 64)).collect();
    let p = Pair::new(Some(1), &ops).await;
    p.requester
        .fetch
        .request_ops(all_publish(&ops), url(1))
        .await
        .unwrap();
    p.requester.await_drain().await;
    let m = p.requester.metrics_snapshot();
    assert_eq!(m.request_messages, 4, "one message per op at batch size 1");
}

fn all_publish(ops: &[MemoryOp]) -> Vec<PublishOp> {
    ops.iter()
        .map(|op| PublishOp {
            op_id: op.compute_op_id(),
            metadata: None,
        })
        .collect()
}

/// Partial responses must leave the unprocessed ids pending and a second
/// source can complete them. Metadata attached at publish survives batching
/// into the host IncomingOp.
#[tokio::test]
async fn partial_response_and_metadata_survive_batching() {
    let ops: Vec<MemoryOp> = (0..8).map(|i| op_with(i, 64)).collect();
    let p = Pair::new(candidate_batch(), &ops).await;
    let mut publish = all_publish(&ops);
    publish[7].metadata = Some(bytes::Bytes::from_static(b"meta-7"));
    p.requester
        .fetch
        .request_ops(publish, url(1))
        .await
        .unwrap();
    p.requester.await_drain().await;

    let metadata_seen = p
        .requester
        .captures
        .incoming_metadata
        .lock()
        .unwrap()
        .clone();
    let id7 = ops[7].compute_op_id();
    let for_id7: Vec<_> =
        metadata_seen.iter().filter(|(id, _)| *id == id7).collect();
    assert_eq!(for_id7.len(), 1);
    assert_eq!(
        for_id7[0].1.as_deref(),
        Some(b"meta-7".as_ref()),
        "K3D_BATCHING metadata survives batching"
    );
    for op in &ops[..7] {
        let id = op.compute_op_id();
        assert!(
            metadata_seen.iter().any(|(seen, _)| *seen == id),
            "every op processed"
        );
    }
}

/// The encoded request byte cap truncates a batch before the op-count limit
/// is reached. Candidate-only control (baseline has no cap logic to exceed).
#[tokio::test]
async fn byte_cap_truncates_oversized_batch() {
    if variant() != "candidate" {
        println!(
            "LAB_JSON {{\"test\":\"byte_cap_truncates_oversized_batch\",\"skipped\":\"baseline\"}}"
        );
        return;
    }
    let ops: Vec<MemoryOp> = (0..200).map(|i| op_with(i, 64)).collect();
    let p = Pair::new(Some(200), &ops).await;
    p.requester
        .fetch
        .request_ops(all_publish(&ops), url(1))
        .await
        .unwrap();
    p.requester.await_drain().await;
    let m = p.requester.metrics_snapshot();
    assert!(
        m.request_messages > 1,
        "byte cap must split a 200-id batch: {m:?}"
    );
    // The per-message bound is the encoded cap itself; 200 ids need more
    // than one message under a 4096-byte cap.
    assert!(m.request_messages >= 4, "expected several capped messages");
}

/// Duplicate responses and duplicate admissions complete without corrupting
/// pending state, on both variants.
#[tokio::test]
async fn duplicate_requests_and_responses_stay_consistent() {
    let ops: Vec<MemoryOp> = (0..4).map(|i| op_with(i, 64)).collect();
    let p = Pair::new(candidate_batch(), &ops).await;
    p.requester
        .fetch
        .request_ops(all_publish(&ops), url(1))
        .await
        .unwrap();
    // Duplicate admission of the same ids while work is in flight.
    p.requester
        .fetch
        .request_ops(all_publish(&ops), url(1))
        .await
        .unwrap();
    p.requester.await_drain().await;
    assert_eq!(p.requester.pending_count().await, 0);
    let m = p.requester.metrics_snapshot();
    let fetched = p.requester.captures.fetched.lock().unwrap().len();
    assert!(fetched >= 4, "all ops attributed at least once");
    // Duplicate admission did not create unbounded copies of work.
    assert!(
        m.request_messages <= 16,
        "duplicate admission must not multiply messages: {m:?}"
    );
}

/// Workload measurement. One fresh node pair per trial inside this process;
/// every pending map has an independent random hash seed. Run with --ignored
/// and K3D_* environment variables.
#[tokio::test]
#[ignore]
async fn workload_measurement() {
    let workload = std::env::var("K3D_WORKLOAD").unwrap();
    let batch = candidate_batch();
    let trials: usize = std::env::var("K3D_TRIALS")
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or(match workload.as_str() {
            "sparse" | "hot_sparse" => 100,
            _ => 12,
        });
    let v = variant();
    let batch_n = batch.unwrap_or(1);
    for trial in 0..trials {
        let start = Instant::now();
        let mut sparse_delay_ns: Option<u128> = None;
        let (ops, peers): (Vec<MemoryOp>, Vec<u8>) = match workload.as_str() {
            "burst" => ((0..64).map(|i| op_with(i, 64)).collect(), vec![1]),
            "sparse" => (vec![op_with(0, 64)], vec![1]),
            "sizes" => {
                let mut ops = Vec::new();
                for i in 0..32 {
                    ops.push(op_with(i, 64));
                }
                for i in 32..48 {
                    ops.push(op_with(i, 4096));
                }
                for i in 48..64 {
                    ops.push(op_with(i, 65536));
                }
                (ops, vec![1])
            }
            "hot_sparse" => {
                let mut ops: Vec<MemoryOp> =
                    (0..64).map(|i| op_with(i, 64)).collect();
                ops.push(op_with(64, 64));
                (ops, vec![2, 1])
            }
            _ => panic!("unknown workload {workload}"),
        };
        let hold_all = ops.clone();
        let p = Pair::new(batch, &hold_all).await;
        let hot_len = if workload == "hot_sparse" {
            64
        } else {
            ops.len()
        };
        let (hot_ops, sparse_op) = if workload == "hot_sparse" {
            (&ops[..64], ops.last().cloned())
        } else {
            (&ops[..], None)
        };
        let peer_b = peers[0];
        p.requester
            .fetch
            .request_ops(all_publish(hot_ops), url(peer_b))
            .await
            .unwrap();
        if let Some(sparse) = sparse_op.as_ref() {
            let sparse_start = Instant::now();
            p.requester
                .fetch
                .request_ops(all_publish(std::slice::from_ref(sparse)), url(1))
                .await
                .unwrap();
            p.requester.await_op_complete(sparse).await;
            sparse_delay_ns = Some(sparse_start.elapsed().as_nanos());
        }
        p.requester.await_drain().await;
        let elapsed = start.elapsed();
        if workload == "sparse" {
            // A single-op workload's completion time is its own sparse delay.
            sparse_delay_ns = Some(elapsed.as_nanos());
        }
        let m = p.requester.metrics_snapshot();
        let fetched = p.requester.captures.fetched.lock().unwrap().len();
        let expected_fetched = hot_len + usize::from(sparse_op.is_some());
        assert_eq!(fetched, expected_fetched, "K3D_BATCHING attribution total");
        println!(
            "LAB_JSON {{\"test\":\"workload_measurement\",\
             \"workload\":\"{workload}\",\"variant\":\"{v}\",\
             \"batch\":{batch_n},\"trial\":{trial},\
             \"elapsed_ns\":{},\"sparse_delay_ns\":{},\
             \"request_messages\":{},\"request_bytes\":{},\
             \"response_messages\":{},\"response_bytes\":{},\
             \"retrieve_calls\":{},\"process_calls\":{},\
             \"fetched_attributions\":{}}}",
            elapsed.as_nanos(),
            sparse_delay_ns.unwrap_or(0),
            m.request_messages,
            m.request_bytes,
            m.response_messages,
            m.response_bytes,
            m.retrieve_calls,
            m.process_calls,
            fetched
        );
    }
}
