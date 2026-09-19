//! K3d Phase 1: qualify current response bookkeeping against the actual
//! in-tree host op store, and measure response-cleanup membership cost.
//! Real CoreFetch factory + registered wire handler; the actual
//! Kitsune2MemoryOpStore is the host path. Synthetic transport only.
//! All tests use a current-thread runtime; no sleeps establish proofs.

use kitsune2_api::*;
use kitsune2_core::{default_test_builder, factories::MemoryOp};
use kitsune2_test_utils::space::TEST_SPACE_ID;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

fn url(n: u8) -> Url {
    Url::from_str(format!("ws://test:80/{n}")).unwrap()
}

/// Report implementation that records every `fetched_op` attribution.
#[derive(Debug, Default)]
struct CapturingReport {
    fetched: Mutex<Vec<(OpId, u64)>>,
}

impl Report for CapturingReport {
    fn fetched_op(
        &self,
        _space: SpaceId,
        _source: Url,
        op_id: OpId,
        size: u64,
    ) {
        self.fetched.lock().unwrap().push((op_id, size));
    }
}

struct Fixture {
    fetch: DynFetch,
    handler: DynTxModuleHandler,
    sent: mpsc::UnboundedReceiver<()>,
    report: Arc<CapturingReport>,
    _transport: DynTransport,
}

impl Fixture {
    async fn new() -> Self {
        let builder =
            Arc::new(default_test_builder().with_default_config().unwrap());
        let (sent_tx, sent) = mpsc::unbounded_channel();
        let capture = Arc::new(Mutex::new(None));
        let handler_slot = capture.clone();
        let mut tx = MockTransport::new();
        tx.expect_register_module_handler().times(1).returning(
            move |_, _, h| {
                *handler_slot.lock().unwrap() = Some(h);
            },
        );
        tx.expect_send_module().returning(move |_, _, _, _| {
            let signal = sent_tx.clone();
            Box::pin(async move {
                signal.send(()).unwrap();
                Ok(())
            })
        });
        let transport: DynTransport = Arc::new(tx);
        // The actual in-tree host op store, delegated through a mock so the
        // same real code path runs without further transport machinery.
        let backing = builder
            .op_store
            .create(builder.clone(), TEST_SPACE_ID)
            .await
            .unwrap();
        let filtering = backing.clone();
        let processing = backing.clone();
        let mut store = MockOpStore::new();
        store
            .expect_filter_out_existing_ops()
            .returning(move |ids| {
                let store = filtering.clone();
                Box::pin(
                    async move { store.filter_out_existing_ops(ids).await },
                )
            });
        store.expect_process_incoming_ops().returning(move |ops| {
            let store = processing.clone();
            Box::pin(async move { store.process_incoming_ops(ops).await })
        });
        let report = Arc::new(CapturingReport::default());
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
                report.clone(),
                Arc::new(store),
                meta,
                transport.clone(),
            )
            .await
            .unwrap();
        Self {
            fetch,
            handler: capture.lock().unwrap().take().unwrap(),
            sent,
            report,
            _transport: transport,
        }
    }

    async fn admit(&mut self, ops: &[MemoryOp], peer: u8) {
        self.fetch
            .request_ops(
                ops.iter()
                    .map(|op| PublishOp {
                        op_id: op.compute_op_id(),
                        metadata: None,
                    })
                    .collect(),
                url(peer),
            )
            .await
            .unwrap();
        for _ in ops {
            tokio::time::timeout(Duration::from_secs(10), self.sent.recv())
                .await
                .expect("SETUP send completion")
                .unwrap();
        }
    }

    /// Deliver a response message through the real registered handler.
    fn respond_raw(&self, message: bytes::Bytes, peer: u8) {
        self.handler
            .recv_module_msg(url(peer), TEST_SPACE_ID, "Fetch".into(), message)
            .unwrap();
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

    /// Wait until CoreFetch's response task has completed its synchronous
    /// cleanup for this response (pending map shrinks to `target`).
    async fn await_pending(&self, target: usize) {
        let deadline = Duration::from_secs(30);
        let start = Instant::now();
        loop {
            let count = self.pending_count().await;
            if count == target {
                return;
            }
            assert!(
                start.elapsed() < deadline,
                "K3D_CLEANUP timeout at {count}"
            );
            tokio::task::yield_now().await;
        }
    }
}

/// Deterministic distinct ops; payload length varies with `size`.
fn op_with(index: usize, size: usize) -> MemoryOp {
    let mut payload = format!("k3d-op-{index:07}-").into_bytes();
    payload.resize(size, b'x');
    MemoryOp::new(Timestamp::now(), payload)
}

/// Direct probe of the actual host contract for `process_incoming_ops`:
/// full acceptance, reordering, duplicates, empty input, and an
/// id/data-mismatch attempt (partial-acceptance reachability).
#[tokio::test]
async fn probe_host_process_incoming_ops_contract() {
    let builder =
        Arc::new(default_test_builder().with_default_config().unwrap());
    let store = builder
        .op_store
        .create(builder.clone(), TEST_SPACE_ID)
        .await
        .unwrap();

    let ops: Vec<MemoryOp> = (0..5).map(|i| op_with(i, 40)).collect();
    let to_incoming = |ops: &[MemoryOp]| -> Vec<IncomingOp> {
        ops.iter()
            .map(|op| IncomingOp {
                op_id: op.compute_op_id(),
                op_data: MetaOp::from(op.clone()).op_data,
                metadata: None,
            })
            .collect()
    };

    // Full acceptance: every input id returned, in input order.
    let incoming = to_incoming(&ops);
    let expected: Vec<OpId> =
        incoming.iter().map(|o| o.op_id.clone()).collect();
    let out = store.process_incoming_ops(incoming).await.unwrap();
    assert_eq!(out, expected, "full acceptance echoes input order");

    // Reordering: feed ids in a different order; result follows the new order.
    let mut reordered = to_incoming(&ops);
    reordered.reverse();
    let expected: Vec<OpId> =
        reordered.iter().map(|o| o.op_id.clone()).collect();
    let out = store.process_incoming_ops(reordered).await.unwrap();
    assert_eq!(out, expected, "reordered input is echoed in input order");

    // Duplicates within one call: one entry per input element.
    let dup = vec![
        IncomingOp {
            op_id: ops[0].compute_op_id(),
            op_data: MetaOp::from(ops[0].clone()).op_data,
            metadata: None,
        };
        2
    ];
    let out = store.process_incoming_ops(dup).await.unwrap();
    assert_eq!(out.len(), 2, "duplicate inputs each produce an id");

    // Empty input: empty output.
    assert!(
        store.process_incoming_ops(vec![]).await.unwrap().is_empty(),
        "empty input returns empty"
    );

    // Partial-acceptance reachability: an id that does not match its data is
    // still accepted under the supplied id. There is no in-tree rejection.
    let mismatched = IncomingOp {
        op_id: op_with(99, 40).compute_op_id(),
        op_data: MetaOp::from(ops[1].clone()).op_data,
        metadata: None,
    };
    let expected = mismatched.op_id.clone();
    let out = store.process_incoming_ops(vec![mismatched]).await.unwrap();
    assert_eq!(out, vec![expected], "mismatched id accepted as supplied");

    println!(
        "LAB_JSON {{\"test\":\"probe_host_process_incoming_ops_contract\",\
         \"contract\":\"returns every input id, in input order, including \
         duplicates; empty in/empty out; no rejection path: mismatched ids \
         accepted as supplied\"}}"
    );
}

/// Byte reporting pairs returned ids with response ops positionally; under
/// the observed echo contract this attributes each op's own size, including
/// for reversed and duplicate responses.
#[tokio::test]
async fn response_bookkeeping_attributes_bytes_by_identity() {
    let mut f = Fixture::new().await;
    let ops = vec![op_with(0, 10), op_with(1, 200), op_with(2, 30)];
    f.admit(&ops, 1).await;
    assert_eq!(f.pending_count().await, 3);

    // Respond with ops in reverse order.
    let reversed: Vec<MetaOp> =
        ops.iter().rev().map(|o| MetaOp::from(o.clone())).collect();
    f.respond_raw(serialize_response_message(reversed), 1);
    f.await_pending(0).await;

    let mut attributions = f.report.fetched.lock().unwrap().clone();
    attributions.sort();
    let mut expected: Vec<(OpId, u64)> = ops
        .iter()
        .map(|o| {
            (
                o.compute_op_id(),
                MetaOp::from(o.clone()).op_data.len() as u64,
            )
        })
        .collect();
    expected.sort();
    assert_eq!(attributions, expected, "byte counts follow op identity");

    // A duplicate response re-processes and re-attributes; pending stays empty.
    let dup: Vec<MetaOp> =
        vec![MetaOp::from(ops[0].clone()), MetaOp::from(ops[0].clone())];
    f.respond_raw(serialize_response_message(dup), 1);
    let deadline = Instant::now();
    while f.report.fetched.lock().unwrap().len() < 5 {
        assert!(
            deadline.elapsed() < Duration::from_secs(30),
            "duplicate response processed"
        );
        tokio::task::yield_now().await;
    }
    f.await_pending(0).await;
    let count = f.report.fetched.lock().unwrap().len();
    assert_eq!(count, 5, "duplicate response re-attributes both entries");
}

/// Response-cleanup membership cost at predeclared pending/returned sizes.
/// One fresh CoreFetch per trial inside this process; each map has its own
/// random hash seed. Run with --ignored and LAB_* environment variables.
#[tokio::test]
#[ignore]
async fn cleanup_membership_measurement() {
    let pending: usize = std::env::var("LAB_PENDING").unwrap().parse().unwrap();
    let returned: usize =
        std::env::var("LAB_RETURNED").unwrap().parse().unwrap();
    let trials: usize = std::env::var("LAB_TRIALS")
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or(8);
    let variant = std::env::var("K3D_VARIANT").unwrap();

    let all: Vec<MemoryOp> = (0..pending).map(|i| op_with(i, 32)).collect();
    for trial in 0..trials {
        let mut f = Fixture::new().await;
        // Admit in chunks so each send-completion barrier stays bounded.
        for chunk in all.chunks(512) {
            f.admit(chunk, 1).await;
        }
        assert_eq!(f.pending_count().await, pending);
        let responding: Vec<MemoryOp> = all[..returned].to_vec();
        let message = serialize_response_message(
            responding.iter().map(|o| MetaOp::from(o.clone())).collect(),
        );
        let start = Instant::now();
        f.respond_raw(message, 1);
        f.await_pending(pending - returned).await;
        let elapsed = start.elapsed();
        println!(
            "LAB_JSON {{\"test\":\"cleanup_membership_measurement\",\
             \"variant\":\"{variant}\",\"pending\":{pending},\
             \"returned\":{returned},\"trial\":{trial},\
             \"elapsed_ns\":{}}}",
            elapsed.as_nanos()
        );
    }
}
