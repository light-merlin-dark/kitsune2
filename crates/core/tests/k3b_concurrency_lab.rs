//! Real CoreFetch outgoing workers behind the shared receiver guard.
//!
//! All tests run on a current-thread runtime with a paused clock. Every
//! successful transport send signals entry with an explicit per-send release
//! oneshot (the entry barrier) and holds at that await until the test
//! releases it (the exit barrier). No sleep or scheduler-yield loop is used
//! as proof; timeouts below fire only when the runtime is otherwise idle
//! because the clock is paused, so they are exact entry/bound assertions.
//!
//! Tests named `overlap_*` (except `overlap_workers_1`), the different-peer
//! reproduction and the same-peer case assert that a second configured worker
//! can enter a send while a previous send is still held. Those assertions are
//! expected to fail on the unmodified stacking base (baseline) and pass on
//! the dequeue-scope candidate. Baseline failures panic with the marker
//! `K3B_OVERLAP` in their assertion reason.
use futures::channel::oneshot;
use kitsune2_api::*;
use kitsune2_core::{default_test_builder, factories::MemoryOp};
use kitsune2_test_utils::space::TEST_SPACE_ID;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

type Release = oneshot::Sender<()>;
type SendCounts = HashMap<(Url, Vec<u8>), usize>;

/// Local serde mirror of the public `CoreFetchModConfig` module-config shape
/// (`coreFetch.parallelRequestCount`); the concrete core types are not
/// re-exported for integration tests, and the config API is the public path.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FetchWorkersConfig {
    core_fetch: FetchWorkersOverride,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FetchWorkersOverride {
    parallel_request_count: u8,
}

impl FetchWorkersConfig {
    fn with_workers(workers: u8) -> Self {
        Self {
            core_fetch: FetchWorkersOverride {
                parallel_request_count: workers,
            },
        }
    }
}

struct Fixture {
    fetch: DynFetch,
    handler: DynTxModuleHandler,
    entries: mpsc::UnboundedReceiver<(Url, Release)>,
    errors: mpsc::UnboundedReceiver<Url>,
    stored: mpsc::UnboundedReceiver<()>,
    send_counts: Arc<Mutex<SendCounts>>,
    fail_peers: Arc<Mutex<HashSet<Url>>>,
    _transport: DynTransport,
}

impl Fixture {
    async fn new(workers: u8) -> Self {
        let builder = default_test_builder().with_default_config().unwrap();
        builder
            .config
            .set_module_config(&FetchWorkersConfig::with_workers(workers))
            .unwrap();
        let builder = Arc::new(builder);

        let (entry_tx, entries) = mpsc::unbounded_channel();
        let (error_tx, errors) = mpsc::unbounded_channel();
        let send_counts: Arc<Mutex<SendCounts>> =
            Arc::new(Mutex::new(HashMap::new()));
        let fail_peers = Arc::new(Mutex::new(HashSet::new()));

        let handler = Arc::new(Mutex::new(None));
        let capture = handler.clone();
        let mut tx = MockTransport::new();
        tx.expect_register_module_handler().times(1).returning(
            move |_, _, h| {
                *capture.lock().unwrap() = Some(h);
            },
        );
        {
            let entry_tx = entry_tx.clone();
            let error_tx = error_tx.clone();
            let send_counts = send_counts.clone();
            let fail_peers = fail_peers.clone();
            tx.expect_send_module().returning(
                move |peer, space_id, module_id, data| {
                    assert_eq!(space_id, TEST_SPACE_ID);
                    assert_eq!(module_id, "Fetch");
                    let entry_tx = entry_tx.clone();
                    let error_tx = error_tx.clone();
                    let send_counts = send_counts.clone();
                    let fail_peers = fail_peers.clone();
                    Box::pin(async move {
                        // Entry barrier: signal and hold until released.
                        *send_counts
                            .lock()
                            .unwrap()
                            .entry((peer.clone(), data.to_vec()))
                            .or_insert(0) += 1;
                        let (release_tx, release_rx) = oneshot::channel();
                        entry_tx.send((peer.clone(), release_tx)).unwrap();
                        // Exit barrier: held until the test releases.
                        let _ = release_rx.await;
                        if fail_peers.lock().unwrap().contains(&peer) {
                            error_tx.send(peer).unwrap();
                            Err(K2Error::other("controlled send failure"))
                        } else {
                            Ok(())
                        }
                    })
                },
            );
        }
        let transport: DynTransport = Arc::new(tx);

        let (stored_tx, stored) = mpsc::unbounded_channel();
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
            let signal = stored_tx.clone();
            let store = processing.clone();
            Box::pin(async move {
                let result = store.process_incoming_ops(ops).await;
                signal.send(()).unwrap();
                result
            })
        });

        let report = builder
            .report
            .create(builder.clone(), transport.clone())
            .await
            .unwrap();
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
        let handler = handler.lock().unwrap().take().unwrap();
        Self {
            fetch,
            handler,
            entries,
            errors,
            stored,
            send_counts,
            fail_peers,
            _transport: transport,
        }
    }

    async fn admit(&self, n: u8, peer: u8) -> OpId {
        let op = op(n);
        let id = op.compute_op_id();
        self.fetch
            .request_ops(
                vec![PublishOp {
                    op_id: id.clone(),
                    metadata: None,
                }],
                url(peer),
            )
            .await
            .unwrap();
        id
    }

    async fn entry(&mut self) -> (Url, Release) {
        tokio::time::timeout(Duration::from_secs(5), self.entries.recv())
            .await
            .expect("K3B_SETUP send entry")
            .unwrap()
    }

    /// Overlap assertion: another configured worker must enter a send while
    /// previous sends are still held. This is the assertion the unmodified
    /// baseline is expected to fail.
    async fn overlap(&mut self) -> (Url, Release) {
        println!(
            "K3B_OVERLAP awaiting concurrent send entry while previous send is held"
        );
        tokio::time::timeout(Duration::from_millis(500), self.entries.recv())
            .await
            .expect("K3B_OVERLAP a second configured worker must enter a send while the first is held")
            .unwrap()
    }

    /// Bound assertion: no additional send may enter while all configured
    /// workers are held. Valid on both baseline and candidate.
    async fn no_entry(&mut self) {
        let got = tokio::time::timeout(
            Duration::from_millis(200),
            self.entries.recv(),
        )
        .await;
        assert!(
            got.is_err(),
            "K3B_BOUND no send may enter beyond the configured worker bound"
        );
    }

    async fn pending(&self, id: &OpId) -> bool {
        self.fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .contains_key(id)
    }

    fn assert_single_sends(&self) -> usize {
        let counts = self.send_counts.lock().unwrap();
        assert!(
            counts.values().all(|c| *c == 1),
            "K3B_DOUBLE_SEND each admitted request must be sent at most once: {counts:?}"
        );
        counts.len()
    }
}

fn url(n: u8) -> Url {
    Url::from_str(format!("ws://test:80/{n}")).unwrap()
}

fn op(n: u8) -> MemoryOp {
    MemoryOp::new(Timestamp::now(), vec![n])
}

fn release_all(held: Vec<(Url, Release)>) {
    for (_, release) in held {
        let _ = release.send(());
    }
}

/// Workers = 1: exactly one send entered, the second waits. The bound holds
/// on both baseline and candidate; a single worker legitimately serializes.
#[tokio::test(start_paused = true)]
async fn overlap_workers_1_respects_bound() {
    let mut f = Fixture::new(1).await;
    let a = f.admit(1, 1).await;
    let b = f.admit(2, 2).await;
    let first = f.entry().await;
    f.no_entry().await;
    assert!(f.pending(&a).await);
    assert!(f.pending(&b).await);
    release_all(vec![first]);
    let second = f.entry().await;
    release_all(vec![second]);
    assert_eq!(f.assert_single_sends(), 2);
}

/// Workers = 2, three ready requests on distinct peers: two sends must be
/// entered concurrently (min(2, 3)), never three.
#[tokio::test(start_paused = true)]
async fn overlap_workers_2_enters_two_sends() {
    let mut f = Fixture::new(2).await;
    for n in 1..=3u8 {
        f.admit(n, n).await;
    }
    let mut held = vec![f.entry().await, f.overlap().await];
    assert_ne!(held[0].0, held[1].0);
    f.no_entry().await;
    release_all(std::mem::take(&mut held));
    let third = f.entry().await;
    release_all(vec![third]);
    assert_eq!(f.assert_single_sends(), 3);
}

/// Workers = 5, six ready requests on distinct peers: five sends must be
/// entered concurrently, never six.
#[tokio::test(start_paused = true)]
async fn overlap_workers_5_enters_five_sends() {
    let mut f = Fixture::new(5).await;
    for n in 1..=6u8 {
        f.admit(n, n).await;
    }
    let mut held = vec![f.entry().await];
    for _ in 1..5 {
        held.push(f.overlap().await);
    }
    assert_eq!(held.len(), 5);
    f.no_entry().await;
    release_all(std::mem::take(&mut held));
    let sixth = f.entry().await;
    release_all(vec![sixth]);
    assert_eq!(f.assert_single_sends(), 6);
}

/// Baseline reproduction: with two configured workers, a second request to a
/// different peer must enter its send while the first send is held.
#[tokio::test(start_paused = true)]
async fn different_peer_second_send_enters_while_first_held() {
    let mut f = Fixture::new(2).await;
    let a = f.admit(1, 1).await;
    let b = f.admit(2, 2).await;
    let first = f.entry().await;
    assert_eq!(first.0, url(1));
    let second = f.overlap().await;
    assert_eq!(second.0, url(2));
    assert!(f.pending(&a).await);
    assert!(f.pending(&b).await);
    release_all(vec![first, second]);
    assert_eq!(f.assert_single_sends(), 2);
}

/// Two requests to one peer: CoreFetch itself must not serialize them; any
/// remaining serialization belongs to the transport and is legitimate, not a
/// CoreFetch defect (synthetic transport here imposes none).
#[tokio::test(start_paused = true)]
async fn same_peer_both_core_fetch_sends_enter() {
    let mut f = Fixture::new(2).await;
    f.admit(1, 1).await;
    f.admit(2, 1).await;
    let first = f.entry().await;
    let second = f.overlap().await;
    assert_eq!(first.0, url(1));
    assert_eq!(second.0, url(1));
    release_all(vec![first, second]);
    assert_eq!(f.assert_single_sends(), 2);
}

/// A send error for one peer runs concurrently with an in-flight response for
/// another peer's still-held send. Peer-wide cleanup must remove only the
/// failed peer's requests and the drain waiter must fire once state empties.
#[tokio::test(start_paused = true)]
async fn send_error_concurrent_with_inflight_response() {
    let mut f = Fixture::new(2).await;
    let a = f.admit(1, 1).await;
    let b = f.admit(2, 2).await;
    let first = f.entry().await;
    assert_eq!(first.0, url(1));
    // On the candidate the peer-2 send is already in flight here; on the
    // baseline it is still queued behind the shared receiver guard.

    // Deliver a successful response for op A while its send is still held.
    let ops = vec![op(1)];
    f.handler
        .recv_module_msg(
            url(1),
            TEST_SPACE_ID,
            "Fetch".into(),
            serialize_response_message(
                ops.into_iter().map(Into::into).collect(),
            ),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), f.stored.recv())
        .await
        .expect("K3B_SETUP response processing")
        .unwrap();
    assert!(!f.pending(&a).await, "response must remove op A");
    assert!(f.pending(&b).await, "op B must remain pending");

    // Drain waiter registered while state is non-empty.
    let (notify_tx, notify_rx) = oneshot::channel();
    f.fetch.notify_on_drained(notify_tx);

    // Release the first send; the peer-2 send then runs (on the candidate it
    // was already held in flight) and fails, cleaning up that peer.
    f.fail_peers.lock().unwrap().insert(url(2));
    release_all(vec![first]);
    let second = f.entry().await;
    assert_eq!(second.0, url(2));
    release_all(vec![second]);
    tokio::time::timeout(Duration::from_secs(5), f.errors.recv())
        .await
        .expect("K3B_SETUP controlled send failure signal")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), notify_rx)
        .await
        .expect("K3B_SETUP drain notification after failed-peer cleanup")
        .unwrap();
    assert!(!f.pending(&b).await);
    assert_eq!(f.assert_single_sends(), 2);
}

/// Peer-wide cleanup on send failure racing a newly admitted request to the
/// same peer. With genuinely parallel workers the cleanup can remove the
/// state entry of a request whose send is concurrently in flight. Asserted
/// invariants for both variants: no panic, state empties, drain fires, no
/// double send.
#[tokio::test(start_paused = true)]
async fn send_failure_cleanup_racing_new_admission() {
    let mut f = Fixture::new(2).await;
    let a = f.admit(1, 1).await;
    let b = f.admit(2, 1).await;
    let first = f.entry().await;
    assert!(f.pending(&a).await);
    assert!(f.pending(&b).await);
    // On the candidate the second same-peer send is already in flight; on
    // the baseline it remains queued behind the shared receiver guard.
    let second =
        tokio::time::timeout(Duration::from_millis(200), f.entries.recv())
            .await;
    let (notify_tx, notify_rx) = oneshot::channel();
    f.fetch.notify_on_drained(notify_tx);

    f.fail_peers.lock().unwrap().insert(url(1));
    release_all(vec![first]);
    if let Ok(Some((_, release))) = second {
        release_all(vec![(url(1), release)]);
    }
    tokio::time::timeout(Duration::from_secs(5), notify_rx)
        .await
        .expect("K3B_SETUP drain notification after peer-wide cleanup")
        .unwrap();
    assert!(!f.pending(&a).await);
    assert!(!f.pending(&b).await);
    // The baseline skips the never-admitted second request through the
    // existing absent-ID path (1 send); the candidate has both in flight (2).
    // Both orders are recorded outcomes, not asserted defect behavior.
    assert!(f.assert_single_sends() <= 2);
}

/// Shutdown and worker cancellation with a held send and queued work: the
/// CoreFetch drop aborts all workers; no hang, no double send, no further
/// entries. The held future is dropped at its await point; this is worker
/// cancellation, not a stalled-transport-send release (that is lane K2).
#[tokio::test(start_paused = true)]
async fn shutdown_and_worker_cancellation() {
    let mut f = Fixture::new(2).await;
    f.admit(1, 1).await;
    f.admit(2, 2).await;
    f.admit(3, 3).await;
    let _first = f.entry().await;
    // On the candidate a second worker may already have entered its send;
    // on the baseline it is still queued. Drain any already-entered send
    // before dropping so the post-drop assertion sees only new entries.
    let _entered_before_drop =
        tokio::time::timeout(Duration::from_millis(50), f.entries.recv()).await;
    let counts = f.send_counts.clone();
    let mut entries =
        std::mem::replace(&mut f.entries, mpsc::unbounded_channel().1);
    drop(f);
    let got =
        tokio::time::timeout(Duration::from_millis(200), entries.recv()).await;
    assert!(
        !matches!(got, Ok(Some(_))),
        "K3B_SHUTDOWN no send may enter after drop"
    );
    let counts = counts.lock().unwrap();
    assert!(
        counts.values().all(|c| *c == 1),
        "K3B_DOUBLE_SEND no duplicate sends after cancellation: {counts:?}"
    );
}

/// Configuration gate: `parallel_request_count = 0` is accepted without a
/// validation error. Observed behavior on the pin: with zero workers the
/// shared receiver is dropped when `spawn_tasks` returns, so every admitted
/// op fails its queue send, is silently purged from pending state (with a
/// drain notification) and `request_ops` still returns Ok. Recorded as a
/// named follow-up finding; not silently fixed here.
#[tokio::test(start_paused = true)]
async fn zero_workers_configuration_gate() {
    let mut f = Fixture::new(0).await;
    let a = f.admit(1, 1).await;
    println!(
        "K3B_CONFIG_GATE parallel_request_count=0 accepted by validate_config; \
         admission returns Ok and silently purges pending state (queue send fails)"
    );
    let got =
        tokio::time::timeout(Duration::from_millis(200), f.entries.recv())
            .await;
    assert!(
        got.is_err(),
        "K3B_CONFIG_GATE zero workers must not send anything"
    );
    assert!(
        !f.pending(&a).await,
        "K3B_CONFIG_GATE zero workers silently purges admitted state"
    );
    assert_eq!(f.assert_single_sends(), 0);
}
