//! Real CoreFetch admission ownership; synthetic transport/store barriers.
//! Current-thread continuation makes each exit/store signal precede observation
//! of CoreFetch's synchronous state transition. No sleep/yield setup proofs.
use futures::channel::oneshot;
use kitsune2_api::*;
use kitsune2_core::{default_test_builder, factories::MemoryOp};
use kitsune2_test_utils::space::TEST_SPACE_ID;
use prost::Message;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::mpsc;

const CAPACITY: usize = 16_384;
type Entry = (Vec<OpId>, oneshot::Sender<bool>);

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    core_fetch: Workers,
}
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Workers {
    parallel_request_count: u8,
}

struct Fixture {
    fetch: DynFetch,
    handler: DynTxModuleHandler,
    entries: mpsc::UnboundedReceiver<Entry>,
    exits: mpsc::UnboundedReceiver<()>,
    stored: mpsc::UnboundedReceiver<Vec<OpId>>,
    metadata: Arc<Mutex<Vec<Option<bytes::Bytes>>>>,
    _transport: DynTransport,
}

impl Fixture {
    async fn new(workers: u8) -> Self {
        let builder = default_test_builder().with_default_config().unwrap();
        builder
            .config
            .set_module_config(&Config {
                core_fetch: Workers {
                    parallel_request_count: workers,
                },
            })
            .unwrap();
        let builder = Arc::new(builder);
        let capture = Arc::new(Mutex::new(None));
        let handler = capture.clone();
        let (entry_tx, entries) = mpsc::unbounded_channel();
        let (exit_tx, exits) = mpsc::unbounded_channel();
        let mut transport = MockTransport::new();
        transport
            .expect_register_module_handler()
            .times(1)
            .returning(move |_, _, h| {
                *capture.lock().unwrap() = Some(h);
            });
        transport
            .expect_send_module()
            .returning(move |_, _, _, data| {
                let entry_tx = entry_tx.clone();
                let exit_tx = exit_tx.clone();
                Box::pin(async move {
                    let message = K2FetchMessage::decode(data).unwrap();
                    assert_eq!(
                        message.fetch_message_type(),
                        FetchMessageType::Request
                    );
                    let ids: Vec<OpId> =
                        FetchRequest::decode(message.data).unwrap().into();
                    let (tx, rx) = oneshot::channel();
                    entry_tx.send((ids, tx)).unwrap();
                    let fail = rx.await.unwrap_or(false);
                    exit_tx.send(()).unwrap();
                    if fail {
                        Err(K2Error::other("controlled failure"))
                    } else {
                        Ok(())
                    }
                })
            });
        let transport: DynTransport = Arc::new(transport);
        let (stored_tx, stored) = mpsc::unbounded_channel();
        let metadata = Arc::new(Mutex::new(Vec::new()));
        let capture_meta = metadata.clone();
        let mut store = MockOpStore::new();
        // Deliberately stateless filtering permits an old generation's response
        // followed by re-admission of the same key for the ABA control.
        store
            .expect_filter_out_existing_ops()
            .returning(|ids| Box::pin(async move { Ok(ids) }));
        store.expect_process_incoming_ops().returning(move |ops| {
            let stored_tx = stored_tx.clone();
            let capture_meta = capture_meta.clone();
            Box::pin(async move {
                let ids: Vec<OpId> = ops
                    .into_iter()
                    .map(|op| {
                        capture_meta.lock().unwrap().push(op.metadata.clone());
                        op.op_id
                    })
                    .collect();
                stored_tx.send(ids.clone()).unwrap();
                Ok(ids)
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
            exits,
            stored,
            metadata,
            _transport: transport,
        }
    }
    async fn admit(&self, ops: &[MemoryOp], metadata: Option<bytes::Bytes>) {
        self.fetch
            .request_ops(
                ops.iter()
                    .map(|op| PublishOp {
                        op_id: op.compute_op_id(),
                        metadata: metadata.clone(),
                    })
                    .collect(),
                peer(),
            )
            .await
            .unwrap();
    }
    async fn entry(&mut self) -> Entry {
        recv(&mut self.entries).await
    }
    async fn release(&mut self, entry: Entry, fail: bool) {
        entry.1.send(fail).unwrap();
        recv(&mut self.exits).await;
    }
    async fn complete(&mut self, ops: &[MemoryOp]) {
        self.handler
            .recv_module_msg(
                peer(),
                TEST_SPACE_ID,
                "Fetch".into(),
                serialize_response_message(
                    ops.iter().cloned().map(Into::into).collect(),
                ),
            )
            .unwrap();
        assert_eq!(recv(&mut self.stored).await.len(), ops.len());
    }
    async fn pending(&self) -> HashSet<OpId> {
        self.fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .into_keys()
            .collect()
    }
}
async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("K3C_SETUP barrier")
        .unwrap()
}
fn peer() -> Url {
    Url::from_str("ws://test:80/1").unwrap()
}
fn op(n: usize) -> MemoryOp {
    MemoryOp::new(Timestamp::now(), n.to_le_bytes().to_vec())
}
fn candidate() -> bool {
    std::env::var("K3C_VARIANT").unwrap_or_else(|_| "candidate".into())
        == "candidate"
}

#[tokio::test(start_paused = true)]
async fn cancelled_full_queue_has_no_orphans_and_notifies() {
    let mut f = Fixture::new(1).await;
    let first = op(0);
    f.admit(std::slice::from_ref(&first), None).await;
    let held = f.entry().await;
    let queued: Vec<_> = (1..=CAPACITY).map(op).collect();
    f.admit(&queued, None).await;
    assert_eq!(f.pending().await.len(), CAPACITY + 1);
    // All slots were admitted while the sole consumer is held at send entry.
    assert!(f.entries.try_recv().is_err());
    let cancelled: Vec<_> = (CAPACITY + 1..CAPACITY + 9).map(op).collect();
    let cancelled_ids: HashSet<_> =
        cancelled.iter().map(MemoryOp::compute_op_id).collect();
    let fetch = f.fetch.clone();
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let caller = tokio::spawn(async move {
        let future = fetch.request_ops(
            cancelled
                .into_iter()
                .map(|op| PublishOp {
                    op_id: op.compute_op_id(),
                    metadata: None,
                })
                .collect(),
            peer(),
        );
        futures::pin_mut!(future);
        assert!(matches!(futures::poll!(future.as_mut()), Poll::Pending));
        blocked_tx.send(()).unwrap();
        future.await.unwrap();
    });
    blocked_rx.await.unwrap();
    let before = f.pending().await;
    let published_unadmitted = before.intersection(&cancelled_ids).count();
    assert_eq!(published_unadmitted, if candidate() { 0 } else { 8 });
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let (notify_tx, mut notify_rx) = oneshot::channel();
    f.fetch.notify_on_drained(notify_tx);
    assert_eq!(notify_rx.try_recv().unwrap(), None);
    let mut sent = HashSet::new();
    sent.extend(held.0.iter().cloned());
    f.release(held, false).await;
    for _ in 0..CAPACITY {
        let entry = f.entry().await;
        assert_eq!(entry.0.len(), 1);
        assert!(sent.insert(entry.0[0].clone()));
        f.release(entry, false).await;
    }
    assert_eq!(sent.len(), CAPACITY + 1);
    assert!(sent.is_disjoint(&cancelled_ids));
    assert!(f.entries.try_recv().is_err());
    assert!(f.exits.try_recv().is_err());
    let all: Vec<_> = std::iter::once(first).chain(queued).collect();
    f.complete(&all).await;
    let remaining = f.pending().await;
    assert!(remaining.is_subset(&cancelled_ids));
    let notified = notify_rx.try_recv().unwrap();
    println!(
        "K3C_ACCOUNTING admitted={} sent={} queue=0 in_flight=0 published_unadmitted={} remaining={} notified={notified:?}",
        CAPACITY + 1,
        sent.len(),
        published_unadmitted,
        remaining.len()
    );
    assert!(
        remaining.is_empty(),
        "K3C_ORPHAN cancelled caller left never-admitted pending keys"
    );
    assert_eq!(notified, Some(()));
}

#[tokio::test(start_paused = true)]
async fn duplicates_and_metadata_upgrade_are_bounded() {
    let mut f = Fixture::new(1).await;
    let a = op(1);
    f.admit(std::slice::from_ref(&a), None).await;
    let held = f.entry().await;
    let fetch = f.fetch.clone();
    let id = a.compute_op_id();
    let mut callers = Vec::new();
    for _ in 0..32 {
        let fetch = fetch.clone();
        let id = id.clone();
        callers.push(tokio::spawn(async move {
            fetch
                .request_ops(
                    vec![PublishOp {
                        op_id: id,
                        metadata: Some(bytes::Bytes::from_static(b"upgrade")),
                    }],
                    peer(),
                )
                .await
                .unwrap();
        }));
    }
    for caller in callers {
        caller.await.unwrap();
    }
    f.admit(
        std::slice::from_ref(&a),
        Some(bytes::Bytes::from_static(b"must-not-overwrite")),
    )
    .await;
    assert_eq!(f.pending().await.len(), 1);
    f.release(held, false).await;
    let copies = if candidate() { 1 } else { 34 };
    for _ in 1..copies {
        let entry = f.entry().await;
        f.release(entry, false).await;
    }
    // A sentinel gives a dequeue barrier behind all duplicate attempts.
    let sentinel = op(2);
    f.admit(std::slice::from_ref(&sentinel), None).await;
    let entry = f.entry().await;
    assert_eq!(entry.0, vec![sentinel.compute_op_id()]);
    f.release(entry, false).await;
    f.complete(&[a, sentinel]).await;
    assert_eq!(
        f.metadata.lock().unwrap()[0],
        Some(bytes::Bytes::from_static(b"upgrade"))
    );
    assert!(f.pending().await.is_empty());
    println!("K3C_DUPLICATES callers=34 pending_peak=1 sends={copies}");
}

#[tokio::test(start_paused = true)]
async fn failed_send_does_not_erase_later_same_peer_admission() {
    let mut f = Fixture::new(2).await;
    let a = op(1);
    let b = op(2);
    f.admit(std::slice::from_ref(&a), None).await;
    let old = f.entry().await;
    f.admit(std::slice::from_ref(&b), None).await;
    let newer = f.entry().await;
    f.release(old, true).await;
    let pending = f.pending().await;
    assert_eq!(pending.contains(&b.compute_op_id()), candidate());
    assert!(!pending.contains(&a.compute_op_id()));
    println!(
        "K3C_PEER_FAILURE later_admission_survives={}",
        pending.contains(&b.compute_op_id())
    );
    f.release(newer, false).await;
    f.complete(&[b]).await;
    assert!(f.pending().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn old_generation_failure_does_not_erase_re_admission() {
    let mut f = Fixture::new(2).await;
    let a = op(1);
    f.admit(std::slice::from_ref(&a), None).await;
    let old = f.entry().await;
    f.complete(std::slice::from_ref(&a)).await;
    assert!(f.pending().await.is_empty());
    f.admit(std::slice::from_ref(&a), None).await;
    let newer = f.entry().await;
    f.release(old, true).await;
    assert_eq!(f.pending().await.contains(&a.compute_op_id()), candidate());
    f.release(newer, false).await;
    f.complete(&[a]).await;
    assert!(f.pending().await.is_empty());
}

/// Signal only after polling the real request future to Pending. With the
/// consumer held and the known slot count exhausted, this is admission wait.
async fn blocked_caller(
    fetch: DynFetch,
    ops: Vec<PublishOp>,
) -> tokio::task::JoinHandle<()> {
    let (tx, rx) = oneshot::channel();
    let caller = tokio::spawn(async move {
        let future = fetch.request_ops(ops, peer());
        futures::pin_mut!(future);
        assert!(matches!(futures::poll!(future.as_mut()), Poll::Pending));
        tx.send(()).unwrap();
        future.await.unwrap();
    });
    rx.await.unwrap();
    caller
}

#[tokio::test(start_paused = true)]
async fn partial_batch_cancellation_preserves_admitted_prefix() {
    let mut f = Fixture::new(1).await;
    let first = op(0);
    f.admit(std::slice::from_ref(&first), None).await;
    let held = f.entry().await;
    let queued: Vec<_> = (1..CAPACITY).map(op).collect();
    f.admit(&queued, None).await;
    let batch: Vec<_> = (CAPACITY..CAPACITY + 8).map(op).collect();
    let batch_ids: HashSet<_> =
        batch.iter().map(MemoryOp::compute_op_id).collect();
    let caller = blocked_caller(
        f.fetch.clone(),
        batch
            .iter()
            .map(|op| PublishOp {
                op_id: op.compute_op_id(),
                metadata: None,
            })
            .collect(),
    )
    .await;
    let before = f.pending().await;
    assert_eq!(
        before.intersection(&batch_ids).count(),
        if candidate() { 1 } else { 8 }
    );
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let (tx, mut rx) = oneshot::channel();
    f.fetch.notify_on_drained(tx);
    let mut sent: HashSet<_> = held.0.iter().cloned().collect();
    f.release(held, false).await;
    for _ in 0..CAPACITY {
        let entry = f.entry().await;
        assert_eq!(entry.0.len(), 1);
        assert!(sent.insert(entry.0[0].clone()));
        f.release(entry, false).await;
    }
    // Exactly one member of the cancelled caller's batch was admitted.
    assert_eq!(sent.intersection(&batch_ids).count(), 1);
    assert!(f.entries.try_recv().is_err());
    assert!(f.exits.try_recv().is_err());
    let admitted: Vec<_> = std::iter::once(first)
        .chain(queued)
        .chain(batch)
        .filter(|op| sent.contains(&op.compute_op_id()))
        .collect();
    assert_eq!(admitted.len(), CAPACITY + 1);
    f.complete(&admitted).await;
    let remaining = f.pending().await;
    assert!(remaining.is_subset(&batch_ids));
    assert!(remaining.is_disjoint(&sent));
    let notified = rx.try_recv().unwrap();
    println!(
        "K3C_PARTIAL batch=8 admitted_from_batch=1 sent={} queue=0 in_flight=0 remaining={} notified={notified:?}",
        sent.len(),
        remaining.len()
    );
    assert!(
        remaining.is_empty(),
        "K3C_ORPHAN partial batch cancellation left never-admitted pending keys"
    );
    assert_eq!(notified, Some(()));
}

#[tokio::test(start_paused = true)]
async fn competing_waiters_cancel_without_erasing_survivor() {
    let mut f = Fixture::new(1).await;
    let first = op(0);
    f.admit(std::slice::from_ref(&first), None).await;
    let held = f.entry().await;
    let queued: Vec<_> = (1..=CAPACITY).map(op).collect();
    f.admit(&queued, None).await;
    let target = op(CAPACITY + 1);
    let id = target.compute_op_id();
    let mut callers = Vec::new();
    for metadata in [
        None,
        None,
        Some(bytes::Bytes::from_static(b"waiter-upgrade")),
    ] {
        callers.push(
            blocked_caller(
                f.fetch.clone(),
                vec![PublishOp {
                    op_id: id.clone(),
                    metadata,
                }],
            )
            .await,
        );
    }
    let cancelled = callers.remove(0);
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    f.release(held, false).await;
    for _ in 0..CAPACITY {
        let entry = f.entry().await;
        assert!(!entry.0.contains(&id));
        f.release(entry, false).await;
    }
    for caller in callers {
        caller.await.unwrap();
    }
    // Both survivors passed the pre-reservation check while no generation
    // existed on candidate. The second must deduplicate after reservation.
    let copies = if candidate() { 1 } else { 2 };
    for _ in 0..copies {
        let entry = f.entry().await;
        assert_eq!(entry.0, vec![id.clone()]);
        f.release(entry, false).await;
    }
    let sentinel = op(CAPACITY + 2);
    f.admit(std::slice::from_ref(&sentinel), None).await;
    let entry = f.entry().await;
    assert_eq!(entry.0, vec![sentinel.compute_op_id()]);
    f.release(entry, false).await;
    assert!(f.pending().await.contains(&id));
    f.complete(&[target]).await;
    assert_eq!(
        f.metadata.lock().unwrap()[0],
        Some(bytes::Bytes::from_static(b"waiter-upgrade"))
    );
    let rest: Vec<_> = std::iter::once(first)
        .chain(queued)
        .chain(std::iter::once(sentinel))
        .collect();
    let (tx, mut rx) = oneshot::channel();
    f.fetch.notify_on_drained(tx);
    f.complete(&rest).await;
    assert!(f.pending().await.is_empty());
    assert!(f.entries.try_recv().is_err());
    assert!(f.exits.try_recv().is_err());
    assert_eq!(rx.try_recv().unwrap(), Some(()));
    println!(
        "K3C_COMPETING waiters=3 cancelled=1 survivor_sends={copies} pending=0 queue=0 in_flight=0 metadata=upgraded"
    );
}
