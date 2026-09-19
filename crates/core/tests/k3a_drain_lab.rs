//! Real CoreFetch factory and registered wire handler; synthetic host/transport.
//! All tests use a current-thread runtime. A send/store future signals its
//! completion and returns Ready in the same poll. CoreFetch then runs its
//! synchronous continuation through to the next empty-channel await before this
//! test can resume. No sleeps/yields establish setup or completion.
use futures::channel::oneshot;
use kitsune2_api::*;
use kitsune2_core::{default_test_builder, factories::MemoryOp};
use kitsune2_test_utils::space::TEST_SPACE_ID;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc;

struct Fixture {
    fetch: DynFetch,
    handler: DynTxModuleHandler,
    sent: mpsc::UnboundedReceiver<()>,
    stored: mpsc::UnboundedReceiver<()>,
    fail: Arc<AtomicBool>,
    backing: DynOpStore,
    _transport: DynTransport,
}

impl Fixture {
    async fn new() -> Self {
        let builder =
            Arc::new(default_test_builder().with_default_config().unwrap());
        let (sent_tx, sent) = mpsc::unbounded_channel();
        let handler = Arc::new(Mutex::new(None));
        let capture = handler.clone();
        let mut tx = MockTransport::new();
        tx.expect_register_module_handler().times(1).returning(
            move |_, _, h| {
                *capture.lock().unwrap() = Some(h);
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
        let (stored_tx, stored) = mpsc::unbounded_channel();
        let fail = Arc::new(AtomicBool::new(false));
        let failure = fail.clone();
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
            let failure = failure.clone();
            let store = processing.clone();
            Box::pin(async move {
                let result = if failure.load(Ordering::SeqCst) {
                    Err(K2Error::other("controlled store error"))
                } else {
                    store.process_incoming_ops(ops).await
                };
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
            sent,
            stored,
            fail,
            backing,
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
            tokio::time::timeout(Duration::from_secs(2), self.sent.recv())
                .await
                .expect("SETUP send completion")
                .unwrap();
        }
        // Every admitted item has been dequeued and its send returned Ready.
        // On this current-thread runtime no outgoing work remains runnable.
        assert!(self.sent.try_recv().is_err());
        assert!(
            !self
                .fetch
                .get_state_summary()
                .await
                .unwrap()
                .pending_requests
                .is_empty()
        );
    }

    fn waiter(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.fetch.notify_on_drained(tx);
        rx
    }

    async fn respond(&mut self, ops: Vec<MemoryOp>, peer: u8) {
        let ids: Vec<_> = ops.iter().map(MemoryOp::compute_op_id).collect();
        self.handler
            .recv_module_msg(
                url(peer),
                TEST_SPACE_ID,
                "Fetch".into(),
                serialize_response_message(
                    ops.into_iter().map(Into::into).collect(),
                ),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), self.stored.recv())
            .await
            .expect("SETUP store completion")
            .unwrap();
        let stored = self.backing.retrieve_ops(ids.clone()).await.unwrap();
        assert_eq!(
            stored.len(),
            if self.fail.load(Ordering::SeqCst) {
                0
            } else {
                ids.len()
            }
        );
    }

    async fn empty(&self) -> bool {
        self.fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .is_empty()
    }
}

fn url(n: u8) -> Url {
    Url::from_str(format!("ws://test:80/{n}")).unwrap()
}
fn op(n: u8) -> MemoryOp {
    MemoryOp::new(Timestamp::now(), vec![n])
}
async fn completion(rx: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_millis(100), rx)
        .await
        .expect("K3A_COMPLETION_NOTIFICATION after successful store and empty pending map")
        .unwrap();
}

#[tokio::test]
async fn zero_requests() {
    let f = Fixture::new().await;
    assert!(f.empty().await);
    assert_eq!(f.waiter().try_recv().unwrap(), Some(()));
}

#[tokio::test]
async fn final_response_wakes_existing_waiter() {
    let mut f = Fixture::new().await;
    let a = op(1);
    f.admit(std::slice::from_ref(&a), 1).await;
    let mut old = f.waiter();
    assert_eq!(old.try_recv().unwrap(), None);
    f.respond(vec![a], 1).await;
    assert!(f.empty().await);
    assert_eq!(f.waiter().try_recv().unwrap(), Some(()));
    let observed = old.try_recv().unwrap();
    println!(
        "K3A_OBSERVATION empty=true new_waiter=ready existing_waiter={observed:?}"
    );
    assert_eq!(
        observed,
        Some(()),
        "K3A_COMPLETION_NOTIFICATION after successful store and empty pending map"
    );
}

#[tokio::test]
async fn completion_notification_all_waiters_and_dropped_receiver() {
    let mut f = Fixture::new().await;
    let a = op(2);
    f.admit(std::slice::from_ref(&a), 1).await;
    let one = f.waiter();
    let two = f.waiter();
    drop(f.waiter());
    f.respond(vec![a], 1).await;
    assert!(f.empty().await);
    assert_eq!(f.waiter().try_recv().unwrap(), Some(()));
    completion(one).await;
    completion(two).await;
    let b = op(3);
    f.admit(std::slice::from_ref(&b), 1).await;
    let mut later = f.waiter();
    assert_eq!(later.try_recv().unwrap(), None);
    f.respond(vec![b], 1).await;
    completion(later).await;
}

#[tokio::test]
async fn partial_error_and_multiple_peers() {
    let mut f = Fixture::new().await;
    let a = op(4);
    let b = op(5);
    f.admit(&[a.clone(), b.clone()], 1).await;
    f.admit(std::slice::from_ref(&a), 2).await;
    assert_eq!(
        f.fetch.get_state_summary().await.unwrap().pending_requests
            [&a.compute_op_id()]
            .len(),
        2
    );
    let mut waiter = f.waiter();
    f.fail.store(true, Ordering::SeqCst);
    f.respond(vec![a.clone()], 1).await;
    assert_eq!(
        f.fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .len(),
        2
    );
    assert_eq!(waiter.try_recv().unwrap(), None);
    f.fail.store(false, Ordering::SeqCst);
    f.respond(vec![a], 2).await;
    let state = f.fetch.get_state_summary().await.unwrap();
    assert_eq!(state.pending_requests.len(), 1);
    assert!(state.pending_requests.contains_key(&b.compute_op_id()));
    assert_eq!(waiter.try_recv().unwrap(), None);
    f.respond(vec![b], 1).await;
    assert!(f.empty().await);
    assert_eq!(f.waiter().try_recv().unwrap(), Some(()));
    completion(waiter).await;
}

/// A real OS thread races registration against the response task. The mutex
/// admits either order. A guaranteed pre-existing waiter makes baseline failure
/// deterministic, independently of which order the racing registration takes.
#[tokio::test]
async fn registration_racing_completion() {
    let mut f = Fixture::new().await;
    let a = op(6);
    f.admit(std::slice::from_ref(&a), 1).await;
    let old = f.waiter();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let remote_barrier = barrier.clone();
    let fetch = f.fetch.clone();
    let thread = std::thread::spawn(move || {
        remote_barrier.wait();
        let (tx, rx) = oneshot::channel();
        fetch.notify_on_drained(tx);
        rx
    });
    f.handler
        .recv_module_msg(
            url(1),
            TEST_SPACE_ID,
            "Fetch".into(),
            serialize_response_message(vec![a.into()]),
        )
        .unwrap();
    barrier.wait();
    tokio::time::timeout(Duration::from_secs(2), f.stored.recv())
        .await
        .expect("SETUP store completion")
        .unwrap();
    let racing = thread.join().unwrap();
    assert!(f.empty().await);
    assert_eq!(f.waiter().try_recv().unwrap(), Some(()));
    completion(old).await;
    completion(racing).await;

    // Readiness is not quiescence: race a new registration with admission
    // after the previous drain. Either immediate readiness or pending is legal.
    let b = op(7);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let remote_barrier = barrier.clone();
    let fetch = f.fetch.clone();
    let thread = std::thread::spawn(move || {
        remote_barrier.wait();
        let (tx, rx) = oneshot::channel();
        fetch.notify_on_drained(tx);
        rx
    });
    barrier.wait();
    f.admit(std::slice::from_ref(&b), 1).await;
    let mut racing = thread.join().unwrap();
    let fired_before = racing.try_recv().unwrap().is_some();
    let mut after_admission = f.waiter();
    assert_eq!(after_admission.try_recv().unwrap(), None);
    f.respond(vec![b], 1).await;
    completion(after_admission).await;
    if !fired_before {
        completion(racing).await;
    }
    assert!(f.empty().await);
}
