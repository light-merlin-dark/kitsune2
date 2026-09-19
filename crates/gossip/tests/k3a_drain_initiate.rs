//! Actual gossip initiation + CoreFetch over the default in-memory test transport.
//! Host retrieval is gated until gossip registers its real drain waiter.
use futures::channel::oneshot;
use kitsune2_api::*;
use kitsune2_core::{default_test_builder, factories::MemoryOp};
use kitsune2_gossip::{K2GossipConfig, K2GossipFactory, K2GossipModConfig};
use kitsune2_test_utils::{
    agent::{AgentBuilder, TestLocalAgent},
    space::TEST_SPACE_ID,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc};

#[derive(Debug)]
struct Handler(Mutex<Option<Url>>);
impl TxBaseHandler for Handler {
    fn new_listening_address(&self, url: Url) -> BoxFut<'static, ()> {
        *self.0.lock().unwrap() = Some(url);
        Box::pin(async {})
    }
}
impl TxHandler for Handler {}
impl TxSpaceHandler for Handler {
    fn is_any_agent_at_url_blocked(&self, _: &Url) -> K2Result<bool> {
        Ok(false)
    }
}

#[derive(Debug)]
struct ObservedFetch {
    inner: DynFetch,
    registered: mpsc::UnboundedSender<Instant>,
}
impl Fetch for ObservedFetch {
    fn request_ops(
        &self,
        ops: Vec<PublishOp>,
        source: Url,
    ) -> BoxFut<'_, K2Result<()>> {
        self.inner.request_ops(ops, source)
    }
    fn notify_on_drained(&self, tx: oneshot::Sender<()>) {
        self.inner.notify_on_drained(tx);
        self.registered.send(Instant::now()).unwrap();
    }
    fn get_state_summary(&self) -> BoxFut<'_, K2Result<FetchStateSummary>> {
        self.inner.get_state_summary()
    }
}

#[derive(Debug)]
struct Sink(mpsc::UnboundedSender<Instant>);
impl TxBaseHandler for Sink {}
impl TxModuleHandler for Sink {
    fn recv_module_msg(
        &self,
        _: Url,
        _: SpaceId,
        _: String,
        _: bytes::Bytes,
    ) -> K2Result<()> {
        self.0.send(Instant::now()).unwrap();
        Ok(())
    }
}

async fn transport(builder: Arc<Builder>) -> (DynTransport, Url) {
    let handler = Arc::new(Handler(Mutex::new(None)));
    let tx = builder
        .transport
        .create(builder.clone(), handler.clone())
        .await
        .unwrap();
    tx.register_space_handler(TEST_SPACE_ID, handler.clone())
        .unwrap();
    let url = handler
        .0
        .lock()
        .unwrap()
        .clone()
        .expect("SETUP listening address");
    (tx, url)
}
async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(6), rx.recv())
        .await
        .expect("SETUP barrier")
        .unwrap()
}

#[tokio::test]
async fn arc_growth_completion_advances_initiation() {
    arc_growth_fixture(true).await;
}

#[tokio::test]
async fn pending_fetch_retains_delay_fallback() {
    arc_growth_fixture(false).await;
}

async fn arc_growth_fixture(complete: bool) {
    kitsune2_test_utils::enable_tracing();
    const INITIAL: u32 = 200;
    const NORMAL: u32 = 3000;
    let mut builder = default_test_builder();
    builder.gossip = K2GossipFactory::create();
    let builder = Arc::new(builder.with_default_config().unwrap());
    builder
        .config
        .set_module_config(&K2GossipModConfig {
            k2_gossip: K2GossipConfig {
                initiate_interval_ms: NORMAL,
                initial_initiate_interval_ms: INITIAL,
                min_initiate_interval_ms: 1,
                initiate_jitter_ms: 0,
                ..Default::default()
            },
        })
        .unwrap();
    let (source_tx, source_url) = transport(builder.clone()).await;
    let (dest_tx, _) = transport(builder.clone()).await;
    let operation = MemoryOp::new(Timestamp::now(), vec![42]);
    let op_id = operation.compute_op_id();
    let source_store = builder
        .op_store
        .create(builder.clone(), TEST_SPACE_ID)
        .await
        .unwrap();
    source_store
        .process_incoming_ops(vec![operation.into()])
        .await
        .unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let release = gate.clone();
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let mut gated = MockOpStore::new();
    gated.expect_retrieve_ops().returning(move |ids| {
        let store = source_store.clone();
        let gate = gate.clone();
        let entered = entered_tx.clone();
        Box::pin(async move {
            entered.send(()).unwrap();
            gate.acquire().await.unwrap().forget();
            store.retrieve_ops(ids).await
        })
    });
    let source_meta = builder
        .peer_meta_store
        .create(builder.clone(), TEST_SPACE_ID)
        .await
        .unwrap();
    let source_report = builder
        .report
        .create(builder.clone(), source_tx.clone())
        .await
        .unwrap();
    let _source_fetch = builder
        .fetch
        .create(
            builder.clone(),
            TEST_SPACE_ID,
            source_report,
            Arc::new(gated),
            source_meta,
            source_tx.clone(),
        )
        .await
        .unwrap();
    let store = builder
        .op_store
        .create(builder.clone(), TEST_SPACE_ID)
        .await
        .unwrap();
    let meta = builder
        .peer_meta_store
        .create(builder.clone(), TEST_SPACE_ID)
        .await
        .unwrap();
    let report = builder
        .report
        .create(builder.clone(), dest_tx.clone())
        .await
        .unwrap();
    let fetch = builder
        .fetch
        .create(
            builder.clone(),
            TEST_SPACE_ID,
            report,
            store.clone(),
            meta.clone(),
            dest_tx.clone(),
        )
        .await
        .unwrap();
    fetch
        .request_ops(
            vec![PublishOp {
                op_id: op_id.clone(),
                metadata: None,
            }],
            source_url.clone(),
        )
        .await
        .unwrap();
    println!("K3A_SETUP waiting remote retrieve entry");
    recv(&mut entered).await;
    assert!(
        !fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .is_empty()
    );
    let agents = builder
        .local_agent_store
        .create(builder.clone())
        .await
        .unwrap();
    let agent: DynLocalAgent = Arc::new(TestLocalAgent::default());
    agent.set_tgt_storage_arc_hint(DhtArc::FULL);
    assert_ne!(agent.get_cur_storage_arc(), agent.get_tgt_storage_arc());
    agents.add(agent).await.unwrap();
    let blocks = builder
        .blocks
        .create(builder.clone(), TEST_SPACE_ID)
        .await
        .unwrap();
    let known = builder
        .known_peers
        .create(builder.clone(), TEST_SPACE_ID)
        .await
        .unwrap();
    let peers = builder
        .peer_store
        .create(builder.clone(), TEST_SPACE_ID, blocks, known)
        .await
        .unwrap();
    peers
        .insert(vec![
            AgentBuilder::default()
                .with_url(Some(source_url))
                .build(TestLocalAgent::default()),
        ])
        .await
        .unwrap();
    let (initiated_tx, mut initiated) = mpsc::unbounded_channel();
    source_tx.register_module_handler(
        TEST_SPACE_ID,
        kitsune2_gossip::MOD_NAME.into(),
        Arc::new(Sink(initiated_tx)),
    );
    let (registered_tx, mut registered) = mpsc::unbounded_channel();
    let observed = Arc::new(ObservedFetch {
        inner: fetch.clone(),
        registered: registered_tx,
    });
    let _gossip = builder
        .gossip
        .create(
            builder.clone(),
            TEST_SPACE_ID,
            peers,
            agents,
            meta,
            store.clone(),
            dest_tx.clone(),
            observed,
        )
        .await
        .unwrap();
    println!("K3A_SETUP waiting drain registration");
    let registration = recv(&mut registered).await;
    assert!(
        !fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .is_empty()
    );
    let released = Instant::now();
    if complete {
        release.add_permits(1);
    }
    println!("K3A_SETUP waiting remote initiation");
    let initiation = recv(&mut initiated).await;
    if !complete {
        assert!(
            !fetch
                .get_state_summary()
                .await
                .unwrap()
                .pending_requests
                .is_empty()
        );
        assert!(store.retrieve_ops(vec![op_id]).await.unwrap().is_empty());
        let elapsed = initiation.duration_since(registration);
        assert!(elapsed >= Duration::from_millis(NORMAL as u64));
        println!(
            "K3A_FALLBACK pending=true registration_to_initiation_ms={}",
            elapsed.as_millis()
        );
        return;
    }
    assert!(
        fetch
            .get_state_summary()
            .await
            .unwrap()
            .pending_requests
            .is_empty()
    );
    assert_eq!(store.retrieve_ops(vec![op_id]).await.unwrap().len(), 1);
    let after_release = initiation.duration_since(released);
    let after_registration = initiation.duration_since(registration);
    let baseline = std::env::var("K3A_VARIANT")
        .unwrap_or_else(|_| "candidate".into())
        == "baseline";
    if baseline {
        assert!(
            after_registration >= Duration::from_millis(NORMAL as u64),
            "fallback must retain normal delay"
        );
    } else {
        assert!(
            after_release >= Duration::from_millis(INITIAL as u64),
            "drain must retain initial delay"
        );
    }
    println!(
        "K3A_GOSSIP branch={} registration_to_initiation_ms={} release_to_initiation_ms={} initial_ms={INITIAL} normal_ms={NORMAL}",
        if baseline {
            "delay_expired"
        } else {
            "drain_plus_initial"
        },
        after_registration.as_millis(),
        after_release.as_millis()
    );
    assert!(
        after_release < Duration::from_millis(1500),
        "K3A_GOSSIP_COMPLETION_NOTIFICATION initiation should precede normal fallback"
    );
}
