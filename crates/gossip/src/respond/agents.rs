use crate::error::{K2GossipError, K2GossipResult};
use crate::gossip::K2Gossip;
use crate::protocol::{GossipMessage, K2GossipAgentsMessage};
use crate::state::{GossipRoundState, RoundStage};
use kitsune2_api::{K2Error, Url};

impl K2Gossip {
    pub(super) async fn respond_to_agents(
        &self,
        from_peer: Url,
        agents: K2GossipAgentsMessage,
    ) -> K2GossipResult<Option<GossipMessage>> {
        // Validate the incoming agents message against our own state.
        let mut initiated_lock = self.initiated_round_state.lock().await;
        match initiated_lock.as_ref() {
            Some(state) => {
                state.validate_agents(from_peer.clone(), &agents)?;
                // The session is finished, remove the state.
                initiated_lock.take();
            }
            None => {
                return Err(K2GossipError::peer_behavior(
                    "Unsolicited Agents message",
                ));
            }
        }

        self.receive_agent_infos(agents.provided_agents).await?;

        Ok(None)
    }
}

impl GossipRoundState {
    fn validate_agents(
        &self,
        from_peer: Url,
        agents: &K2GossipAgentsMessage,
    ) -> K2GossipResult<()> {
        if self.session_with_peer != from_peer {
            return Err(K2Error::other(format!(
                "Agents message from wrong peer: {} != {}",
                self.session_with_peer, from_peer
            ))
            .into());
        }

        if self.session_id != agents.session_id {
            return Err(K2GossipError::peer_behavior(format!(
                "Session id mismatch: {:?} != {:?}",
                self.session_id, agents.session_id
            )));
        }

        match &self.stage {
            RoundStage::NoDiff => {
                tracing::trace!(?agents.session_id, "NoDiff round state found");
            }
            stage => {
                return Err(K2GossipError::peer_behavior(format!(
                    "Unexpected round stage for agents: NoDiff != {stage:?}"
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::K2GossipError;
    use crate::protocol::{
        AcceptResponseMessage, GossipMessage, K2GossipAgentsMessage,
        K2GossipNoDiffMessage, encode_agent_infos,
    };
    use crate::respond::harness::{RespondTestHarness, test_session_id};
    use crate::state::{RoundStage, RoundStageAccepted};
    use kitsune2_api::{DhtArc, Timestamp, Url};
    use kitsune2_dht::ArcSet;
    use kitsune2_test_utils::{agent::AgentBuilder, enable_tracing};
    use std::time::Duration;

    #[tokio::test]
    async fn receive_agents() {
        enable_tracing();

        let harness = RespondTestHarness::create().await;

        let local_agent = harness.create_agent(DhtArc::FULL).await;
        let remote_agent = harness.create_agent(DhtArc::FULL).await;

        let session_id = harness
            .insert_initiated_round_state(&local_agent, &remote_agent)
            .await;
        harness
            .gossip
            .initiated_round_state
            .lock()
            .await
            .as_mut()
            .unwrap()
            .stage = RoundStage::NoDiff;

        let discovered_agent_1 = harness.create_agent(DhtArc::Empty).await;
        let discovered_agent_2 = harness.create_agent(DhtArc::FULL).await;

        // Should be nothing in the peer store yet.
        assert!(
            harness
                .gossip
                .peer_store
                .get_all()
                .await
                .unwrap()
                .is_empty()
        );

        let response = harness
            .gossip
            .respond_to_agents(
                remote_agent.url.clone().unwrap(),
                K2GossipAgentsMessage {
                    session_id,
                    provided_agents: encode_agent_infos([
                        discovered_agent_1.agent_info.clone(),
                        discovered_agent_2.agent_info.clone(),
                    ])
                    .unwrap(),
                },
            )
            .await;

        assert!(response.is_ok(), "Response is: {response:?}");

        // Check that the agents were added to the peer store.
        let all_agents = harness.gossip.peer_store.get_all().await.unwrap();
        assert_eq!(all_agents.len(), 2);
        assert!(all_agents.contains(&discovered_agent_1));
        assert!(all_agents.contains(&discovered_agent_2));
    }

    /// A peer can replay an advertisement cached before this node learned a
    /// newer endpoint. Gossip must not let that stale record replace the URL
    /// used to resolve the agent.
    #[tokio::test]
    async fn stale_cached_agent_info_does_not_replace_newer_endpoint() {
        // Alice receives advertisements; Bob and Sue are remote peers.
        let harness = RespondTestHarness::create().await;
        let alice = harness.create_agent(DhtArc::FULL).await;
        let bob = harness.create_agent(DhtArc::FULL).await;
        let sue = harness.create_agent(DhtArc::FULL).await;

        // Sue moves from the old endpoint to the new endpoint.
        let old_url = Url::from_str("ws://test:80/old").unwrap();
        let new_url = Url::from_str("ws://test:80/new").unwrap();
        let now = Timestamp::now();
        let older = AgentBuilder {
            created_at: Some(now),
            url: Some(Some(old_url.clone())),
            ..Default::default()
        }
        .build(sue.local.clone());
        let newer = AgentBuilder {
            created_at: Some(now + Duration::from_secs(1)),
            url: Some(Some(new_url.clone())),
            ..Default::default()
        }
        .build(sue.local.clone());

        // Sue sends Alice her current advertisement.
        let session_id =
            harness.insert_initiated_round_state(&alice, &sue).await;
        harness
            .gossip
            .initiated_round_state
            .lock()
            .await
            .as_mut()
            .unwrap()
            .stage = RoundStage::NoDiff;
        harness
            .gossip
            .respond_to_agents(
                sue.url.clone().unwrap(),
                K2GossipAgentsMessage {
                    session_id,
                    provided_agents: encode_agent_infos([newer.clone()])
                        .unwrap(),
                },
            )
            .await
            .unwrap();

        // Bob later sends Alice his stale cached advertisement for Sue.
        let session_id =
            harness.insert_initiated_round_state(&alice, &bob).await;
        harness
            .gossip
            .initiated_round_state
            .lock()
            .await
            .as_mut()
            .unwrap()
            .stage = RoundStage::NoDiff;
        harness
            .gossip
            .respond_to_agents(
                bob.url.clone().unwrap(),
                K2GossipAgentsMessage {
                    session_id,
                    provided_agents: encode_agent_infos([older]).unwrap(),
                },
            )
            .await
            .unwrap();

        // Alice retains Sue's current advertisement in the peer store.
        assert_eq!(
            harness
                .gossip
                .peer_store
                .get(sue.agent.clone())
                .await
                .unwrap()
                .unwrap(),
            newer
        );

        // Alice resolves Sue only through her current endpoint.
        assert_eq!(
            harness.known_peers.get_by_url(new_url).await.unwrap(),
            vec![sue.agent.clone()]
        );
        assert!(
            harness
                .known_peers
                .get_by_url(old_url)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A stale response selected before a bootstrap update may arrive after it.
    #[tokio::test]
    async fn in_flight_stale_gossip_response_does_not_replace_bootstrap_update()
    {
        // Alice receives updates; Bob caches Sue's old advertisement.
        let alice_harness = RespondTestHarness::create().await;
        let mut bob_harness = RespondTestHarness::create().await;
        let alice = alice_harness.create_agent(DhtArc::FULL).await;
        let bob = bob_harness.create_agent(DhtArc::FULL).await;
        let sue = alice_harness.create_agent(DhtArc::FULL).await;
        let old_url = Url::from_str("ws://test:80/old").unwrap();
        let new_url = Url::from_str("ws://test:80/new").unwrap();
        let now = Timestamp::now();
        let older = AgentBuilder {
            created_at: Some(now),
            url: Some(Some(old_url.clone())),
            ..Default::default()
        }
        .build(sue.local.clone());
        let newer = AgentBuilder {
            created_at: Some(now + Duration::from_secs(1)),
            url: Some(Some(new_url.clone())),
            ..Default::default()
        }
        .build(sue.local.clone());
        bob_harness
            .gossip
            .peer_store
            .insert(vec![older])
            .await
            .unwrap();

        // Alice requests Sue while Bob still has Sue's old advertisement.
        let session_id =
            bob_harness.insert_accepted_round_state(&bob, &alice).await;
        {
            let accepted =
                bob_harness.gossip.accepted_round_states.read().await;
            let mut state = accepted
                .get(alice.url.as_ref().unwrap())
                .unwrap()
                .lock()
                .await;
            state.stage = RoundStage::Accepted(RoundStageAccepted {
                our_agents: vec![sue.agent.clone()],
                common_arc_set: ArcSet::new(vec![DhtArc::FULL]).unwrap(),
            });
        }
        bob_harness
            .gossip
            .respond_to_msg(
                alice.url.clone().unwrap(),
                GossipMessage::NoDiff(K2GossipNoDiffMessage {
                    session_id: session_id.clone(),
                    accept_response: Some(AcceptResponseMessage {
                        missing_agents: vec![sue.agent.0.clone().into()],
                        provided_agents: vec![],
                        new_ops: vec![],
                        updated_new_since: now.as_micros(),
                    }),
                    cannot_compare: false,
                }),
            )
            .await
            .unwrap();
        let stale_response = bob_harness.wait_for_sent_response().await;
        assert!(matches!(stale_response, GossipMessage::Agents(_)));

        // Alice learns Sue's new advertisement while Bob's response is in flight.
        alice_harness
            .gossip
            .peer_store
            .insert(vec![newer.clone()])
            .await
            .unwrap();

        // Bob's delayed response delivers Sue's old advertisement to Alice.
        alice_harness
            .insert_initiated_round_state(&alice, &bob)
            .await;
        {
            let mut state =
                alice_harness.gossip.initiated_round_state.lock().await;
            let state = state.as_mut().unwrap();
            state.session_id = session_id;
            state.stage = RoundStage::NoDiff;
        }
        alice_harness
            .gossip
            .respond_to_msg(bob.url.clone().unwrap(), stale_response)
            .await
            .unwrap();

        // Alice's peer store retains Sue's new advertisement.
        assert_eq!(
            alice_harness
                .gossip
                .peer_store
                .get(sue.agent.clone())
                .await
                .unwrap()
                .unwrap(),
            newer
        );

        // Alice resolves Sue only through the new endpoint.
        assert_eq!(
            alice_harness.known_peers.get_by_url(new_url).await.unwrap(),
            vec![sue.agent.clone()]
        );
        assert!(
            alice_harness
                .known_peers
                .get_by_url(old_url)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn receive_agents_from_wrong_peer() {
        enable_tracing();

        let harness = RespondTestHarness::create().await;

        let local_agent = harness.create_agent(DhtArc::FULL).await;
        let remote_agent = harness.create_agent(DhtArc::FULL).await;

        let session_id = harness
            .insert_initiated_round_state(&local_agent, &remote_agent)
            .await;
        harness
            .gossip
            .initiated_round_state
            .lock()
            .await
            .as_mut()
            .unwrap()
            .stage = RoundStage::NoDiff;

        let discovered_agent_1 = harness.create_agent(DhtArc::Empty).await;
        let discovered_agent_2 = harness.create_agent(DhtArc::FULL).await;

        let error = harness
            .gossip
            .respond_to_agents(
                harness
                    .create_agent(DhtArc::Empty)
                    .await
                    .url
                    .clone()
                    .unwrap(),
                K2GossipAgentsMessage {
                    session_id,
                    provided_agents: encode_agent_infos([
                        discovered_agent_1.agent_info.clone(),
                        discovered_agent_2.agent_info.clone(),
                    ])
                    .unwrap(),
                },
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("Agents message from wrong peer"),
            "Expected error for agents message from wrong peer, got: {error:?}"
        );

        // Should not have added any agents to the peer store,
        let all_agents = harness.gossip.peer_store.get_all().await.unwrap();
        assert!(all_agents.is_empty());
    }

    #[tokio::test]
    async fn receive_agents_with_mismatched_session_id() {
        enable_tracing();

        let harness = RespondTestHarness::create().await;

        let local_agent = harness.create_agent(DhtArc::FULL).await;
        let remote_agent = harness.create_agent(DhtArc::FULL).await;

        harness
            .insert_initiated_round_state(&local_agent, &remote_agent)
            .await;
        harness
            .gossip
            .initiated_round_state
            .lock()
            .await
            .as_mut()
            .unwrap()
            .stage = RoundStage::NoDiff;

        let discovered_agent_1 = harness.create_agent(DhtArc::Empty).await;
        let discovered_agent_2 = harness.create_agent(DhtArc::FULL).await;

        let error = harness
            .gossip
            .respond_to_agents(
                remote_agent.url.clone().unwrap(),
                K2GossipAgentsMessage {
                    session_id: test_session_id(),
                    provided_agents: encode_agent_infos([
                        discovered_agent_1.agent_info.clone(),
                        discovered_agent_2.agent_info.clone(),
                    ])
                    .unwrap(),
                },
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("Session id mismatch"),
            "Expected error for mismatched session id, got: {error:?}"
        );

        // Should not have added any agents to the peer store,
        let all_agents = harness.gossip.peer_store.get_all().await.unwrap();
        assert!(all_agents.is_empty());
    }

    #[tokio::test]
    async fn receive_agents_at_wrong_stage() {
        enable_tracing();

        let harness = RespondTestHarness::create().await;

        let local_agent = harness.create_agent(DhtArc::FULL).await;
        let remote_agent = harness.create_agent(DhtArc::FULL).await;

        let session_id = harness
            .insert_initiated_round_state(&local_agent, &remote_agent)
            .await;

        let discovered_agent_1 = harness.create_agent(DhtArc::Empty).await;
        let discovered_agent_2 = harness.create_agent(DhtArc::FULL).await;

        let error = harness
            .gossip
            .respond_to_agents(
                remote_agent.url.clone().unwrap(),
                K2GossipAgentsMessage {
                    session_id,
                    provided_agents: encode_agent_infos([
                        discovered_agent_1.agent_info.clone(),
                        discovered_agent_2.agent_info.clone(),
                    ])
                    .unwrap(),
                },
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains(
                "Unexpected round stage for agents: NoDiff != Initiated"
            ),
            "Expected error for wrong round stage, got: {error:?}"
        );

        // Should not have added any agents to the peer store,
        let all_agents = harness.gossip.peer_store.get_all().await.unwrap();
        assert!(all_agents.is_empty());
    }

    #[tokio::test]
    async fn receive_agents_with_no_initiated_session() {
        enable_tracing();

        let harness = RespondTestHarness::create().await;

        let remote_agent = harness.create_agent(DhtArc::FULL).await;

        let discovered_agent_1 = harness.create_agent(DhtArc::Empty).await;
        let discovered_agent_2 = harness.create_agent(DhtArc::FULL).await;

        let error = harness
            .gossip
            .respond_to_agents(
                remote_agent.url.clone().unwrap(),
                K2GossipAgentsMessage {
                    session_id: test_session_id(),
                    provided_agents: encode_agent_infos([
                        discovered_agent_1.agent_info.clone(),
                        discovered_agent_2.agent_info.clone(),
                    ])
                    .unwrap(),
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, K2GossipError::PeerBehaviorError { .. }),
            "Expected PeerBehavior error for unsolicited Agents message, got: {error:?}"
        );

        // Should not have added any agents to the peer store,
        let all_agents = harness.gossip.peer_store.get_all().await.unwrap();
        assert!(all_agents.is_empty());
    }
}
