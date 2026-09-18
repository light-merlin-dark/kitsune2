//! Known-peers index types.
//!
//! The [`KnownPeers`] trait indexes the newest accepted agent info for each
//! agent, including agents that are currently blocked. It is used purely for
//! URL → agent-ID resolution by the access control layer so that a blocked
//! agent cannot slip through just because it shares a URL with a non-blocked
//! agent.

use crate::*;
use std::sync::Arc;

/// An index of the newest accepted agent info for each agent, including
/// blocked ones.
///
/// Unlike [`PeerStore`] this store never removes entries on blocking – its
/// only purpose is to let the access control layer resolve the currently
/// accepted URL for agents excluded from the active peer store.
pub trait KnownPeers: 'static + Send + Sync + std::fmt::Debug {
    /// Record agent infos already accepted by the peer store's version check.
    ///
    /// This is called before block and expiry filters are applied, so blocked
    /// or inactive agents remain discoverable by URL.
    fn record(
        &self,
        agent_infos: Vec<Arc<AgentInfoSigned>>,
    ) -> BoxFut<'_, K2Result<()>>;

    /// Look up all known agent IDs reachable at the given URL, regardless of
    /// their current block status.
    fn get_by_url(&self, url: Url) -> BoxFut<'_, K2Result<Vec<AgentId>>>;
}

/// Trait-object [`KnownPeers`].
pub type DynKnownPeers = Arc<dyn KnownPeers>;

/// A factory for constructing [`KnownPeers`] instances.
pub trait KnownPeersFactory: 'static + Send + Sync + std::fmt::Debug {
    /// Help the builder construct a default config from the chosen
    /// module factories.
    fn default_config(&self, config: &mut Config) -> K2Result<()>;

    /// Validate configuration.
    fn validate_config(&self, config: &Config) -> K2Result<()>;

    /// Construct a [`KnownPeers`] instance.
    fn create(
        &self,
        builder: Arc<Builder>,
        space_id: SpaceId,
    ) -> BoxFut<'static, K2Result<DynKnownPeers>>;
}

/// Trait-object [`KnownPeersFactory`].
pub type DynKnownPeersFactory = Arc<dyn KnownPeersFactory>;
