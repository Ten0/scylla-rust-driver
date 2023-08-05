use super::{DefaultPolicy, FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::transport::{cluster::ClusterData, Node};
use std::sync::Arc;
use uuid::Uuid;

/// This policy will always return the same node, unless it is not available anymore, in which case it will
/// fallback to the provided policy.
///
/// This is meant to be used for shard-aware batching.
#[derive(Debug)]
pub struct EnforceTargetNodePolicy {
    target_node: Uuid,
    fallback: Arc<dyn LoadBalancingPolicy>,
}

impl EnforceTargetNodePolicy {
    pub fn new(target_node: &Arc<Node>, fallback: Arc<dyn LoadBalancingPolicy>) -> Self {
        Self {
            target_node: target_node.host_id,
            fallback,
        }
    }
}
impl LoadBalancingPolicy for EnforceTargetNodePolicy {
    fn pick<'a>(&'a self, query: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<NodeRef<'a>> {
        cluster
            .known_peers
            .get(&self.target_node)
            .filter(DefaultPolicy::is_alive)
            .or_else(|| self.fallback.pick(query, cluster))
    }

    fn fallback<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> FallbackPlan<'a> {
        self.fallback.fallback(query, cluster)
    }

    fn name(&self) -> String {
        format!(
            "Enforce target node Load balancing policy - Node: {} - fallback: {}",
            self.target_node,
            self.fallback.name()
        )
    }
}