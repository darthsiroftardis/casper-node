//! Block proposer.
//!
//! The block proposer stores deploy hashes in memory, tracking their suitability for inclusion into
//! a new block. Upon request, it returns a list of candidates that can be included.

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    fmt::{self, Display, Formatter},
    time::Duration,
};

use datasize::DataSize;
use derive_more::From;
use prometheus::{self, IntGauge, Registry};
use semver::Version;
use tracing::{error, info, trace};

use crate::{
    components::{chainspec_loader::DeployConfig, Component},
    effect::{
        requests::{BlockProposerRequest, StorageRequest},
        EffectBuilder, EffectExt, Effects, Responder,
    },
    types::{DeployHash, DeployHeader, ProtoBlock, ProtoBlockHash, Timestamp},
    NodeRng,
};

const PRUNE_INTERVAL: Duration = Duration::from_secs(10);

/// An event for when using the block proposer as a component.
#[derive(Debug, From)]
pub enum Event {
    #[from]
    Request(BlockProposerRequest),
    /// A new deploy should be buffered.
    Buffer {
        hash: DeployHash,
        header: Box<DeployHeader>,
    },
    /// The deploy-buffer has been asked to prune stale deploys
    BufferPrune,
    /// A proto block has been proposed. We should not propose duplicates of its deploys.
    ProposedProtoBlock(ProtoBlock),
    /// A proto block has been finalized. We should never propose its deploys again.
    FinalizedProtoBlock(ProtoBlock),
    /// A proto block has been orphaned. Its deploys should be re-proposed.
    OrphanedProtoBlock(ProtoBlock),
    /// The result of the `BlockProposer` getting the chainspec from the storage component.
    GetChainspecResult {
        maybe_deploy_config: Box<Option<DeployConfig>>,
        chainspec_version: Version,
        block_timestamp: Timestamp,
        past_blocks: HashSet<ProtoBlockHash>,
        responder: Responder<HashSet<DeployHash>>,
    },
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Event::BufferPrune => write!(f, "buffer prune"),
            Event::Request(req) => write!(f, "deploy-buffer request: {}", req),
            Event::Buffer { hash, .. } => write!(f, "deploy-buffer add {}", hash),
            Event::ProposedProtoBlock(block) => {
                write!(f, "deploy-buffer proposed proto block {}", block)
            }
            Event::FinalizedProtoBlock(block) => {
                write!(f, "deploy-buffer finalized proto block {}", block)
            }
            Event::OrphanedProtoBlock(block) => {
                write!(f, "deploy-buffer orphaned proto block {}", block)
            }
            Event::GetChainspecResult {
                maybe_deploy_config,
                ..
            } => {
                if maybe_deploy_config.is_some() {
                    write!(f, "deploy-buffer got chainspec")
                } else {
                    write!(f, "deploy-buffer failed to get chainspec")
                }
            }
        }
    }
}

type DeployCollection = HashMap<DeployHash, DeployHeader>;
pub type ProtoBlockCollection = HashMap<ProtoBlockHash, DeployCollection>;

pub(crate) trait ReactorEventT: From<Event> + From<StorageRequest> + Send + 'static {}

impl<REv> ReactorEventT for REv where REv: From<Event> + From<StorageRequest> + Send + 'static {}

/// Stores the internal state of the BlockProposer.
#[derive(DataSize, Default, Debug, Clone, PartialEq)]
pub(crate) struct BlockProposerState {
    pending: DeployCollection,
    proposed: ProtoBlockCollection,
    finalized: ProtoBlockCollection,
}

impl Display for BlockProposerState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "(pending:{}, proposed:{}, finalized:{})",
            self.pending.len(),
            self.proposed.len(),
            self.finalized.len()
        )
    }
}

impl BlockProposerState {
    /// Prunes expired deploy information from the BlockProposerState, returns the total deploys
    /// pruned
    pub(crate) fn prune(&mut self, current_instant: Timestamp) -> usize {
        let pending = prune::prune_deploys(&mut self.pending, current_instant);
        let proposed = prune::prune_blocks(&mut self.proposed, current_instant);
        let finalized = prune::prune_blocks(&mut self.finalized, current_instant);
        pending + proposed + finalized
    }
}

mod prune {
    use super::*;

    /// Prunes expired deploy information from an individual DeployCollection, returns the total
    /// deploys pruned
    pub(super) fn prune_deploys(
        deploys: &mut DeployCollection,
        current_instant: Timestamp,
    ) -> usize {
        let initial_len = deploys.len();
        deploys.retain(|_hash, header| !header.expired(current_instant));
        initial_len - deploys.len()
    }

    /// Prunes expired deploy information from each ProtoBlockCollection, returns the total
    /// deploys pruned
    pub(super) fn prune_blocks(
        blocks: &mut ProtoBlockCollection,
        current_instant: Timestamp,
    ) -> usize {
        let mut pruned = 0;
        let mut remove = Vec::new();
        for (block_hash, deploys) in blocks.iter_mut() {
            pruned += prune_deploys(deploys, current_instant);
            if deploys.is_empty() {
                remove.push(*block_hash);
            }
        }
        remove.iter().for_each(|block_hash| {
            blocks.remove(block_hash);
        });
        pruned
    }
}

/// Block proposer.
#[derive(DataSize, Debug, Clone)]
pub(crate) struct BlockProposer {
    state: BlockProposerState,
    #[data_size(skip)]
    metrics: BlockProposerMetrics,
    // We don't need the whole Chainspec here (it's also unnecessarily big), just the deploy
    // config.
    #[data_size(skip)]
    chainspecs: HashMap<Version, DeployConfig>,
}

impl BlockProposer {
    /// Creates a new, block proposer instance with the provided internal state.
    pub(crate) fn new<REv>(
        registry: Registry,
        effect_builder: EffectBuilder<REv>,
        state: BlockProposerState,
    ) -> Result<(Self, Effects<Event>), prometheus::Error>
    where
        REv: ReactorEventT,
    {
        let effects = effect_builder
            .set_timeout(PRUNE_INTERVAL)
            .event(|_| Event::BufferPrune);

        let metrics = BlockProposerMetrics::new(registry)?;
        let this = BlockProposer {
            metrics,
            state,
            chainspecs: HashMap::new(),
        };
        Ok((this, effects))
    }

    /// Adds a deploy to the block proposer.
    ///
    /// Returns `false` if the deploy has been rejected.
    fn add_deploy(&mut self, current_instant: Timestamp, hash: DeployHash, header: DeployHeader) {
        if header.expired(current_instant) {
            trace!("expired deploy {} rejected from the buffer", hash);
            return;
        }
        // only add the deploy if it isn't contained in a finalized block
        if !self
            .state
            .finalized
            .values()
            .any(|block| block.contains_key(&hash))
        {
            self.state.pending.insert(hash, header);
            info!("added deploy {} to the buffer", hash);
        } else {
            info!("deploy {} rejected from the buffer", hash);
        }
    }

    /// Gets the chainspec from the cache or, if not cached, from the storage.
    fn get_chainspec<REv>(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        block_timestamp: Timestamp,
        past_blocks: HashSet<ProtoBlockHash>,
        responder: Responder<HashSet<DeployHash>>,
    ) -> Effects<Event>
    where
        REv: ReactorEventT,
    {
        // TODO - should the current protocol version be passed in here?
        let chainspec_version = Version::from((1, 0, 0));
        let cached_chainspec = self.chainspecs.get(&chainspec_version).cloned();
        match cached_chainspec {
            Some(chainspec) => {
                effect_builder
                    .immediately()
                    .event(move |_| Event::GetChainspecResult {
                        maybe_deploy_config: Box::new(Some(chainspec)),
                        chainspec_version,
                        block_timestamp,
                        past_blocks,
                        responder,
                    })
            }
            None => self.get_chainspec_from_storage(
                effect_builder,
                chainspec_version,
                block_timestamp,
                past_blocks,
                responder,
            ),
        }
    }

    /// Gets the chainspec from storage in order to call `remaining_deploys()`.
    fn get_chainspec_from_storage<REv: ReactorEventT>(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        chainspec_version: Version,
        block_timestamp: Timestamp,
        past_blocks: HashSet<ProtoBlockHash>,
        responder: Responder<HashSet<DeployHash>>,
    ) -> Effects<Event>
    where
        REv: From<StorageRequest> + Send,
    {
        effect_builder
            .get_chainspec(chainspec_version.clone())
            .event(move |maybe_chainspec| Event::GetChainspecResult {
                maybe_deploy_config: Box::new(maybe_chainspec.map(|c| c.genesis.deploy_config)),
                chainspec_version,
                block_timestamp,
                past_blocks,
                responder,
            })
    }

    /// Returns a list of candidates for inclusion into a block.
    /// rename to proposed deploys
    /// maybe use cuckoofilter
    fn remaining_deploys(
        &mut self,
        deploy_config: DeployConfig,
        block_timestamp: Timestamp,
        past_blocks: HashSet<ProtoBlockHash>,
    ) -> HashSet<DeployHash> {
        let past_deploys = past_blocks
            .iter()
            .filter_map(|block_hash| self.state.proposed.get(block_hash))
            .chain(self.state.finalized.values())
            .flat_map(|deploys| deploys.keys())
            .collect::<HashSet<_>>();

        // deploys_to_return = all deploys in pending that aren't in finalized blocks or
        // proposed blocks from the set `past_blocks`
        self.state
            .pending
            .iter()
            .filter(|&(hash, deploy)| {
                self.is_deploy_valid(deploy, block_timestamp, &deploy_config, &past_deploys)
                    && !past_deploys.contains(hash)
            })
            .map(|(hash, _deploy)| *hash)
            .take(deploy_config.block_max_deploy_count as usize)
            .collect::<HashSet<_>>()
        // TODO: check gas and block size limits
    }

    /// Checks if a deploy is valid (for inclusion into the next block).
    fn is_deploy_valid(
        &self,
        deploy: &DeployHeader,
        block_timestamp: Timestamp,
        deploy_config: &DeployConfig,
        past_deploys: &HashSet<&DeployHash>,
    ) -> bool {
        let all_deps_resolved = || {
            deploy
                .dependencies()
                .iter()
                .all(|dep| past_deploys.contains(dep))
        };
        let ttl_valid = deploy.ttl() <= deploy_config.max_ttl;
        let timestamp_valid = deploy.timestamp() <= block_timestamp;
        let deploy_valid = deploy.timestamp() + deploy.ttl() >= block_timestamp;
        let num_deps_valid = deploy.dependencies().len() <= deploy_config.max_dependencies as usize;
        ttl_valid && timestamp_valid && deploy_valid && num_deps_valid && all_deps_resolved()
    }

    /// Notifies the block proposer of a new block that has been proposed, so that the block's
    /// deploys are not returned again by `remaining_deploys`.
    fn added_block<I>(&mut self, block: ProtoBlockHash, deploys: I)
    where
        I: IntoIterator<Item = DeployHash>,
    {
        // TODO: This will ignore deploys that weren't in `pending`. They might be added
        // later, and then would be proposed as duplicates.
        let deploy_map: HashMap<_, _> = deploys
            .into_iter()
            .filter_map(|deploy_hash| {
                self.state
                    .pending
                    .get(&deploy_hash)
                    .map(|deploy| (deploy_hash, deploy.clone()))
            })
            .collect();
        self.state
            .pending
            .retain(|deploy_hash, _| !deploy_map.contains_key(deploy_hash));
        self.state.proposed.insert(block, deploy_map);
    }

    /// Notifies the block proposer that a block has been finalized.
    fn finalized_block(&mut self, block: ProtoBlockHash) {
        if let Some(deploys) = self.state.proposed.remove(&block) {
            self.state
                .pending
                .retain(|deploy_hash, _| !deploys.contains_key(deploy_hash));
            self.state.finalized.insert(block, deploys);
        } else if !block.is_empty() {
            // TODO: Events are not guaranteed to be handled in order, so this could happen!
            error!("finalized block that hasn't been proposed!");
        }
    }

    /// Notifies the block proposer that a block has been orphaned.
    fn orphaned_block(&mut self, block: ProtoBlockHash) {
        if let Some(deploys) = self.state.proposed.remove(&block) {
            self.state.pending.extend(deploys);
        } else {
            // TODO: Events are not guaranteed to be handled in order, so this could happen!
            error!("orphaned block that hasn't been proposed!");
        }
    }

    /// Prunes expired deploy information from the BlockProposer, returns the total deploys pruned
    fn prune(&mut self, current_instant: Timestamp) -> usize {
        self.state.prune(current_instant)
    }
}

impl<REv> Component<REv> for BlockProposer
where
    REv: ReactorEventT,
{
    type Event = Event;
    type ConstructionError = Infallible;

    fn handle_event(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        _rng: &mut NodeRng,
        event: Self::Event,
    ) -> Effects<Self::Event> {
        self.metrics
            .pending_deploys
            .set(self.state.pending.len() as i64);
        match event {
            Event::BufferPrune => {
                let pruned = self.prune(Timestamp::now());
                log::debug!("Pruned {} deploys from buffer", pruned);
                return effect_builder
                    .set_timeout(PRUNE_INTERVAL)
                    .event(|_| Event::BufferPrune);
            }
            Event::Request(BlockProposerRequest::ListForInclusion {
                current_instant,
                past_blocks,
                responder,
            }) => {
                return self.get_chainspec(effect_builder, current_instant, past_blocks, responder);
            }
            Event::Buffer { hash, header } => self.add_deploy(Timestamp::now(), hash, *header),
            Event::ProposedProtoBlock(block) => {
                let (hash, deploys, _) = block.destructure();
                self.added_block(hash, deploys)
            }
            Event::FinalizedProtoBlock(block) => self.finalized_block(*block.hash()),
            Event::OrphanedProtoBlock(block) => self.orphaned_block(*block.hash()),
            Event::GetChainspecResult {
                maybe_deploy_config,
                chainspec_version,
                block_timestamp,
                past_blocks,
                responder,
            } => {
                let deploy_config = maybe_deploy_config.expect("should return chainspec");
                // Update chainspec cache.
                self.chainspecs.insert(chainspec_version, deploy_config);
                let deploys = self.remaining_deploys(deploy_config, block_timestamp, past_blocks);
                return responder.respond(deploys).ignore();
            }
        }
        Effects::new()
    }
}

#[derive(Debug, Clone)]
pub struct BlockProposerMetrics {
    /// Amount of pending deploys
    pending_deploys: IntGauge,
    /// registry Component.
    registry: Registry,
}

impl BlockProposerMetrics {
    pub fn new(registry: Registry) -> Result<Self, prometheus::Error> {
        let pending_deploys = IntGauge::new("pending_deploy", "amount of pending deploys")?;
        registry.register(Box::new(pending_deploys.clone()))?;
        Ok(BlockProposerMetrics {
            pending_deploys,
            registry,
        })
    }
}

impl Drop for BlockProposerMetrics {
    fn drop(&mut self) {
        self.registry
            .unregister(Box::new(self.pending_deploys.clone()))
            .expect("did not expect deregistering pending_deploys to fail");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use casper_execution_engine::core::engine_state::executable_deploy_item::ExecutableDeployItem;
    use rand::random;

    use super::*;
    use crate::{
        crypto::{asymmetric_key::SecretKey, hash::hash},
        reactor::{EventQueueHandle, QueueKind, Scheduler},
        testing::TestRng,
        types::{Deploy, DeployHash, DeployHeader, ProtoBlockHash, TimeDiff},
        utils,
    };

    fn generate_deploy(
        rng: &mut TestRng,
        timestamp: Timestamp,
        ttl: TimeDiff,
        dependencies: Vec<DeployHash>,
    ) -> (DeployHash, DeployHeader) {
        let secret_key = SecretKey::random(rng);
        let gas_price = 10;
        let chain_name = "chain".to_string();
        let payment = ExecutableDeployItem::ModuleBytes {
            module_bytes: vec![],
            args: vec![],
        };
        let session = ExecutableDeployItem::ModuleBytes {
            module_bytes: vec![],
            args: vec![],
        };

        let deploy = Deploy::new(
            timestamp,
            ttl,
            gas_price,
            dependencies,
            chain_name,
            payment,
            session,
            &secret_key,
            rng,
        );

        (*deploy.id(), deploy.take_header())
    }

    fn create_test_buffer() -> (BlockProposer, Effects<Event>) {
        let registry = Registry::new();
        let scheduler = utils::leak(Scheduler::<Event>::new(QueueKind::weights()));
        let event_queue = EventQueueHandle::new(&scheduler);
        let effect_builder = EffectBuilder::new(event_queue);
        BlockProposer::new(registry, effect_builder, BlockProposerState::default())
            .expect("Failure to create a new Block Proposer")
    }

    impl From<StorageRequest> for Event {
        fn from(_: StorageRequest) -> Self {
            // we never send a storage request in our unit tests, but if this does become
            // meaningful....
            todo!()
        }
    }

    #[test]
    fn add_and_take_deploys() {
        let creation_time = Timestamp::from(100);
        let ttl = TimeDiff::from(100);
        let block_time1 = Timestamp::from(80);
        let block_time2 = Timestamp::from(120);
        let block_time3 = Timestamp::from(220);

        let no_blocks = HashSet::new();
        let (mut buffer, _effects) = create_test_buffer();
        let mut rng = crate::new_rng();
        let (hash1, deploy1) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash2, deploy2) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash3, deploy3) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash4, deploy4) = generate_deploy(&mut rng, creation_time, ttl, vec![]);

        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
            .is_empty());

        // add two deploys
        buffer.add_deploy(block_time2, hash1, deploy1);
        buffer.add_deploy(block_time2, hash2, deploy2.clone());

        // if we try to create a block with a timestamp that is too early, we shouldn't get any
        // deploys
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time1, no_blocks.clone())
            .is_empty());

        // if we try to create a block with a timestamp that is too late, we shouldn't get any
        // deploys, either
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time3, no_blocks.clone())
            .is_empty());

        // take the deploys out
        let deploys =
            buffer.remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone());

        assert_eq!(deploys.len(), 2);
        assert!(deploys.contains(&hash1));
        assert!(deploys.contains(&hash2));

        // the deploys should not have been removed yet
        assert!(!buffer
            .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
            .is_empty());

        // the two deploys will be included in block 1
        let block_hash1 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        buffer.added_block(block_hash1, deploys);

        // the deploys should have been removed now
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
            .is_empty());

        let mut blocks = HashSet::new();
        blocks.insert(block_hash1);

        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, blocks.clone())
            .is_empty());

        // try adding the same deploy again
        buffer.add_deploy(block_time2, hash2, deploy2.clone());

        // it shouldn't be returned if we include block 1 in the past blocks
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, blocks)
            .is_empty());
        // ...but it should be returned if we don't include it
        assert!(
            buffer
                .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
                .len()
                == 1
        );

        // the previous check removed the deploy from the buffer, let's re-add it
        buffer.add_deploy(block_time2, hash2, deploy2);

        // finalize the block
        buffer.finalized_block(block_hash1);

        // add more deploys
        buffer.add_deploy(block_time2, hash3, deploy3);
        buffer.add_deploy(block_time2, hash4, deploy4);

        let deploys = buffer.remaining_deploys(DeployConfig::default(), block_time2, no_blocks);

        // since block 1 is now finalized, deploy2 shouldn't be among the ones returned
        assert_eq!(deploys.len(), 2);
        assert!(deploys.contains(&hash3));
        assert!(deploys.contains(&hash4));
    }

    #[test]
    fn test_prune() {
        let expired_time = Timestamp::from(201);
        let creation_time = Timestamp::from(100);
        let test_time = Timestamp::from(120);
        let ttl = TimeDiff::from(100);

        let mut rng = crate::new_rng();
        let (hash1, deploy1) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash2, deploy2) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash3, deploy3) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash4, deploy4) = generate_deploy(
            &mut rng,
            creation_time + Duration::from_secs(20).into(),
            ttl,
            vec![],
        );
        let (mut buffer, _effects) = create_test_buffer();

        // pending
        buffer.add_deploy(creation_time, hash1, deploy1);
        buffer.add_deploy(creation_time, hash2, deploy2);
        buffer.add_deploy(creation_time, hash3, deploy3);
        buffer.add_deploy(creation_time, hash4, deploy4);

        // pending => proposed
        let block_hash1 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        let block_hash2 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        buffer.added_block(block_hash1, vec![hash1]);
        buffer.added_block(block_hash2, vec![hash2]);

        // proposed => finalized
        buffer.finalized_block(block_hash1);

        assert_eq!(buffer.state.pending.len(), 2);
        assert_eq!(buffer.state.proposed.get(&block_hash2).unwrap().len(), 1);
        assert_eq!(buffer.state.finalized.get(&block_hash1).unwrap().len(), 1);

        // test for retained values
        let pruned = buffer.prune(test_time);
        assert_eq!(pruned, 0);

        assert_eq!(buffer.state.pending.len(), 2);
        assert_eq!(buffer.state.proposed.len(), 1);
        assert_eq!(buffer.state.proposed.get(&block_hash2).unwrap().len(), 1);
        assert_eq!(buffer.state.finalized.len(), 1);
        assert_eq!(buffer.state.finalized.get(&block_hash1).unwrap().len(), 1);

        // now move the clock to make some things expire
        let pruned = buffer.prune(expired_time);
        assert_eq!(pruned, 3);

        assert_eq!(buffer.state.pending.len(), 1); // deploy4 is still valid
        assert_eq!(buffer.state.proposed.len(), 0);
        assert_eq!(buffer.state.finalized.len(), 0);
    }

    #[test]
    fn test_deploy_dependencies() {
        let creation_time = Timestamp::from(100);
        let ttl = TimeDiff::from(100);
        let block_time = Timestamp::from(120);

        let mut rng = crate::new_rng();
        let (hash1, deploy1) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        // let deploy2 depend on deploy1
        let (hash2, deploy2) = generate_deploy(&mut rng, creation_time, ttl, vec![hash1]);

        let mut blocks = HashSet::new();
        let (mut buffer, _effects) = create_test_buffer();

        // add deploy2
        buffer.add_deploy(creation_time, hash2, deploy2);

        // deploy2 has an unsatisfied dependency
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time, blocks.clone())
            .is_empty());

        // add deploy1
        buffer.add_deploy(creation_time, hash1, deploy1);

        let deploys = buffer.remaining_deploys(DeployConfig::default(), block_time, blocks.clone());
        // only deploy1 should be returned, as it has no dependencies
        assert_eq!(deploys.len(), 1);
        assert!(deploys.contains(&hash1));

        // the deploy will be included in block 1
        let block_hash1 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        buffer.added_block(block_hash1, deploys);
        blocks.insert(block_hash1);

        let deploys2 = buffer.remaining_deploys(DeployConfig::default(), block_time, blocks);
        // `blocks` contains a block that contains deploy1 now, so we should get deploy2
        assert_eq!(deploys2.len(), 1);
        assert!(deploys2.contains(&hash2));
    }
}
