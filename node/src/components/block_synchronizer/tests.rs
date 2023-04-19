pub(crate) mod test_utils;

use assert_matches::assert_matches;
use casper_execution_engine::storage::trie::merkle_proof::TrieMerkleProof;
use num_rational::Ratio;
use std::{
    collections::{BTreeMap, VecDeque},
    iter,
    time::Duration,
};

use casper_types::{AccessRights, CLValue, EraId, PublicKey, SecretKey, StoredValue, URef, U512};
use derive_more::From;
use rand::{seq::IteratorRandom, Rng};

use casper_types::testing::TestRng;

use super::*;
use crate::{
    components::{
        block_synchronizer::block_acquisition::BlockAcquisitionState,
        consensus::tests::utils::{ALICE_PUBLIC_KEY, ALICE_SECRET_KEY},
    },
    reactor::{EventQueueHandle, QueueKind, Scheduler},
    tls::KeyFingerprint,
    types::{DeployId, TestBlockBuilder},
    utils,
};

const MAX_SIMULTANEOUS_PEERS: usize = 5;

/// Event for the mock reactor.
#[derive(Debug, From)]
enum MockReactorEvent {
    BlockCompleteConfirmationRequest(BlockCompleteConfirmationRequest),
    BlockFetcherRequest(FetcherRequest<Block>),
    BlockHeaderFetcherRequest(FetcherRequest<BlockHeader>),
    LegacyDeployFetcherRequest(FetcherRequest<LegacyDeploy>),
    DeployFetcherRequest(FetcherRequest<Deploy>),
    FinalitySignatureFetcherRequest(FetcherRequest<FinalitySignature>),
    TrieOrChunkFetcherRequest(FetcherRequest<TrieOrChunk>),
    BlockExecutionResultsOrChunkFetcherRequest(FetcherRequest<BlockExecutionResultsOrChunk>),
    SyncLeapFetcherRequest(FetcherRequest<SyncLeap>),
    ApprovalsHashesFetcherRequest(FetcherRequest<ApprovalsHashes>),
    NetworkInfoRequest(NetworkInfoRequest),
    BlockAccumulatorRequest(BlockAccumulatorRequest),
    PeerBehaviorAnnouncement(PeerBehaviorAnnouncement),
    StorageRequest(StorageRequest),
    TrieAccumulatorRequest(TrieAccumulatorRequest),
    ContractRuntimeRequest(ContractRuntimeRequest),
    SyncGlobalStateRequest(SyncGlobalStateRequest),
    MakeBlockExecutableRequest(MakeBlockExecutableRequest),
    MetaBlockAnnouncement(MetaBlockAnnouncement),
}

struct MockReactor {
    scheduler: &'static Scheduler<MockReactorEvent>,
    effect_builder: EffectBuilder<MockReactorEvent>,
}

impl MockReactor {
    fn new() -> Self {
        let scheduler = utils::leak(Scheduler::new(QueueKind::weights()));
        let event_queue_handle = EventQueueHandle::without_shutdown(scheduler);
        let effect_builder = EffectBuilder::new(event_queue_handle);
        MockReactor {
            scheduler,
            effect_builder,
        }
    }

    fn effect_builder(&self) -> EffectBuilder<MockReactorEvent> {
        self.effect_builder
    }

    async fn crank(&self) -> MockReactorEvent {
        let ((_ancestor, reactor_event), _) = self.scheduler.pop().await;
        reactor_event
    }

    async fn process_effects(&self, mut effects: Effects<Event>) -> Vec<MockReactorEvent> {
        let mut events = Vec::new();
        for effect in effects.drain(0..) {
            tokio::spawn(async move { effect.await });
            let event = self.crank().await;
            events.push(event);
        }
        events
    }
}

struct TestEnv {
    block: Block,
    validator_keys: Vec<Arc<SecretKey>>,
    peers: Vec<NodeId>,
}

// Utility struct used to generate common test artifacts
impl TestEnv {
    // Replaces the test block with the one provided as parameter
    fn with_block(self, block: Block) -> Self {
        Self {
            block,
            validator_keys: self.validator_keys,
            peers: self.peers,
        }
    }

    fn block(&self) -> &Block {
        &self.block
    }

    fn validator_keys(&self) -> &Vec<Arc<SecretKey>> {
        &self.validator_keys
    }

    fn peers(&self) -> &Vec<NodeId> {
        &self.peers
    }

    // Generates a `ValidatorMatrix` that has the validators for the era of the test block
    // All validators have equal weights
    fn gen_validator_matrix(&self) -> ValidatorMatrix {
        let validator_weights: BTreeMap<PublicKey, U512> = self
            .validator_keys
            .iter()
            .map(|key| (PublicKey::from(key.as_ref()), 100.into())) // we give each validator equal weight
            .collect();

        assert_eq!(validator_weights.len(), self.validator_keys.len());

        // Set up a validator matrix for the era in which our test block was created
        let mut validator_matrix = ValidatorMatrix::new(
            Ratio::new(1, 3),
            None,
            EraId::from(0),
            self.validator_keys[0].clone(),
            PublicKey::from(self.validator_keys[0].as_ref()),
            1,
        );
        validator_matrix
            .register_validator_weights(self.block.header().era_id(), validator_weights);

        validator_matrix
    }

    fn random(rng: &mut TestRng) -> TestEnv {
        let num_validators: usize = rng.gen_range(3..100);
        let validator_keys: Vec<Arc<SecretKey>> = iter::repeat(())
            .take(num_validators)
            .map(|_| Arc::new(SecretKey::random(rng)))
            .collect();

        let num_peers = rng.gen_range(10..20);

        TestEnv {
            block: TestBlockBuilder::new().build(rng),
            validator_keys,
            peers: iter::repeat(())
                .take(num_peers)
                .map(|_| NodeId::from(rng.gen::<KeyFingerprint>()))
                .collect(),
        }
    }
}

fn check_sync_global_state_event(event: MockReactorEvent, block: &Block) {
    assert!(matches!(
        event,
        MockReactorEvent::SyncGlobalStateRequest { .. }
    ));
    let global_sync_request = match event {
        MockReactorEvent::SyncGlobalStateRequest(req) => req,
        _ => unreachable!(),
    };
    assert_eq!(global_sync_request.block_hash, *block.hash());
    assert_eq!(
        global_sync_request.state_root_hash,
        *block.state_root_hash()
    );
}

// Calls need_next for the block_synchronizer and processes the effects resulted returning a list of
// the new events that were generated
async fn need_next(
    rng: &mut TestRng,
    reactor: &MockReactor,
    block_synchronizer: &mut BlockSynchronizer,
    num_expected_events: usize,
) -> Vec<MockReactorEvent> {
    let effects = block_synchronizer.need_next(reactor.effect_builder(), rng);
    assert_eq!(effects.len(), num_expected_events);
    reactor.process_effects(effects).await
}

fn register_multiple_signatures<'a, I: IntoIterator<Item = &'a Arc<SecretKey>>>(
    builder: &mut BlockBuilder,
    block: &Block,
    validator_keys_iter: I,
) {
    for secret_key in validator_keys_iter {
        // Register a finality signature
        let signature = FinalitySignature::create(
            *block.hash(),
            block.header().era_id(),
            secret_key.as_ref(),
            PublicKey::from(secret_key.as_ref()),
        );
        assert!(signature.is_verified().is_ok());
        assert!(builder.register_finality_signature(signature, None).is_ok());
    }
}

impl BlockSynchronizer {
    // Create an initialized block synchronizer with default config and MAX_SIMULTANEOUS_PEERS peers
    fn new_initialized(rng: &mut TestRng, validator_matrix: ValidatorMatrix) -> BlockSynchronizer {
        let mut block_synchronizer = BlockSynchronizer::new(
            Config::default(),
            Arc::new(Chainspec::random(rng)),
            MAX_SIMULTANEOUS_PEERS as u32,
            validator_matrix,
            &prometheus::Registry::new(),
        )
        .expect("Failed to create BlockSynchronizer");

        <BlockSynchronizer as InitializedComponent<MainEvent>>::set_state(
            &mut block_synchronizer,
            ComponentState::Initialized,
        );

        block_synchronizer
    }

    fn forward_builder(&self) -> &BlockBuilder {
        self.forward.as_ref().expect("Forward builder missing")
    }
}

/// Returns the number of validators that need a signature for a weak finality of 1/3.
fn weak_finality_threshold(n: usize) -> usize {
    n / 3 + if n % 3 == 0 { 0 } else { 1 }
}

#[tokio::test]
async fn global_state_sync_wont_stall_with_bad_peers() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng).with_block(
        TestBlockBuilder::new()
            .era(1)
            .random_deploys(1, &mut rng)
            .build(&mut rng),
    );
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Set up the synchronizer for the test block such that the next step is getting global state
    block_synchronizer.register_block_by_hash(*block.hash(), true);
    assert!(
        block_synchronizer.historical.is_some(),
        "we only get global state on historical sync"
    );
    block_synchronizer.register_peers(*block.hash(), peers.clone());
    let historical_builder = block_synchronizer.historical.as_mut().unwrap();
    assert!(
        historical_builder
            .register_block_header(block.header().clone(), None)
            .is_ok(),
        "historical builder should register header"
    );
    historical_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        historical_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );
    assert!(
        historical_builder.register_block(block, None).is_ok(),
        "should register block"
    );
    // Register the remaining signatures to reach strict finality
    register_multiple_signatures(
        historical_builder,
        block,
        validators_secret_keys
            .iter()
            .skip(weak_finality_threshold(validators_secret_keys.len())),
    );

    // At this point, the next step the synchronizer takes should be to get global state
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(
        effects.len(),
        1,
        "need next should have 1 effect at this step, not {}",
        effects.len()
    );
    tokio::spawn(async move { effects.remove(0).await });
    let event = mock_reactor.crank().await;

    // Expect a `SyncGlobalStateRequest` for the `GlobalStateSynchronizer`
    // The peer list that the GlobalStateSynchronizer will use to fetch the tries
    let first_peer_set = peers.iter().copied().choose_multiple(&mut rng, 4);
    check_sync_global_state_event(event, block);

    // Wait for the latch to reset
    std::thread::sleep(Duration::from_secs(6));

    // Simulate an error form the global_state_synchronizer;
    // make it seem that the `TrieAccumulator` did not find the required tries on any of the peers
    block_synchronizer.global_state_synced(
        *block.hash(),
        Err(GlobalStateSynchronizerError::TrieAccumulator(
            first_peer_set.to_vec(),
        )),
    );

    // At this point we expect that another request for the global state would be made,
    // this time with other peers
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(
        effects.len(),
        1,
        "need next should still have 1 effect at this step, not {}",
        effects.len()
    );
    tokio::spawn(async move { effects.remove(0).await });
    let event = mock_reactor.crank().await;

    let second_peer_set = peers.iter().copied().choose_multiple(&mut rng, 4);
    check_sync_global_state_event(event, block);

    // Wait for the latch to reset
    std::thread::sleep(Duration::from_secs(6));

    // Simulate a successful global state sync;
    // Although the request was successful, some peers did not have the data.
    let unreliable_peers = second_peer_set.into_iter().choose_multiple(&mut rng, 2);
    block_synchronizer.global_state_synced(
        *block.hash(),
        Ok(GlobalStateSynchronizerResponse::new(
            (*block.state_root_hash()).into(),
            unreliable_peers.clone(),
        )),
    );
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(
        effects.len(),
        1,
        "need next should still have 1 effect after global state sync'd, not {}",
        effects.len()
    );
    tokio::spawn(async move { effects.remove(0).await });
    let event = mock_reactor.crank().await;

    assert!(
        false == matches!(event, MockReactorEvent::SyncGlobalStateRequest { .. }),
        "synchronizer should have progressed"
    );

    // Check if the peers returned by the `GlobalStateSynchronizer` in the response were marked
    // unreliable.
    for peer in unreliable_peers.iter() {
        assert!(
            block_synchronizer
                .historical
                .as_ref()
                .unwrap()
                .peer_list()
                .is_peer_unreliable(peer),
            "{} should be marked unreliable",
            peer
        );
    }
}

#[tokio::test]
async fn should_not_stall_after_registering_new_era_validator_weights() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();

    // Set up an empty validator matrix.
    let mut validator_matrix = ValidatorMatrix::new_with_validator(ALICE_SECRET_KEY.clone());
    let mut block_synchronizer =
        BlockSynchronizer::new_initialized(&mut rng, validator_matrix.clone());

    // Set up the synchronizer for the test block such that the next step is getting era validators.
    block_synchronizer.register_block_by_hash(*block.hash(), true);
    block_synchronizer.register_peers(*block.hash(), peers.clone());
    block_synchronizer
        .historical
        .as_mut()
        .expect("should have historical builder")
        .register_block_header(block.header().clone(), None)
        .expect("should register block header");

    // At this point, the next step the synchronizer takes should be to get era validators.
    let effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(
        effects.len(),
        MAX_SIMULTANEOUS_PEERS,
        "need next should have an effect per peer when needing peers"
    );
    for effect in effects {
        tokio::spawn(async move { effect.await });
        let event = mock_reactor.crank().await;
        match event {
            MockReactorEvent::SyncLeapFetcherRequest(_) => (),
            _ => panic!("unexpected event: {:?}", event),
        };
    }

    // Ensure the in-flight latch has been set, i.e. that `need_next` returns nothing.
    let effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert!(
        effects.is_empty(),
        "should not have need next while latched"
    );

    // Update the validator matrix to now have an entry for the era of our random block.
    validator_matrix.register_validator_weights(
        block.header().era_id(),
        iter::once((ALICE_PUBLIC_KEY.clone(), 100.into())).collect(),
    );

    block_synchronizer
        .historical
        .as_mut()
        .expect("should have historical builder")
        .register_era_validator_weights(&validator_matrix);

    // Ensure the in-flight latch has been released, i.e. that `need_next` returns something.
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(
        effects.len(),
        1,
        "need next should produce 1 effect now that we have peers and the latch is removed"
    );
    tokio::spawn(async move { effects.remove(0).await });
    let event = mock_reactor.crank().await;
    assert_matches!(
        event,
        MockReactorEvent::FinalitySignatureFetcherRequest(FetcherRequest {
            id,
            peer,
            ..
        }) if peers.contains(&peer) && id.block_hash == *block.hash()
    );
}

#[test]
fn duplicate_register_block_not_allowed_if_builder_is_not_failed() {
    let mut rng = TestRng::new();
    let test_env = TestEnv::random(&mut rng);
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for forward sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some()); // we only get global state on historical sync

    // Registering the block again should not be allowed until the sync finishes
    assert!(!block_synchronizer.register_block_by_hash(*block.hash(), false));

    // Trying to register a different block should replace the old one
    let new_block = Block::random(&mut rng);
    assert!(block_synchronizer.register_block_by_hash(*new_block.hash(), false));
    assert_eq!(
        block_synchronizer.forward.unwrap().block_hash(),
        *new_block.hash()
    );
}

#[tokio::test]
async fn historical_sync_gets_peers_form_both_connected_peers_and_accumulator() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for historical sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), true));
    assert!(block_synchronizer.historical.is_some());

    let effects = block_synchronizer.handle_event(
        mock_reactor.effect_builder(),
        &mut rng,
        Event::Request(BlockSynchronizerRequest::NeedNext),
    );
    assert_eq!(effects.len(), 2);
    let events = mock_reactor.process_effects(effects).await;

    // The first thing the synchronizer should do is get peers.
    // For the historical flow, the synchronizer will get a random sampling of the connected
    // peers and also ask the accumulator to provide peers from which it has received information
    // for the block that is being synchronized.
    assert_matches!(
        events[0],
        MockReactorEvent::NetworkInfoRequest(NetworkInfoRequest::FullyConnectedPeers {
            count,
            ..
        }) if count == MAX_SIMULTANEOUS_PEERS
    );

    assert_matches!(
        events[1],
        MockReactorEvent::BlockAccumulatorRequest(BlockAccumulatorRequest::GetPeersForBlock {
            block_hash,
            ..
        }) if block_hash == *block.hash()
    )
}

#[tokio::test]
async fn fwd_sync_gets_peers_only_from_accumulator() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for forward sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());

    let effects = block_synchronizer.handle_event(
        mock_reactor.effect_builder(),
        &mut rng,
        Event::Request(BlockSynchronizerRequest::NeedNext),
    );
    assert_eq!(effects.len(), 1);
    let events = mock_reactor.process_effects(effects).await;

    // The first thing the synchronizer should do is get peers.
    // For the forward flow, the synchronizer will ask the accumulator to provide peers
    // from which it has received information for the block that is being synchronized.
    assert_matches!(
        events[0],
        MockReactorEvent::BlockAccumulatorRequest(BlockAccumulatorRequest::GetPeersForBlock {
            block_hash,
            ..
        }) if block_hash == *block.hash()
    )
}

#[tokio::test]
async fn sync_starts_with_header_fetch() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let block = test_env.block();
    let peers = test_env.peers();
    let validator_matrix = test_env.gen_validator_matrix();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        MAX_SIMULTANEOUS_PEERS,
    )
    .await;

    // The first thing needed after the synchronizer has peers is
    // to fetch the block header from peers.
    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::BlockHeaderFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) if peers.contains(&peer) && id == *block.hash()
        );
    }
}

//TODO: remove this ignore when the synchronizer can recover from the stall
#[ignore]
#[tokio::test]
async fn fwd_sync_is_not_blocked_by_failed_header_fetch_within_latch_interval() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let block = test_env.block();
    let peers = test_env.peers();
    let validator_matrix = test_env.gen_validator_matrix();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        MAX_SIMULTANEOUS_PEERS,
    )
    .await;

    let mut peers_asked = Vec::new();
    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::BlockHeaderFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) if peers.contains(&peer) && id == *block.hash() => {
                peers_asked.push(peer);
            }
        );
    }

    // Simulate fetch errors for the header
    let mut generated_effects = Effects::new();
    for peer in peers_asked {
        let effects = block_synchronizer.handle_event(
            mock_reactor.effect_builder(),
            &mut rng,
            Event::BlockHeaderFetched(Err(FetcherError::Absent {
                id: Box::new(*block.hash()),
                peer,
            })),
        );

        // the effects array should be empty while the latch is active
        // once the latch is reset, we should get some effects
        generated_effects.extend(effects);
    }

    // If the effects are empty at this point, then the synchronizer gets stuck
    assert!(!generated_effects.is_empty());
}

#[tokio::test]
async fn registering_header_successfully_triggers_signatures_fetch_for_weak_finality() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        MAX_SIMULTANEOUS_PEERS,
    )
    .await;

    let mut peers_asked = Vec::new();
    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::BlockHeaderFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) if peers.contains(&peer) && id == *block.hash() => {
                peers_asked.push(peer);
            }
        );
    }

    // Simulate successful fetch of the block header
    let effects = block_synchronizer.handle_event(
        mock_reactor.effect_builder(),
        &mut rng,
        Event::BlockHeaderFetched(Ok(FetchedData::FromPeer {
            item: Box::new(block.clone().take_header()),
            peer: peers_asked[0],
        })),
    );

    // Check the block acquisition state
    let fwd_builder = block_synchronizer.forward_builder();
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlockHeader(header, _) if header.block_hash() == *block.hash()
    );

    // Check if the peer that provided the successful response was promoted
    assert!(fwd_builder.peer_list().is_peer_reliable(&peers_asked[0]));

    // Next the synchronizer should fetch finality signatures to reach weak finality.
    // The number of requests should be limited to the number of peers even if we
    // need to get more signatures to reach weak finality.
    assert_eq!(effects.len(), MAX_SIMULTANEOUS_PEERS);
    for event in mock_reactor.process_effects(effects).await {
        assert_matches!(
            event,
            MockReactorEvent::FinalitySignatureFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) if peers.contains(&peer) && id.block_hash == *block.hash() && id.era_id == block.header().era_id()
        );
    }
}

#[tokio::test]
async fn fwd_more_signatures_are_requested_if_weak_finality_is_not_reached() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlockHeader(header, _) if header.block_hash() == *block.hash()
    );

    // Simulate a successful fetch of a single signature
    let signature = FinalitySignature::create(
        *block.hash(),
        block.header().era_id(),
        validators_secret_keys[0].as_ref(),
        PublicKey::from(validators_secret_keys[0].as_ref()),
    );
    assert!(signature.is_verified().is_ok());
    let effects = block_synchronizer.handle_event(
        mock_reactor.effect_builder(),
        &mut rng,
        Event::FinalitySignatureFetched(Ok(FetchedData::FromPeer {
            item: Box::new(signature),
            peer: peers[0],
        })),
    );

    // A single signature isn't enough to reach weak finality.
    // The synchronizer should ask for the remaining signatures.
    // The peer limit should still be in place.
    assert_eq!(
        effects.len(),
        std::cmp::min(validators_secret_keys.len() - 1, MAX_SIMULTANEOUS_PEERS)
    );
    for event in mock_reactor.process_effects(effects).await {
        assert_matches!(
            event,
            MockReactorEvent::FinalitySignatureFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) => {
                assert!(peers.contains(&peer));
                assert_eq!(id.block_hash, *block.hash());
                assert_eq!(id.era_id, block.header().era_id());
                assert_ne!(id.public_key, PublicKey::from(validators_secret_keys[0].as_ref()));
            }
        );
    }

    // Register finality signatures to reach weak finality
    let mut generated_effects = Effects::new();
    for secret_key in validators_secret_keys
        .iter()
        .skip(1)
        .take(weak_finality_threshold(validators_secret_keys.len()))
    {
        // Register a finality signature
        let signature = FinalitySignature::create(
            *block.hash(),
            block.header().era_id(),
            secret_key.as_ref(),
            PublicKey::from(secret_key.as_ref()),
        );
        assert!(signature.is_verified().is_ok());
        let effects = block_synchronizer.handle_event(
            mock_reactor.effect_builder(),
            &mut rng,
            Event::FinalitySignatureFetched(Ok(FetchedData::FromPeer {
                item: Box::new(signature),
                peer: peers[2],
            })),
        );
        generated_effects.extend(effects);
    }

    // Now the block should have weak finality.
    // We are only interested in the last effects generated since as soon as the block has weak
    // finality it should start to fetch the block body.
    let events = mock_reactor
        .process_effects(
            generated_effects
                .into_iter()
                .rev()
                .take(MAX_SIMULTANEOUS_PEERS)
                .collect(),
        )
        .await;
    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::BlockFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) => {
                assert!(peers.contains(&peer));
                assert_eq!(id, *block.hash());
            }
        );
    }
}

//TODO: remove this ignore when the synchronizer can recover from the stall
#[ignore]
#[tokio::test]
async fn fwd_sync_is_not_blocked_by_failed_signatures_fetch_within_latch_interval() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let num_validators = test_env.validator_keys().len();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());
    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlockHeader(header, _) if header.block_hash() == *block.hash()
    );

    // Synchronizer should fetch finality signatures
    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        std::cmp::min(num_validators, MAX_SIMULTANEOUS_PEERS), /* We have num_validators
                                                                * validators so we
                                                                * require the num_validators
                                                                * signatures */
    )
    .await;

    // Check what signatures were requested
    let mut sigs_requested = Vec::new();
    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::FinalitySignatureFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) => {
                assert!(peers.contains(&peer));
                assert_eq!(id.block_hash, *block.hash());
                assert_eq!(id.era_id, block.header().era_id());
                sigs_requested.push((peer, id.public_key));
            }
        );
    }

    // Simulate failed fetch of finality signatures
    let mut generated_effects = Effects::new();
    for (peer, public_key) in sigs_requested {
        let effects = block_synchronizer.handle_event(
            mock_reactor.effect_builder(),
            &mut rng,
            Event::FinalitySignatureFetched(Err(FetcherError::Absent {
                id: Box::new(Box::new(FinalitySignatureId {
                    block_hash: *block.hash(),
                    era_id: block.header().era_id(),
                    public_key,
                })),
                peer,
            })),
        );
        // the effects array should be empty while the latch is active
        // once the latch is reset, we should get some effects
        generated_effects.extend(effects);
    }

    // If the effects are empty at this point, then the synchronizer gets stuck
    assert!(!generated_effects.is_empty());
}

#[tokio::test]
async fn next_action_for_have_weak_finality_is_fetching_block_body() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveWeakFinalitySignatures(header, _) if header.block_hash() == *block.hash()
    );

    // Now the block should have weak finality.
    // Next step is to get the block body.
    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        MAX_SIMULTANEOUS_PEERS,
    )
    .await;

    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::BlockFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) => {
                assert!(peers.contains(&peer));
                assert_eq!(id, *block.hash());
            }
        );
    }
}

#[tokio::test]
async fn registering_block_body_transitions_builder_to_have_block_state() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveWeakFinalitySignatures(header, _) if header.block_hash() == *block.hash()
    );

    // Now the block should have weak finality.
    // Next step is to get the block body.
    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        MAX_SIMULTANEOUS_PEERS,
    )
    .await;

    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::BlockFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) => {
                assert!(peers.contains(&peer));
                assert_eq!(id, *block.hash());
            }
        );
    }

    block_synchronizer.handle_event(
        mock_reactor.effect_builder(),
        &mut rng,
        Event::BlockFetched(Ok(FetchedData::FromPeer {
            item: Box::new(block.clone()),
            peer: peers[0],
        })),
    );

    assert_matches!(
        block_synchronizer.forward_builder().block_acquisition_state(),
        BlockAcquisitionState::HaveBlock(acquired_block, _, _) if acquired_block.hash() == block.hash()
    );
}

#[tokio::test]
async fn fwd_having_block_body_for_block_without_deploys_requires_only_signatures() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );

    assert!(fwd_builder.register_block(block, None).is_ok());

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlock(acquired_block, _, _) if acquired_block.hash() == block.hash()
    );

    // Since the block doesn't have any deploys,
    // the next step should be to fetch the finality signatures for strict finality.
    let effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    for event in mock_reactor.process_effects(effects).await {
        assert_matches!(
            event,
            MockReactorEvent::FinalitySignatureFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) if peers.contains(&peer) && id.block_hash == *block.hash() && id.era_id == block.header().era_id()
        );
    }
}

#[tokio::test]
async fn fwd_having_block_body_for_block_with_deploys_requires_approvals_hashes() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng).with_block(
        TestBlockBuilder::new()
            .era(1)
            .random_deploys(1, &mut rng)
            .build(&mut rng),
    );
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );

    assert!(fwd_builder.register_block(block, None).is_ok());

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlock(acquired_block, _, _) if acquired_block.hash() == block.hash()
    );

    // Since the block has deploys,
    // the next step should be to fetch the approvals hashes.
    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        MAX_SIMULTANEOUS_PEERS,
    )
    .await;

    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::ApprovalsHashesFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) if peers.contains(&peer) && id == *block.hash()
        );
    }
}

#[tokio::test]
async fn fwd_registering_approvals_hashes_triggers_fetch_for_deploys() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let deploys = [Deploy::random(&mut rng)];
    let test_env = TestEnv::random(&mut rng).with_block(
        TestBlockBuilder::new()
            .era(1)
            .deploys(deploys.iter())
            .build(&mut rng),
    );
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );

    assert!(fwd_builder.register_block(block, None).is_ok());

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlock(acquired_block, _, _) if acquired_block.hash() == block.hash()
    );

    let approvals_hashes = ApprovalsHashes::new(
        block.hash(),
        deploys
            .iter()
            .map(|deploy| deploy.approvals_hash().unwrap())
            .collect(),
        TrieMerkleProof::new(
            URef::new([255; 32], AccessRights::NONE).into(),
            StoredValue::CLValue(CLValue::from_t(()).unwrap()),
            VecDeque::new(),
        ),
    );

    // Since the block has approvals hashes,
    // the next step should be to fetch the deploys.
    let effects = block_synchronizer.handle_event(
        mock_reactor.effect_builder(),
        &mut rng,
        Event::ApprovalsHashesFetched(Ok(FetchedData::FromPeer {
            item: Box::new(approvals_hashes.clone()),
            peer: peers[0],
        })),
    );
    assert_eq!(effects.len(), MAX_SIMULTANEOUS_PEERS);
    for event in mock_reactor.process_effects(effects).await {
        assert_matches!(
            event,
            MockReactorEvent::DeployFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) => {
                assert!(peers.contains(&peer));
                assert_eq!(id, DeployId::new(
                    *deploys[0].hash(),
                    approvals_hashes.approvals_hashes()[0],
                ));
            }
        );
    }
}

#[tokio::test]
async fn fwd_have_block_body_without_deploys_and_strict_finality_transitions_state_machine() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach strict finality
    register_multiple_signatures(fwd_builder, block, validators_secret_keys.iter());

    assert!(fwd_builder.register_block(block, None).is_ok());

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlock(acquired_block, _, _) if acquired_block.hash() == block.hash()
    );

    // Since the block doesn't have any deploys and already has achieved strict finality, we expect
    // it to transition directly to HaveStrictFinality and ask for the next piece of work
    // immediately
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(effects.len(), 1);

    let fwd_builder = block_synchronizer
        .forward
        .as_ref()
        .expect("Forward builder should have been initialized");
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveStrictFinalitySignatures(acquired_block, ..) if acquired_block.hash() == block.hash()
    );

    // Expect a single NeedNext event
    let events = effects.remove(0).await;
    assert_eq!(events.len(), 1);
    assert_matches!(
        events[0],
        Event::Request(BlockSynchronizerRequest::NeedNext)
    );
}

#[tokio::test]
async fn fwd_have_block_with_strict_finality_requires_creation_of_finalized_block() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register signatures for weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );
    assert!(fwd_builder.register_block(block, None).is_ok());

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveBlock(acquired_block, _, _) if acquired_block.hash() == block.hash()
    );

    // Register the remaining signatures to reach strict finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .skip(weak_finality_threshold(validators_secret_keys.len())),
    );

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveStrictFinalitySignatures(acquired_block, ..) if acquired_block.hash() == block.hash()
    );

    // Block should have strict finality and will require to be executed
    let events = need_next(&mut rng, &mock_reactor, &mut block_synchronizer, 1).await;

    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::MakeBlockExecutableRequest(MakeBlockExecutableRequest {
                block_hash,
                ..
            }) if block_hash == *block.hash()
        );
    }
}

#[tokio::test]
async fn fwd_have_strict_finality_requests_enqueue_when_finalized_block_is_created() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );
    assert!(fwd_builder.register_block(block, None).is_ok());
    // Register the remaining signatures to reach strict finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .skip(weak_finality_threshold(validators_secret_keys.len())),
    );

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveStrictFinalitySignatures(acquired_block, ..) if acquired_block.hash() == block.hash()
    );

    assert_matches!(
        block_synchronizer.forward_progress(),
        BlockSynchronizerProgress::Syncing(block_hash, _, _) if block_hash == *block.hash()
    );

    // After the FinalizedBlock is created, the block synchronizer will request for it to be
    // enqueued for execution
    let event = Event::MadeFinalizedBlock {
        block_hash: *block.hash(),
        result: Some((block.clone().into(), Vec::new())),
    };
    let effects = block_synchronizer.handle_event(mock_reactor.effect_builder(), &mut rng, event);
    assert_eq!(effects.len(), 1);
    let events = mock_reactor.process_effects(effects).await;

    // Check the block acquisition state
    let fwd_builder = block_synchronizer
        .forward
        .as_ref()
        .expect("Forward builder should have been initialized");
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveFinalizedBlock(block_hash, _, _, _) if block_hash == block.hash()
    );

    assert_matches!(
        &events[0],
        MockReactorEvent::ContractRuntimeRequest(ContractRuntimeRequest::EnqueueBlockForExecution {
            finalized_block,
            ..
        }) if finalized_block.height() == block.height()
    );

    // Progress is syncing until we get a confirmation that the block was enqueued for execution
    assert_matches!(
        block_synchronizer.forward_progress(),
        BlockSynchronizerProgress::Syncing(block_hash, _, _) if block_hash == *block.hash()
    );
}

#[tokio::test]
async fn fwd_builder_status_is_executing_when_block_is_enqueued_for_execution() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );
    assert!(fwd_builder.register_block(block, None).is_ok());
    // Register the remaining signatures to reach strict finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .skip(weak_finality_threshold(validators_secret_keys.len())),
    );

    // Check the block acquisition state
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveStrictFinalitySignatures(acquired_block, ..) if acquired_block.hash() == block.hash()
    );

    // Register finalized block
    fwd_builder.register_made_finalized_block(block.clone().into(), Vec::new());
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::HaveFinalizedBlock(block_hash, _, _, _) if block_hash == block.hash()
    );

    // Simulate that enqueuing the block for execution was successful
    let event = Event::MarkBlockExecutionEnqueued(*block.hash());

    // There is nothing for the synchronizer to do at this point.
    // It will wait for the block to be executed
    let effects = block_synchronizer.handle_event(mock_reactor.effect_builder(), &mut rng, event);
    assert_eq!(effects.len(), 0);

    // Progress should now indicate that the block is executing
    assert_matches!(
        block_synchronizer.forward_progress(),
        BlockSynchronizerProgress::Executing(block_hash, _, _) if block_hash == *block.hash()
    );
}

#[tokio::test]
async fn fwd_sync_is_finished_when_block_is_marked_as_executed() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );
    assert!(fwd_builder.register_block(block, None).is_ok());
    // Register the remaining signatures to reach strict finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .skip(weak_finality_threshold(validators_secret_keys.len())),
    );

    // Register finalized block
    fwd_builder.register_made_finalized_block(block.clone().into(), Vec::new());
    fwd_builder.register_block_execution_enqueued();

    // Progress should now indicate that the block is executing
    assert_matches!(
        block_synchronizer.forward_progress(),
        BlockSynchronizerProgress::Executing(block_hash, _, _) if block_hash == *block.hash()
    );

    // Simulate a MarkBlockExecuted event
    let event = Event::MarkBlockExecuted(*block.hash());

    // There is nothing for the synchronizer to do at this point, the sync is finished.
    let effects = block_synchronizer.handle_event(mock_reactor.effect_builder(), &mut rng, event);
    assert_eq!(effects.len(), 0);

    // Progress should now indicate that the block is executing
    assert_matches!(
        block_synchronizer.forward_progress(),
        BlockSynchronizerProgress::Synced(block_hash, _, _) if block_hash == *block.hash()
    );
}

#[test]
fn builders_are_purged_when_requested() {
    let mut rng = TestRng::new();
    let test_env = TestEnv::random(&mut rng);
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for forward sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));

    // Registering block for historical sync
    assert!(block_synchronizer.register_block_by_hash(*Block::random(&mut rng).hash(), true));

    assert!(block_synchronizer.forward.is_some());
    assert!(block_synchronizer.historical.is_some());

    block_synchronizer.purge_historical();
    assert!(block_synchronizer.forward.is_some());
    assert!(block_synchronizer.historical.is_none());

    assert!(block_synchronizer.register_block_by_hash(*Block::random(&mut rng).hash(), true));
    assert!(block_synchronizer.forward.is_some());
    assert!(block_synchronizer.historical.is_some());

    block_synchronizer.purge_forward();
    assert!(block_synchronizer.forward.is_none());
    assert!(block_synchronizer.historical.is_some());

    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    assert!(block_synchronizer.historical.is_some());

    block_synchronizer.purge();
    assert!(block_synchronizer.forward.is_none());
    assert!(block_synchronizer.historical.is_none());
}

#[tokio::test]
async fn synchronizer_halts_if_block_cannot_be_made_executable() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();
    let test_env = TestEnv::random(&mut rng);
    let peers = test_env.peers();
    let block = test_env.block();
    let validator_matrix = test_env.gen_validator_matrix();
    let validators_secret_keys = test_env.validator_keys();
    let mut block_synchronizer = BlockSynchronizer::new_initialized(&mut rng, validator_matrix);

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false));
    assert!(block_synchronizer.forward.is_some());
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let fwd_builder = block_synchronizer
        .forward
        .as_mut()
        .expect("Forward builder should have been initialized");
    assert!(fwd_builder
        .register_block_header(block.clone().take_header(), None)
        .is_ok());
    fwd_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);
    // Register finality signatures to reach weak finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .take(weak_finality_threshold(validators_secret_keys.len())),
    );
    assert!(fwd_builder.register_block(block, None).is_ok());
    // Register the remaining signatures to reach strict finality
    register_multiple_signatures(
        fwd_builder,
        block,
        validators_secret_keys
            .iter()
            .skip(weak_finality_threshold(validators_secret_keys.len())),
    );

    // Block should have strict finality and will require to be executed
    let events = need_next(&mut rng, &mock_reactor, &mut block_synchronizer, 1).await;

    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::MakeBlockExecutableRequest(MakeBlockExecutableRequest {
                block_hash,
                ..
            }) if block_hash == *block.hash()
        );
    }

    // Simulate an error (the block couldn't be converted for execution).
    // This can happen if the synchronizer didn't fetch the right approvals hashes.
    // Don't expect to progress any further here. The control logic should
    // leap and backfill this block during a historical sync.
    let effects = block_synchronizer.handle_event(
        mock_reactor.effect_builder(),
        &mut rng,
        Event::MadeFinalizedBlock {
            block_hash: *block.hash(),
            result: None,
        },
    );
    assert_eq!(effects.len(), 0);

    // Check the block acquisition state
    let fwd_builder = block_synchronizer
        .forward
        .as_ref()
        .expect("Forward builder should have been initialized");
    assert_matches!(
        fwd_builder.block_acquisition_state(),
        BlockAcquisitionState::Failed(block_hash, _) if block_hash == block.hash()
    );

    // Progress should now indicate that the block is syncing
    assert_matches!(
        block_synchronizer.forward_progress(),
        BlockSynchronizerProgress::Syncing(block_hash, _, _) if block_hash == *block.hash()
    );
}