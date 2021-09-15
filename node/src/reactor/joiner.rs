//! Reactor used to join the network.

mod memory_metrics;

use std::{
    env,
    fmt::{self, Display, Formatter},
    path::PathBuf,
};

use datasize::DataSize;
use derive_more::From;
use memory_metrics::MemoryMetrics;
use prometheus::Registry;
use reactor::ReactorEvent;
use serde::Serialize;
use tracing::{debug, error, info, warn};

use casper_execution_engine::{shared::stored_value::StoredValue, storage::trie::Trie};
use casper_types::Key;

#[cfg(test)]
use crate::testing::network::NetworkedReactor;
use crate::{
    components::{
        chainspec_loader::{self, ChainspecLoader},
        contract_runtime::ContractRuntime,
        deploy_acceptor::{self, DeployAcceptor},
        event_stream_server,
        event_stream_server::{DeployGetter, EventStreamServer},
        fetcher::{self, Fetcher},
        gossiper::{self, Gossiper},
        linear_chain_sync::{self, LinearChainSyncState},
        metrics::Metrics,
        network::{self, Network, NetworkIdentity, ENABLE_LIBP2P_NET_ENV_VAR},
        rest_server::{self, RestServer},
        small_network::{self, GossipedAddress, SmallNetwork, SmallNetworkIdentity},
        storage::Storage,
        Component,
    },
    effect::{
        announcements::{
            BlocklistAnnouncement, ChainspecLoaderAnnouncement, ContractRuntimeAnnouncement,
            ControlAnnouncement, DeployAcceptorAnnouncement, GossiperAnnouncement,
            LinearChainAnnouncement, NetworkAnnouncement,
        },
        requests::{
            ChainspecLoaderRequest, ConsensusRequest, ContractRuntimeRequest, FetcherRequest,
            MetricsRequest, NetworkInfoRequest, NetworkRequest, RestRequest, StorageRequest,
        },
        EffectBuilder, EffectExt, EffectOptionExt, Effects,
    },
    fatal,
    protocol::Message,
    reactor::{
        self,
        event_queue_metrics::EventQueueMetrics,
        initializer,
        participating::{self, Error, ParticipatingInitConfig},
        EventQueueHandle, Finalize, ReactorExit,
    },
    types::{
        Block, BlockHeader, BlockHeaderWithMetadata, BlockWithMetadata, Deploy, NodeId, Tag,
        Timestamp,
    },
    utils::{Source, WithDir},
    NodeRng,
};

/// Top-level event for the reactor.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, From, Serialize)]
#[must_use]
pub(crate) enum JoinerEvent {
    /// Finished joining event.
    FinishedJoining { block_header: Box<BlockHeader> },

    /// Network event.
    #[from]
    Network(network::Event<Message>),

    /// Small Network event.
    #[from]
    SmallNetwork(small_network::Event<Message>),

    /// Storage event.
    #[from]
    Storage(#[serde(skip_serializing)] StorageRequest),

    #[from]
    /// REST server event.
    RestServer(#[serde(skip_serializing)] rest_server::Event),

    #[from]
    /// Event stream server event.
    EventStreamServer(#[serde(skip_serializing)] event_stream_server::Event),

    /// Metrics request.
    #[from]
    MetricsRequest(#[serde(skip_serializing)] MetricsRequest),

    #[from]
    /// Chainspec Loader event.
    ChainspecLoader(#[serde(skip_serializing)] chainspec_loader::Event),

    /// Chainspec info request
    #[from]
    ChainspecLoaderRequest(#[serde(skip_serializing)] ChainspecLoaderRequest),

    /// Network info request.
    #[from]
    NetworkInfoRequest(#[serde(skip_serializing)] NetworkInfoRequest<NodeId>),

    /// Block fetcher event.
    #[from]
    BlockFetcher(#[serde(skip_serializing)] fetcher::Event<Block>),

    /// Trie fetcher event.
    #[from]
    TrieFetcher(#[serde(skip_serializing)] fetcher::Event<Trie<Key, StoredValue>>),

    /// Block header (without metadata) fetcher event.
    #[from]
    BlockHeaderFetcher(#[serde(skip_serializing)] fetcher::Event<BlockHeader>),

    /// Block header with metadata by height fetcher event.
    #[from]
    BlockHeaderByHeightFetcher(#[serde(skip_serializing)] fetcher::Event<BlockHeaderWithMetadata>),

    /// Linear chain (by height) fetcher event.
    #[from]
    BlockByHeightFetcher(#[serde(skip_serializing)] fetcher::Event<BlockWithMetadata>),

    /// Deploy fetcher event.
    #[from]
    DeployFetcher(#[serde(skip_serializing)] fetcher::Event<Deploy>),

    /// Deploy acceptor event.
    #[from]
    DeployAcceptor(#[serde(skip_serializing)] deploy_acceptor::Event),

    /// Contract Runtime event.
    #[from]
    ContractRuntime(#[serde(skip_serializing)] ContractRuntimeRequest),

    /// Address gossiper event.
    #[from]
    AddressGossiper(gossiper::Event<GossipedAddress>),

    /// Requests.
    /// Linear chain block by hash fetcher request.
    #[from]
    BlockFetcherRequest(#[serde(skip_serializing)] FetcherRequest<NodeId, Block>),

    /// Trie fetcher request.
    #[from]
    TrieFetcherRequest(#[serde(skip_serializing)] FetcherRequest<NodeId, Trie<Key, StoredValue>>),

    /// Blocker header (with no metadata) fetcher request.
    #[from]
    BlockHeaderFetcherRequest(#[serde(skip_serializing)] FetcherRequest<NodeId, BlockHeader>),

    /// Block header with metadata by height fetcher request.
    #[from]
    BlockHeaderByHeightFetcherRequest(
        #[serde(skip_serializing)] FetcherRequest<NodeId, BlockHeaderWithMetadata>,
    ),

    /// Linear chain block by height fetcher request.
    #[from]
    BlockByHeightFetcherRequest(
        #[serde(skip_serializing)] FetcherRequest<NodeId, BlockWithMetadata>,
    ),

    /// Deploy fetcher request.
    #[from]
    DeployFetcherRequest(#[serde(skip_serializing)] FetcherRequest<NodeId, Deploy>),

    // Announcements
    /// A control announcement.
    #[from]
    ControlAnnouncement(ControlAnnouncement),

    /// Network announcement.
    #[from]
    NetworkAnnouncement(#[serde(skip_serializing)] NetworkAnnouncement<NodeId, Message>),

    /// Blocklist announcement.
    #[from]
    BlocklistAnnouncement(#[serde(skip_serializing)] BlocklistAnnouncement<NodeId>),

    /// Block executor announcement.
    #[from]
    ContractRuntimeAnnouncement(#[serde(skip_serializing)] ContractRuntimeAnnouncement),

    /// Address Gossiper announcement.
    #[from]
    AddressGossiperAnnouncement(#[serde(skip_serializing)] GossiperAnnouncement<GossipedAddress>),

    /// DeployAcceptor announcement.
    #[from]
    DeployAcceptorAnnouncement(#[serde(skip_serializing)] DeployAcceptorAnnouncement<NodeId>),

    /// Linear chain announcement.
    #[from]
    LinearChainAnnouncement(#[serde(skip_serializing)] LinearChainAnnouncement),

    /// Chainspec loader announcement.
    #[from]
    ChainspecLoaderAnnouncement(#[serde(skip_serializing)] ChainspecLoaderAnnouncement),

    /// Consensus request.
    #[from]
    ConsensusRequest(#[serde(skip_serializing)] ConsensusRequest),
}

impl ReactorEvent for JoinerEvent {
    fn as_control(&self) -> Option<&ControlAnnouncement> {
        if let Self::ControlAnnouncement(ref ctrl_ann) = self {
            Some(ctrl_ann)
        } else {
            None
        }
    }
}

impl From<NetworkRequest<NodeId, Message>> for JoinerEvent {
    fn from(request: NetworkRequest<NodeId, Message>) -> Self {
        if env::var(ENABLE_LIBP2P_NET_ENV_VAR).is_ok() {
            JoinerEvent::Network(network::Event::from(request))
        } else {
            JoinerEvent::SmallNetwork(small_network::Event::from(request))
        }
    }
}

impl From<NetworkRequest<NodeId, gossiper::Message<GossipedAddress>>> for JoinerEvent {
    fn from(request: NetworkRequest<NodeId, gossiper::Message<GossipedAddress>>) -> Self {
        JoinerEvent::SmallNetwork(small_network::Event::from(
            request.map_payload(Message::from),
        ))
    }
}

impl From<RestRequest<NodeId>> for JoinerEvent {
    fn from(request: RestRequest<NodeId>) -> Self {
        JoinerEvent::RestServer(rest_server::Event::RestRequest(request))
    }
}

impl Display for JoinerEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            JoinerEvent::Network(event) => write!(f, "network: {}", event),
            JoinerEvent::SmallNetwork(event) => write!(f, "small network: {}", event),
            JoinerEvent::NetworkAnnouncement(event) => write!(f, "network announcement: {}", event),
            JoinerEvent::BlocklistAnnouncement(event) => {
                write!(f, "blocklist announcement: {}", event)
            }
            JoinerEvent::Storage(request) => write!(f, "storage: {}", request),
            JoinerEvent::RestServer(event) => write!(f, "rest server: {}", event),
            JoinerEvent::EventStreamServer(event) => write!(f, "event stream server: {}", event),
            JoinerEvent::MetricsRequest(req) => write!(f, "metrics request: {}", req),
            JoinerEvent::ChainspecLoader(event) => write!(f, "chainspec loader: {}", event),
            JoinerEvent::ChainspecLoaderRequest(req) => {
                write!(f, "chainspec loader request: {}", req)
            }
            JoinerEvent::NetworkInfoRequest(req) => write!(f, "network info request: {}", req),
            JoinerEvent::BlockFetcherRequest(request) => {
                write!(f, "block fetcher request: {}", request)
            }
            JoinerEvent::DeployFetcherRequest(request) => {
                write!(f, "deploy fetcher request: {}", request)
            }
            JoinerEvent::BlockFetcher(event) => write!(f, "block fetcher: {}", event),
            JoinerEvent::BlockByHeightFetcherRequest(request) => {
                write!(f, "block by height fetcher request: {}", request)
            }
            JoinerEvent::DeployFetcher(event) => write!(f, "deploy fetcher event: {}", event),
            JoinerEvent::ContractRuntime(event) => write!(f, "contract runtime event: {:?}", event),
            JoinerEvent::ContractRuntimeAnnouncement(announcement) => {
                write!(f, "block executor announcement: {}", announcement)
            }
            JoinerEvent::AddressGossiper(event) => write!(f, "address gossiper: {}", event),
            JoinerEvent::AddressGossiperAnnouncement(ann) => {
                write!(f, "address gossiper announcement: {}", ann)
            }
            JoinerEvent::BlockByHeightFetcher(event) => {
                write!(f, "block by height fetcher event: {}", event)
            }
            JoinerEvent::DeployAcceptorAnnouncement(ann) => {
                write!(f, "deploy acceptor announcement: {}", ann)
            }
            JoinerEvent::DeployAcceptor(event) => write!(f, "deploy acceptor: {}", event),
            JoinerEvent::ControlAnnouncement(ctrl_ann) => write!(f, "control: {}", ctrl_ann),
            JoinerEvent::LinearChainAnnouncement(ann) => {
                write!(f, "linear chain announcement: {}", ann)
            }
            JoinerEvent::ChainspecLoaderAnnouncement(ann) => {
                write!(f, "chainspec loader announcement: {}", ann)
            }
            JoinerEvent::ConsensusRequest(req) => write!(f, "consensus request: {:?}", req),
            JoinerEvent::TrieFetcher(trie) => {
                write!(f, "trie fetcher event: {}", trie)
            }
            JoinerEvent::TrieFetcherRequest(req) => {
                write!(f, "trie fetcher request: {}", req)
            }
            JoinerEvent::BlockHeaderFetcher(block_header) => {
                write!(f, "block header fetcher event: {}", block_header)
            }
            JoinerEvent::BlockHeaderFetcherRequest(req) => {
                write!(f, "block header fetcher request: {}", req)
            }
            JoinerEvent::BlockHeaderByHeightFetcher(block_header_by_height) => {
                write!(
                    f,
                    "block header by height fetcher event: {}",
                    block_header_by_height
                )
            }
            JoinerEvent::BlockHeaderByHeightFetcherRequest(req) => {
                write!(f, "block header by height fetcher request: {}", req)
            }
            JoinerEvent::FinishedJoining { block_header } => {
                write!(f, "finished joining with block header: {}", block_header)
            }
        }
    }
}

/// Joining node reactor.
#[derive(DataSize)]
pub(crate) struct Reactor {
    root: PathBuf,
    metrics: Metrics,
    network: Network<JoinerEvent, Message>,
    small_network: SmallNetwork<JoinerEvent, Message>,
    address_gossiper: Gossiper<GossipedAddress, JoinerEvent>,
    config: participating::Config,
    chainspec_loader: ChainspecLoader,
    storage: Storage,
    contract_runtime: ContractRuntime,
    linear_chain_sync: LinearChainSyncState,
    deploy_fetcher: Fetcher<Deploy>,
    block_by_hash_fetcher: Fetcher<Block>,
    block_by_height_fetcher: Fetcher<BlockWithMetadata>,
    block_header_by_hash_fetcher: Fetcher<BlockHeader>,
    block_header_and_finality_signatures_by_height_fetcher: Fetcher<BlockHeaderWithMetadata>,
    // Handles request for fetching tries from the network.
    #[data_size(skip)]
    trie_fetcher: Fetcher<Trie<Key, StoredValue>>,
    #[data_size(skip)]
    deploy_acceptor: DeployAcceptor,
    #[data_size(skip)]
    event_queue_metrics: EventQueueMetrics,
    #[data_size(skip)]
    rest_server: RestServer,
    #[data_size(skip)]
    event_stream_server: EventStreamServer,
    // Attach memory metrics for the joiner.
    #[data_size(skip)] // Never allocates data on the heap.
    memory_metrics: MemoryMetrics,
}

impl reactor::Reactor for Reactor {
    type Event = JoinerEvent;

    // The "configuration" is in fact the whole state of the initializer reactor, which we
    // deconstruct and reuse.
    type Config = WithDir<initializer::Reactor>;
    type Error = Error;

    fn new(
        initializer: Self::Config,
        registry: &Registry,
        event_queue: EventQueueHandle<Self::Event>,
        _rng: &mut NodeRng,
    ) -> Result<(Self, Effects<Self::Event>), Self::Error> {
        let (root, initializer) = initializer.into_parts();

        let initializer::Reactor {
            config: with_dir_config,
            chainspec_loader,
            storage,
            contract_runtime,
            small_network_identity,
            network_identity,
        } = initializer;

        // TODO: Remove wrapper around Reactor::Config instead.
        let (_, config) = with_dir_config.into_parts();

        let memory_metrics = MemoryMetrics::new(registry.clone())?;

        let event_queue_metrics = EventQueueMetrics::new(registry.clone(), event_queue)?;

        let metrics = Metrics::new(registry.clone());

        let network_config = network::Config::from(&config.network);
        let (network, network_effects) = Network::new(
            event_queue,
            network_config,
            registry,
            network_identity,
            chainspec_loader.chainspec(),
        )?;
        let (small_network, small_network_effects) = SmallNetwork::new(
            event_queue,
            config.network.clone(),
            Some(WithDir::new(&root, &config.consensus)),
            registry,
            small_network_identity,
            chainspec_loader.chainspec().as_ref(),
            None,
        )?;

        let mut effects = reactor::wrap_effects(JoinerEvent::Network, network_effects);
        effects.extend(reactor::wrap_effects(
            JoinerEvent::SmallNetwork,
            small_network_effects,
        ));

        let address_gossiper =
            Gossiper::new_for_complete_items("address_gossiper", config.gossip, registry)?;

        let effect_builder = EffectBuilder::new(event_queue);

        let maybe_trusted_hash = match config.node.trusted_hash {
            Some(trusted_hash) => Some(trusted_hash),
            None => storage
                .read_highest_block_header()
                .expect("Could not read highest block header")
                .map(|block_header| block_header.hash()),
        };

        let chainspec = chainspec_loader.chainspec().clone();
        let linear_chain_sync = match maybe_trusted_hash {
            None => {
                if let Some(start_time) = chainspec
                    .protocol_config
                    .activation_point
                    .genesis_timestamp()
                {
                    let era_duration = chainspec.core_config.era_duration;
                    if Timestamp::now() > start_time + era_duration {
                        error!(
                            "Node started with no trusted hash after the expected end of \
                             the genesis era! Please specify a trusted hash and restart. \
                             Time: {}, End of genesis era: {}",
                            Timestamp::now(),
                            start_time + era_duration
                        );
                        panic!("should have trusted hash after genesis era")
                    }
                }
                LinearChainSyncState::NotGoingToSync
            }
            Some(hash) => {
                let node_config = config.node.clone();
                effects.extend(
                    (async move {
                        info!(trusted_hash=%hash, "synchronizing linear chain");
                        match linear_chain_sync::run_fast_sync_task(
                            effect_builder,
                            hash,
                            chainspec,
                            node_config,
                        )
                        .await
                        {
                            Ok(block_header) => Some(JoinerEvent::FinishedJoining {
                                block_header: Box::new(block_header),
                            }),
                            Err(error) => {
                                fatal!(effect_builder, "{:?}", error).await;
                                None
                            }
                        }
                    })
                    .map_some(std::convert::identity),
                );
                LinearChainSyncState::Syncing
            }
        };

        let protocol_version = &chainspec_loader.chainspec().protocol_config.version;
        let rest_server = RestServer::new(
            config.rest_server.clone(),
            effect_builder,
            *protocol_version,
        )?;

        let event_stream_server = EventStreamServer::new(
            config.event_stream_server.clone(),
            storage.root_path().to_path_buf(),
            *protocol_version,
            DeployGetter::new(effect_builder),
        )?;

        let deploy_fetcher = Fetcher::new("deploy", config.fetcher, registry)?;
        let block_by_height_fetcher = Fetcher::new("block_by_height", config.fetcher, registry)?;
        let block_by_hash_fetcher = Fetcher::new("block", config.fetcher, registry)?;
        let trie_fetcher = Fetcher::new("trie", config.fetcher, registry)?;
        let block_header_and_finality_signatures_by_height_fetcher =
            Fetcher::new("block_header_by_height", config.fetcher, registry)?;
        let block_header_by_hash_fetcher: Fetcher<BlockHeader> =
            Fetcher::new("block_header", config.fetcher, registry)?;

        let deploy_acceptor =
            DeployAcceptor::new(config.deploy_acceptor, &*chainspec_loader.chainspec());

        effects.extend(reactor::wrap_effects(
            JoinerEvent::ChainspecLoader,
            chainspec_loader.start_checking_for_upgrades(effect_builder),
        ));

        Ok((
            Self {
                root,
                metrics,
                network,
                small_network,
                address_gossiper,
                config,
                chainspec_loader,
                storage,
                contract_runtime,
                linear_chain_sync,
                block_by_hash_fetcher,
                trie_fetcher,
                deploy_fetcher,
                block_by_height_fetcher,
                block_header_by_hash_fetcher,
                block_header_and_finality_signatures_by_height_fetcher,
                deploy_acceptor,
                event_queue_metrics,
                rest_server,
                event_stream_server,
                memory_metrics,
            },
            effects,
        ))
    }

    fn dispatch_event(
        &mut self,
        effect_builder: EffectBuilder<Self::Event>,
        rng: &mut NodeRng,
        event: Self::Event,
    ) -> Effects<Self::Event> {
        match event {
            JoinerEvent::Network(event) => reactor::wrap_effects(
                JoinerEvent::Network,
                self.network.handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::SmallNetwork(event) => reactor::wrap_effects(
                JoinerEvent::SmallNetwork,
                self.small_network.handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::ControlAnnouncement(ctrl_ann) => {
                unreachable!("unhandled control announcement: {}", ctrl_ann)
            }
            JoinerEvent::NetworkAnnouncement(NetworkAnnouncement::NewPeer(_id)) => {
                Effects::new()
            }
            JoinerEvent::NetworkAnnouncement(NetworkAnnouncement::GossipOurAddress(
                gossiped_address,
            )) => {
                let event = gossiper::Event::ItemReceived {
                    item_id: gossiped_address,
                    source: Source::<NodeId>::Ourself,
                };
                self.dispatch_event(effect_builder, rng, JoinerEvent::AddressGossiper(event))
            }
            JoinerEvent::BlocklistAnnouncement(ann) => {
                self.dispatch_event(effect_builder, rng, JoinerEvent::SmallNetwork(ann.into()))
            }
            JoinerEvent::NetworkAnnouncement(NetworkAnnouncement::MessageReceived {
                sender,
                payload,
            }) => match payload {
                Message::GetResponse {
                    tag: Tag::Block,
                    serialized_item,
                } => {
                    match fetcher::Event::<Block>::from_get_response_serialized_item(
                        sender,
                        &serialized_item,
                    ) {
                        Some(fetcher_event) => {
                            self.dispatch_event(effect_builder, rng, JoinerEvent::BlockFetcher(fetcher_event))
                        } ,
                        None => {
                            info!("{} sent us a block we couldn't parse! Banning", sender);
                            effect_builder.announce_disconnect_from_peer(sender).ignore()
                        }
                    }
                }
                Message::GetResponse {
                    tag: Tag::BlockAndMetadataByHeight,
                    serialized_item,
                } => {
                    match fetcher::Event::<BlockWithMetadata>::from_get_response_serialized_item(
                        sender,
                        &serialized_item,
                    ) {
                        Some(fetcher_event) => {
                            self.dispatch_event(effect_builder, rng, JoinerEvent::BlockByHeightFetcher(fetcher_event))
                        }
                        None => {
                            info!("{} sent us a block with metadata we couldn't parse! Banning", sender);
                            effect_builder.announce_disconnect_from_peer(sender).ignore()
                        }
                    }
                }
                Message::GetResponse {
                    tag: Tag::Trie,
                    serialized_item,
                } => {
                    match fetcher::Event::<Trie<Key, StoredValue>>::from_get_response_serialized_item(
                        sender,
                        &serialized_item,
                    ) {
                        Some(fetcher_event) => {
                            self.dispatch_event(effect_builder, rng, JoinerEvent::TrieFetcher(fetcher_event))
                        } ,
                        None => {
                            info!("{} sent us a trie we couldn't parse! Banning", sender);
                            effect_builder.announce_disconnect_from_peer(sender).ignore()
                        }
                    }
                }
                Message::GetResponse {
                    tag: Tag::BlockHeaderByHash,
                    serialized_item,
                } => {
                    match fetcher::Event::<BlockHeader>::from_get_response_serialized_item(
                        sender,
                        &serialized_item,
                    ) {
                        Some(fetcher_event) => {
                            self.dispatch_event(effect_builder, rng, JoinerEvent::BlockHeaderFetcher(fetcher_event))
                        } ,
                        None => {
                            info!("{} sent us a block header we couldn't parse! Banning", sender);
                            effect_builder.announce_disconnect_from_peer(sender).ignore()
                        }
                    }
                }
                Message::GetResponse {
                    tag: Tag::BlockHeaderAndFinalitySignaturesByHeight,
                    serialized_item,
                } => {
                    match fetcher::Event::<BlockHeaderWithMetadata>::from_get_response_serialized_item(
                        sender,
                        &serialized_item,
                    ) {
                        Some(fetcher_event) => {
                            self.dispatch_event(effect_builder, rng, JoinerEvent::BlockHeaderByHeightFetcher(fetcher_event))
                        } ,
                        None => {
                            info!("{} sent us a block header with finality signatures we couldn't parse! Banning", sender);
                            effect_builder.announce_disconnect_from_peer(sender).ignore()
                        }
                    }
                }
                Message::GetResponse {
                    tag: Tag::Deploy,
                    serialized_item,
                } => {
                    match fetcher::Event::<Deploy>::from_get_response_serialized_item(
                        sender,
                        &serialized_item,
                    ) {
                        Some(fetcher_event) => {
                            self.dispatch_event(effect_builder, rng, JoinerEvent::DeployFetcher(fetcher_event))
                        },
                        None => {
                            info!("{} sent us a deploy we couldn't parse! Banning", sender);
                            effect_builder.announce_disconnect_from_peer(sender).ignore()
                        }
                    }
                }
                Message::AddressGossiper(message) => {
                    let event = JoinerEvent::AddressGossiper(gossiper::Event::MessageReceived {
                        sender,
                        message,
                    });
                    self.dispatch_event(effect_builder, rng, event)
                }
                Message::FinalitySignature(_) => {
                    debug!("finality signatures not handled in joiner reactor");
                    Effects::new()
                }
                other => {
                    debug!(?other, "network announcement ignored.");
                    Effects::new()
                }
            },
            JoinerEvent::DeployAcceptorAnnouncement(
                DeployAcceptorAnnouncement::AcceptedNewDeploy { deploy, source },
            ) => {
                let event = event_stream_server::Event::DeployAccepted(*deploy.id());
                let mut effects =
                    self.dispatch_event(effect_builder, rng, JoinerEvent::EventStreamServer(event));

                let event = fetcher::Event::GotRemotely {
                    item: deploy,
                    source,
                };
                effects.extend(self.dispatch_event(
                    effect_builder,
                    rng,
                    JoinerEvent::DeployFetcher(event),
                ));

                effects
            }
            JoinerEvent::DeployAcceptorAnnouncement(
                DeployAcceptorAnnouncement::InvalidDeploy { deploy, source },
            ) => {
                let deploy_hash = *deploy.id();
                let peer = source;
                warn!(?deploy_hash, ?peer, "Invalid deploy received from a peer.");
                Effects::new()
            }
            JoinerEvent::Storage(event) => reactor::wrap_effects(
                JoinerEvent::Storage,
                self.storage.handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::BlockFetcherRequest(request) => {
                self.dispatch_event(effect_builder, rng, JoinerEvent::BlockFetcher(request.into()))
            }
            JoinerEvent::DeployAcceptor(event) => reactor::wrap_effects(
                JoinerEvent::DeployAcceptor,
                self.deploy_acceptor
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::BlockFetcher(event) => reactor::wrap_effects(
                JoinerEvent::BlockFetcher,
                self.block_by_hash_fetcher
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::DeployFetcher(event) => reactor::wrap_effects(
                JoinerEvent::DeployFetcher,
                self.deploy_fetcher.handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::BlockByHeightFetcher(event) => reactor::wrap_effects(
                JoinerEvent::BlockByHeightFetcher,
                self.block_by_height_fetcher
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::TrieFetcher(event) => reactor::wrap_effects(
                JoinerEvent::TrieFetcher,
                self.trie_fetcher.handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::BlockHeaderFetcher(event) => reactor::wrap_effects(
                JoinerEvent::BlockHeaderFetcher,
                self.block_header_by_hash_fetcher
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::DeployFetcherRequest(request) => {
                self.dispatch_event(effect_builder, rng, JoinerEvent::DeployFetcher(request.into()))
            }
            JoinerEvent::BlockByHeightFetcherRequest(request) => self.dispatch_event(
                effect_builder,
                rng,
                JoinerEvent::BlockByHeightFetcher(request.into()),
            ),
            JoinerEvent::TrieFetcherRequest(request) => {
                self.dispatch_event(effect_builder, rng, JoinerEvent::TrieFetcher(request.into()))
            }
            JoinerEvent::BlockHeaderFetcherRequest(request) => self.dispatch_event(
                effect_builder,
                rng,
                JoinerEvent::BlockHeaderFetcher(request.into()),
            ),
            JoinerEvent::ContractRuntime(event) => reactor::wrap_effects(
                JoinerEvent::ContractRuntime,
                self.contract_runtime
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::ContractRuntimeAnnouncement(_) => {
                Effects::new()
            }
            JoinerEvent::AddressGossiper(event) => reactor::wrap_effects(
                JoinerEvent::AddressGossiper,
                self.address_gossiper
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::AddressGossiperAnnouncement(GossiperAnnouncement::NewCompleteItem(
                gossiped_address,
            )) => {
                let reactor_event = JoinerEvent::SmallNetwork(
                    small_network::Event::PeerAddressReceived(gossiped_address),
                );
                self.dispatch_event(effect_builder, rng, reactor_event)
            }
            JoinerEvent::AddressGossiperAnnouncement(GossiperAnnouncement::FinishedGossiping(
                _,
            )) => {
                // We don't care about completion of gossiping an address.
                Effects::new()
            }

            JoinerEvent::LinearChainAnnouncement(LinearChainAnnouncement::BlockAdded(block)) => {
                let mut effects = reactor::wrap_effects(
                    JoinerEvent::EventStreamServer,
                    self.event_stream_server.handle_event(
                        effect_builder,
                        rng,
                        event_stream_server::Event::BlockAdded(block.clone()),
                    ),
                );
                effects.extend(self.dispatch_event(
                    effect_builder,
                    rng,
                    small_network::Event::from(LinearChainAnnouncement::BlockAdded(block)).into(),
                ));
                effects
            }
            JoinerEvent::LinearChainAnnouncement(
                LinearChainAnnouncement::NewFinalitySignature(fs),
            ) => {
                let reactor_event = JoinerEvent::EventStreamServer(
                    event_stream_server::Event::FinalitySignature(fs),
                );
                self.dispatch_event(effect_builder, rng, reactor_event)
            }
            JoinerEvent::RestServer(event) => reactor::wrap_effects(
                JoinerEvent::RestServer,
                self.rest_server.handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::EventStreamServer(event) => reactor::wrap_effects(
                JoinerEvent::EventStreamServer,
                self.event_stream_server
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::MetricsRequest(req) => reactor::wrap_effects(
                JoinerEvent::MetricsRequest,
                self.metrics.handle_event(effect_builder, rng, req),
            ),
            JoinerEvent::ChainspecLoader(event) => reactor::wrap_effects(
                JoinerEvent::ChainspecLoader,
                self.chainspec_loader
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::ChainspecLoaderRequest(req) => self.dispatch_event(
                effect_builder,
                rng,
                JoinerEvent::ChainspecLoader(req.into()),
            ),
            JoinerEvent::NetworkInfoRequest(req) => {
                let event = if env::var(ENABLE_LIBP2P_NET_ENV_VAR).is_ok() {
                    JoinerEvent::Network(network::Event::from(req))
                } else {
                    JoinerEvent::SmallNetwork(small_network::Event::from(req))
                };
                self.dispatch_event(effect_builder, rng, event)
            }
            JoinerEvent::ChainspecLoaderAnnouncement(
                ChainspecLoaderAnnouncement::UpgradeActivationPointRead(next_upgrade),
            ) => {
                let reactor_event = JoinerEvent::ChainspecLoader(
                    chainspec_loader::Event::GotNextUpgrade(next_upgrade),
                );
                self.dispatch_event(effect_builder, rng, reactor_event)
            }
            // This is done to handle status requests from the RestServer
            JoinerEvent::ConsensusRequest(ConsensusRequest::Status(responder)) => {
                // no consensus, respond with None
                responder.respond(None).ignore()
            }
            JoinerEvent::BlockHeaderByHeightFetcher(event) => reactor::wrap_effects(
                JoinerEvent::BlockHeaderByHeightFetcher,
                self.block_header_and_finality_signatures_by_height_fetcher
                    .handle_event(effect_builder, rng, event),
            ),
            JoinerEvent::BlockHeaderByHeightFetcherRequest(request) => self.dispatch_event(
                effect_builder,
                rng,
                JoinerEvent::BlockHeaderByHeightFetcher(request.into()),
            ),
            JoinerEvent::FinishedJoining { block_header } => {
                self.linear_chain_sync = LinearChainSyncState::Done(block_header);
                Effects::new()
            }
        }
    }

    fn maybe_exit(&self) -> Option<ReactorExit> {
        if self.linear_chain_sync.is_synced() {
            Some(ReactorExit::ProcessShouldContinue)
        } else {
            None
        }
    }

    fn update_metrics(&mut self, event_queue_handle: EventQueueHandle<Self::Event>) {
        self.memory_metrics.estimate(self);
        self.event_queue_metrics
            .record_event_queue_counts(&event_queue_handle);
    }
}

impl Reactor {
    /// Deconstructs the reactor into config useful for creating a Validator reactor. Shuts down
    /// the network, closing all incoming and outgoing connections, and frees up the listening
    /// socket.
    pub(crate) async fn into_participating_config(self) -> Result<ParticipatingInitConfig, Error> {
        let maybe_latest_block_header = self.linear_chain_sync.into_maybe_latest_block_header();
        let config = ParticipatingInitConfig {
            root: self.root,
            chainspec_loader: self.chainspec_loader,
            config: self.config,
            contract_runtime: self.contract_runtime,
            storage: self.storage,
            maybe_latest_block_header,
            event_stream_server: self.event_stream_server,
            small_network_identity: SmallNetworkIdentity::from(&self.small_network),
            network_identity: NetworkIdentity::from(&self.network),
        };
        self.network.finalize().await;
        self.small_network.finalize().await;
        self.rest_server.finalize().await;
        Ok(config)
    }
}

#[cfg(test)]
impl NetworkedReactor for Reactor {
    type NodeId = NodeId;
    fn node_id(&self) -> Self::NodeId {
        if env::var(ENABLE_LIBP2P_NET_ENV_VAR).is_err() {
            self.small_network.node_id()
        } else {
            self.network.node_id()
        }
    }
}

#[cfg(test)]
impl Reactor {
    /// Inspect storage.
    pub(crate) fn storage(&self) -> &Storage {
        &self.storage
    }
}
