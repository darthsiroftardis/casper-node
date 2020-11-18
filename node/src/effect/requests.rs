//! Request effects.
//!
//! Requests typically ask other components to perform a service and report back the result. See the
//! top-level module documentation for details.

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug, Display, Formatter},
    net::SocketAddr,
    sync::Arc,
};

use datasize::DataSize;
use semver::Version;
use serde::Serialize;

use casper_execution_engine::{
    core::engine_state::{
        self,
        balance::{BalanceRequest, BalanceResult},
        era_validators::GetEraValidatorsError,
        execute_request::ExecuteRequest,
        execution_result::ExecutionResults,
        genesis::GenesisResult,
        query::{QueryRequest, QueryResult},
        step::{StepRequest, StepResult},
        upgrade::{UpgradeConfig, UpgradeResult},
    },
    shared::{additive_map::AdditiveMap, transform::Transform},
    storage::{global_state::CommitResult, protocol_data::ProtocolData},
};
use casper_types::{
    auction::{EraValidators, ValidatorWeights},
    Key, ProtocolVersion, URef,
};

use super::{Multiple, Responder};
use crate::{
    components::{
        chainspec_loader::ChainspecInfo,
        contract_runtime::{EraValidatorsRequest, ValidatorWeightsByEraIdRequest},
        fetcher::FetchResult,
    },
    crypto::{asymmetric_key::Signature, hash::Digest},
    rpcs::chain::BlockIdentifier,
    types::{
        json_compatibility::ExecutionResult, Block as LinearBlock, Block, BlockHash, BlockHeader,
        Deploy, DeployHash, DeployHeader, DeployMetadata, FinalizedBlock, Item, ProtoBlockHash,
        StatusFeed, Timestamp,
    },
    utils::DisplayIter,
    Chainspec,
};

/// A metrics request.
#[derive(Debug)]
pub enum MetricsRequest {
    /// Render current node metrics as prometheus-formatted string.
    RenderNodeMetricsText {
        /// Resopnder returning the rendered metrics or `None`, if an internal error occurred.
        responder: Responder<Option<String>>,
    },
}

impl Display for MetricsRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MetricsRequest::RenderNodeMetricsText { .. } => write!(formatter, "get metrics text"),
        }
    }
}

/// A networking request.
#[derive(Debug)]
#[must_use]
pub enum NetworkRequest<I, P> {
    /// Send a message on the network to a specific peer.
    SendMessage {
        /// Message destination.
        dest: I,
        /// Message payload.
        payload: P,
        /// Responder to be called when the message is queued.
        responder: Responder<()>,
    },
    /// Send a message on the network to all peers.
    /// Note: This request is deprecated and should be phased out, as not every network
    ///       implementation is likely to implement broadcast support.
    Broadcast {
        /// Message payload.
        payload: P,
        /// Responder to be called when all messages are queued.
        responder: Responder<()>,
    },
    /// Gossip a message to a random subset of peers.
    Gossip {
        /// Payload to gossip.
        payload: P,
        /// Number of peers to gossip to. This is an upper bound, otherwise best-effort.
        count: usize,
        /// Node IDs of nodes to exclude from gossiping to.
        exclude: HashSet<I>,
        /// Responder to be called when all messages are queued.
        responder: Responder<HashSet<I>>,
    },
}

impl<I, P> NetworkRequest<I, P> {
    /// Transform a network request by mapping the contained payload.
    ///
    /// This is a replacement for a `From` conversion that is not possible without specialization.
    pub(crate) fn map_payload<F, P2>(self, wrap_payload: F) -> NetworkRequest<I, P2>
    where
        F: FnOnce(P) -> P2,
    {
        match self {
            NetworkRequest::SendMessage {
                dest,
                payload,
                responder,
            } => NetworkRequest::SendMessage {
                dest,
                payload: wrap_payload(payload),
                responder,
            },
            NetworkRequest::Broadcast { payload, responder } => NetworkRequest::Broadcast {
                payload: wrap_payload(payload),
                responder,
            },
            NetworkRequest::Gossip {
                payload,
                count,
                exclude,
                responder,
            } => NetworkRequest::Gossip {
                payload: wrap_payload(payload),
                count,
                exclude,
                responder,
            },
        }
    }
}

impl<I, P> Display for NetworkRequest<I, P>
where
    I: Display,
    P: Display,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NetworkRequest::SendMessage { dest, payload, .. } => {
                write!(formatter, "send to {}: {}", dest, payload)
            }
            NetworkRequest::Broadcast { payload, .. } => {
                write!(formatter, "broadcast: {}", payload)
            }
            NetworkRequest::Gossip { payload, .. } => write!(formatter, "gossip: {}", payload),
        }
    }
}

/// A networking info request.
#[derive(Debug)]
#[must_use]
pub enum NetworkInfoRequest<I> {
    /// Get incoming and outgoing peers.
    GetPeers {
        /// Responder to be called with all connected peers.
        responder: Responder<HashMap<I, SocketAddr>>,
    },
}

impl<I> Display for NetworkInfoRequest<I>
where
    I: Display,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NetworkInfoRequest::GetPeers { responder: _ } => write!(formatter, "get peers"),
        }
    }
}

#[derive(Debug)]
/// A storage request.
#[must_use]
pub enum StorageRequest {
    /// Store given block.
    PutBlock {
        /// Block to be stored.
        block: Box<Block>,
        /// Responder to call with the result.  Returns true if the block was stored on this
        /// attempt or false if it was previously stored.
        responder: Responder<bool>,
    },
    /// Retrieve block with given hash.
    GetBlock {
        /// Hash of block to be retrieved.
        block_hash: BlockHash,
        /// Responder to call with the result.  Returns `None` is the block doesn't exist in local
        /// storage.
        responder: Responder<Option<Block>>,
    },
    /// Retrieve block with given height.
    GetBlockAtHeight {
        /// Height of the block.
        height: BlockHeight,
        /// Responder.
        responder: Responder<Option<Block>>,
    },
    /// Retrieve highest block.
    GetHighestBlock {
        /// Responder.
        responder: Responder<Option<Block>>,
    },
    /// Retrieve block header with given hash.
    GetBlockHeader {
        /// Hash of block to get header of.
        block_hash: BlockHash,
        /// Responder to call with the result.  Returns `None` is the block header doesn't exist in
        /// local storage.
        responder: Responder<Option<BlockHeader>>,
    },
    /// Store given deploy.
    PutDeploy {
        /// Deploy to store.
        deploy: Box<Deploy>,
        /// Responder to call with the result.  Returns true if the deploy was stored on this
        /// attempt or false if it was previously stored.
        responder: Responder<bool>,
    },
    /// Retrieve deploys with given hashes.
    GetDeploys {
        /// Hashes of deploys to be retrieved.
        deploy_hashes: Multiple<DeployHash>,
        /// Responder to call with the results.
        responder: Responder<Vec<Option<Deploy>>>,
    },
    /// Retrieve deploy headers with given hashes.
    GetDeployHeaders {
        /// Hashes of deploy headers to be retrieved.
        deploy_hashes: Multiple<DeployHash>,
        /// Responder to call with the results.
        responder: Responder<Vec<Option<DeployHeader>>>,
    },
    /// Store execution results for a set of deploys of a single block.
    ///
    /// Will return a fatal error if there are already execution results known for a specific
    /// deploy/block combination and a different result is inserted.
    ///
    /// Inserting the same block/deploy combination multiple times with the same execution results
    /// is not an error and will silently be ignored.
    PutExecutionResults {
        /// Hash of block.
        block_hash: BlockHash,
        /// Mapping of deploys to execution results of the block.
        execution_results: HashMap<DeployHash, ExecutionResult>,
        /// Responder to call when done storing.
        responder: Responder<()>,
    },
    /// Retrieve deploy and its metadata.
    GetDeployAndMetadata {
        /// Hash of deploy to be retrieved.
        deploy_hash: DeployHash,
        /// Responder to call with the results.
        responder: Responder<Option<(Deploy, DeployMetadata)>>,
    },
    /// Store given chainspec.
    PutChainspec {
        /// Chainspec.
        chainspec: Arc<Chainspec>,
        /// Responder to call with the result.
        responder: Responder<()>,
    },
    /// Retrieve chainspec with given version.
    GetChainspec {
        /// Version.
        version: Version,
        /// Responder to call with the result.
        responder: Responder<Option<Arc<Chainspec>>>,
    },
}

impl Display for StorageRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            StorageRequest::PutBlock { block, .. } => write!(formatter, "put {}", block),
            StorageRequest::GetBlock { block_hash, .. } => write!(formatter, "get {}", block_hash),
            StorageRequest::GetBlockAtHeight { height, .. } => {
                write!(formatter, "get block at height {}", height)
            }
            StorageRequest::GetHighestBlock { .. } => write!(formatter, "get highest block"),
            StorageRequest::GetBlockHeader { block_hash, .. } => {
                write!(formatter, "get {}", block_hash)
            }
            StorageRequest::PutDeploy { deploy, .. } => write!(formatter, "put {}", deploy),
            StorageRequest::GetDeploys { deploy_hashes, .. } => {
                write!(formatter, "get {}", DisplayIter::new(deploy_hashes.iter()))
            }
            StorageRequest::GetDeployHeaders { deploy_hashes, .. } => write!(
                formatter,
                "get headers {}",
                DisplayIter::new(deploy_hashes.iter())
            ),
            StorageRequest::PutExecutionResults { block_hash, .. } => {
                write!(formatter, "put execution results for {}", block_hash)
            }
            StorageRequest::GetDeployAndMetadata { deploy_hash, .. } => {
                write!(formatter, "get deploy and metadata for {}", deploy_hash)
            }
            StorageRequest::PutChainspec { chainspec, .. } => write!(
                formatter,
                "put chainspec {}",
                chainspec.genesis.protocol_version
            ),
            StorageRequest::GetChainspec { version, .. } => {
                write!(formatter, "get chainspec {}", version)
            }
        }
    }
}

/// A `BlockProposer` request.
#[derive(Debug)]
#[must_use]
pub enum BlockProposerRequest {
    /// Request a list of deploys to propose in a new block.
    ListForInclusion {
        /// The instant for which the deploy is requested.
        current_instant: Timestamp,
        /// Set of block hashes pointing to blocks whose deploys should be excluded.
        past_blocks: HashSet<ProtoBlockHash>,
        /// Responder to call with the result.
        responder: Responder<HashSet<DeployHash>>,
    },
}

impl Display for BlockProposerRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BlockProposerRequest::ListForInclusion {
                current_instant,
                past_blocks,
                responder: _,
            } => write!(
                formatter,
                "list for inclusion: instant {} past {}",
                current_instant,
                past_blocks.len()
            ),
        }
    }
}

/// Abstract RPC request.
///
/// An RPC request is an abstract request that does not concern itself with serialization or
/// transport.
#[derive(Debug)]
#[must_use]
pub enum RpcRequest<I> {
    /// Submit a deploy to be announced.
    SubmitDeploy {
        /// The deploy to be announced.
        deploy: Box<Deploy>,
        /// Responder to call.
        responder: Responder<()>,
    },
    /// If `maybe_hash` is `Some`, return the specified block if it exists, else `None`.  If
    /// `maybe_hash` is `None`, return the latest block.
    GetBlock {
        /// The hash of the block to be retrieved.
        maybe_id: Option<BlockIdentifier>,
        /// Responder to call with the result.
        responder: Responder<Option<LinearBlock>>,
    },
    /// Query the global state at the given root hash.
    QueryGlobalState {
        /// The state root hash.
        state_root_hash: Digest,
        /// Hex-encoded `casper_types::Key`.
        base_key: Key,
        /// The path components starting from the key as base.
        path: Vec<String>,
        /// Responder to call with the result.
        responder: Responder<Result<QueryResult, engine_state::Error>>,
    },
    /// Query the global state at the given root hash.
    QueryEraValidators {
        /// The global state hash.
        state_root_hash: Digest,
        /// The protocol version.
        protocol_version: ProtocolVersion,
        /// Responder to call with the result.
        responder: Responder<Result<EraValidators, GetEraValidatorsError>>,
    },
    /// Query the contract runtime for protocol version data.
    QueryProtocolData {
        /// The protocol version.
        protocol_version: ProtocolVersion,
        /// Responder to call with the result.
        responder: Responder<Result<Option<Box<ProtocolData>>, engine_state::Error>>,
    },
    /// Query the global state at the given root hash.
    GetBalance {
        /// The state root hash.
        state_root_hash: Digest,
        /// The purse URef.
        purse_uref: URef,
        /// Responder to call with the result.
        responder: Responder<Result<BalanceResult, engine_state::Error>>,
    },
    /// Return the specified deploy and metadata if it exists, else `None`.
    GetDeploy {
        /// The hash of the deploy to be retrieved.
        hash: DeployHash,
        /// Responder to call with the result.
        responder: Responder<Option<(Deploy, DeployMetadata)>>,
    },
    /// Return the connected peers.
    GetPeers {
        /// Responder to call with the result.
        responder: Responder<HashMap<I, SocketAddr>>,
    },
    /// Return string formatted status or `None` if an error occurred.
    GetStatus {
        /// Responder to call with the result.
        responder: Responder<StatusFeed<I>>,
    },
    /// Return string formatted, prometheus compatible metrics or `None` if an error occurred.
    GetMetrics {
        /// Responder to call with the result.
        responder: Responder<Option<String>>,
    },
}

impl<I> Display for RpcRequest<I> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RpcRequest::SubmitDeploy { deploy, .. } => write!(formatter, "submit {}", *deploy),
            RpcRequest::GetBlock {
                maybe_id: Some(BlockIdentifier::Hash(hash)),
                ..
            } => write!(formatter, "get {}", hash),
            RpcRequest::GetBlock {
                maybe_id: Some(BlockIdentifier::Height(height)),
                ..
            } => write!(formatter, "get {}", height),
            RpcRequest::GetBlock { maybe_id: None, .. } => write!(formatter, "get latest block"),
            RpcRequest::QueryProtocolData {
                protocol_version, ..
            } => write!(formatter, "protocol_version {}", protocol_version),
            RpcRequest::QueryGlobalState {
                state_root_hash,
                base_key,
                path,
                ..
            } => write!(
                formatter,
                "query {}, base_key: {}, path: {:?}",
                state_root_hash, base_key, path
            ),
            RpcRequest::QueryEraValidators {
                state_root_hash, ..
            } => write!(formatter, "auction {}", state_root_hash),
            RpcRequest::GetBalance {
                state_root_hash,
                purse_uref,
                ..
            } => write!(
                formatter,
                "balance {}, purse_uref: {}",
                state_root_hash, purse_uref
            ),
            RpcRequest::GetDeploy { hash, .. } => write!(formatter, "get {}", hash),
            RpcRequest::GetPeers { .. } => write!(formatter, "get peers"),
            RpcRequest::GetStatus { .. } => write!(formatter, "get status"),
            RpcRequest::GetMetrics { .. } => write!(formatter, "get metrics"),
        }
    }
}

/// Abstract REST request.
///
/// An REST request is an abstract request that does not concern itself with serialization or
/// transport.
#[derive(Debug)]
#[must_use]
pub enum RestRequest<I> {
    /// Return string formatted status or `None` if an error occurred.
    GetStatus {
        /// Responder to call with the result.
        responder: Responder<StatusFeed<I>>,
    },
    /// Return string formatted, prometheus compatible metrics or `None` if an error occurred.
    GetMetrics {
        /// Responder to call with the result.
        responder: Responder<Option<String>>,
    },
}

impl<I> Display for RestRequest<I> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RestRequest::GetStatus { .. } => write!(formatter, "get status"),
            RestRequest::GetMetrics { .. } => write!(formatter, "get metrics"),
        }
    }
}

/// A contract runtime request.
#[derive(Debug, Serialize)]
#[must_use]
pub enum ContractRuntimeRequest {
    /// Get `ProtocolData` by `ProtocolVersion`.
    GetProtocolData {
        /// The protocol version.
        protocol_version: ProtocolVersion,
        /// Responder to call with the result.
        responder: Responder<Result<Option<Box<ProtocolData>>, engine_state::Error>>,
    },
    /// Commit genesis chainspec.
    CommitGenesis {
        /// The chainspec.
        chainspec: Box<Chainspec>,
        /// Responder to call with the result.
        responder: Responder<Result<GenesisResult, engine_state::Error>>,
    },
    /// An `ExecuteRequest` that contains multiple deploys that will be executed.
    Execute {
        /// Execution request containing deploys.
        #[serde(skip_serializing)]
        execute_request: ExecuteRequest,
        /// Responder to call with the execution result.
        responder: Responder<Result<ExecutionResults, engine_state::RootNotFound>>,
    },
    /// A request to commit existing execution transforms.
    Commit {
        /// A valid state root hash.
        state_root_hash: Digest,
        /// Effects obtained through `ExecutionResult`
        #[serde(skip_serializing)]
        effects: AdditiveMap<Key, Transform>,
        /// Responder to call with the commit result.
        responder: Responder<Result<CommitResult, engine_state::Error>>,
    },
    /// A request to run upgrade.
    Upgrade {
        /// Upgrade config.
        #[serde(skip_serializing)]
        upgrade_config: Box<UpgradeConfig>,
        /// Responder to call with the upgrade result.
        responder: Responder<Result<UpgradeResult, engine_state::Error>>,
    },
    /// A query request.
    Query {
        /// Query request.
        #[serde(skip_serializing)]
        query_request: QueryRequest,
        /// Responder to call with the query result.
        responder: Responder<Result<QueryResult, engine_state::Error>>,
    },
    /// A balance request.
    GetBalance {
        /// Balance request.
        #[serde(skip_serializing)]
        balance_request: BalanceRequest,
        /// Responder to call with the balance result.
        responder: Responder<Result<BalanceResult, engine_state::Error>>,
    },
    /// Returns validator weights.
    GetEraValidators {
        /// Get validators weights request.
        #[serde(skip_serializing)]
        request: EraValidatorsRequest,
        /// Responder to call with the result.
        responder: Responder<Result<EraValidators, GetEraValidatorsError>>,
    },
    /// Returns validator weights for given era.
    GetValidatorWeightsByEraId {
        /// Get validators weights request.
        #[serde(skip_serializing)]
        request: ValidatorWeightsByEraIdRequest,
        /// Responder to call with the result.
        responder: Responder<Result<Option<ValidatorWeights>, GetEraValidatorsError>>,
    },
    /// Performs a step consisting of calculating rewards, slashing and running the auction at the
    /// end of an era.
    Step {
        /// The step request.
        #[serde(skip_serializing)]
        step_request: StepRequest,
        /// Responder to call with the result.
        responder: Responder<Result<StepResult, engine_state::Error>>,
    },
}

impl Display for ContractRuntimeRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ContractRuntimeRequest::CommitGenesis { chainspec, .. } => write!(
                formatter,
                "commit genesis {}",
                chainspec.genesis.protocol_version
            ),
            ContractRuntimeRequest::Execute {
                execute_request, ..
            } => write!(
                formatter,
                "execute request: {}",
                execute_request.parent_state_hash
            ),

            ContractRuntimeRequest::Commit {
                state_root_hash,
                effects,
                ..
            } => write!(
                formatter,
                "commit request: {} {:?}",
                state_root_hash, effects
            ),

            ContractRuntimeRequest::Upgrade { upgrade_config, .. } => {
                write!(formatter, "upgrade request: {:?}", upgrade_config)
            }

            ContractRuntimeRequest::Query { query_request, .. } => {
                write!(formatter, "query request: {:?}", query_request)
            }

            ContractRuntimeRequest::GetBalance {
                balance_request, ..
            } => write!(formatter, "balance request: {:?}", balance_request),

            ContractRuntimeRequest::GetEraValidators { request, .. } => {
                write!(formatter, "get era validators: {:?}", request)
            }

            ContractRuntimeRequest::GetValidatorWeightsByEraId { request, .. } => {
                write!(formatter, "get validator weights: {:?}", request)
            }

            ContractRuntimeRequest::Step { step_request, .. } => {
                write!(formatter, "step: {:?}", step_request)
            }

            ContractRuntimeRequest::GetProtocolData {
                protocol_version, ..
            } => write!(formatter, "protocol_version: {}", protocol_version),
        }
    }
}

/// Fetcher related requests.
#[derive(Debug)]
#[must_use]
pub enum FetcherRequest<I, T: Item> {
    /// Return the specified item if it exists, else `None`.
    Fetch {
        /// The ID of the item to be retrieved.
        id: T::Id,
        /// The peer id of the peer to be asked if the item is not held locally
        peer: I,
        /// Responder to call with the result.
        responder: Responder<Option<FetchResult<T>>>,
    },
}

impl<I, T: Item> Display for FetcherRequest<I, T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FetcherRequest::Fetch { id, .. } => write!(formatter, "request item by id {}", id),
        }
    }
}

/// A contract runtime request.
#[derive(Debug)]
#[must_use]
pub enum BlockExecutorRequest {
    /// A request to execute finalized block.
    ExecuteBlock(FinalizedBlock),
}

impl Display for BlockExecutorRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BlockExecutorRequest::ExecuteBlock(finalized_block) => {
                write!(f, "execute block {}", finalized_block)
            }
        }
    }
}

/// A block validator request.
#[derive(Debug)]
#[must_use]
pub struct BlockValidationRequest<T, I> {
    /// The block to be validated.
    pub(crate) block: T,
    /// The sender of the block, which will be asked to provide all missing deploys.
    pub(crate) sender: I,
    /// Responder to call with the result.
    ///
    /// Indicates whether or not validation was successful and returns `block` unchanged.
    pub(crate) responder: Responder<(bool, T)>,
    /// A check will be performed against the deploys to ensure their timestamp is
    /// older than or equal to the block itself.
    pub(crate) block_timestamp: Timestamp,
}

impl<T: Display, I: Display> Display for BlockValidationRequest<T, I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let BlockValidationRequest { block, sender, .. } = self;
        write!(f, "validate block {} from {}", block, sender)
    }
}

type BlockHeight = u64;

#[derive(Debug)]
/// Requests issued to the Linear Chain component.
pub enum LinearChainRequest<I> {
    /// Request whole block from the linear chain, by hash.
    BlockRequest(BlockHash, I),
    /// Request for a linear chain block at height.
    BlockAtHeight(BlockHeight, I),
    /// Local request for a linear chain block at height.
    /// TODO: Unify `BlockAtHeight` and `BlockAtHeightLocal`.
    BlockAtHeightLocal(BlockHeight, Responder<Option<Block>>),
}

impl<I: Display> Display for LinearChainRequest<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LinearChainRequest::BlockRequest(bh, peer) => {
                write!(f, "block request for hash {} from {}", bh, peer)
            }
            LinearChainRequest::BlockAtHeight(height, sender) => {
                write!(f, "block request for {} from {}", height, sender)
            }
            LinearChainRequest::BlockAtHeightLocal(height, _) => {
                write!(f, "local request for block at height {}", height)
            }
        }
    }
}

#[derive(DataSize, Debug)]
#[must_use]
/// Consensus component requests.
pub enum ConsensusRequest {
    /// Request for consensus to sign a new linear chain block and possibly start a new era.
    HandleLinearBlock(Box<BlockHeader>, Responder<Signature>),
}

/// ChainspecLoader componenent requests.
#[derive(Debug, Serialize)]
pub enum ChainspecLoaderRequest {
    /// Chainspec info request.
    GetChainspecInfo(Responder<ChainspecInfo>),
}

impl Display for ChainspecLoaderRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChainspecLoaderRequest::GetChainspecInfo(_) => write!(f, "get chainspec info"),
        }
    }
}
