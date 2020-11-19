use std::{collections::HashMap, hash::Hash, net::SocketAddr};

use semver::Version;
use serde::{Deserialize, Serialize};

use crate::{
    components::{chainspec_loader::ChainspecInfo, consensus::EraId},
    rpcs::info::JsonPeers,
    types::{Block, BlockHash, NodeId, Timestamp},
};

/// Data feed for client "info_get_status" endpoint.
#[derive(Debug, Serialize)]
#[serde(bound = "I: Eq + Hash + Serialize")]
pub struct StatusFeed<I> {
    /// The last block added to the chain.
    pub last_added_block: Option<Block>,
    /// The peer nodes which are connected to this node.
    pub peers: HashMap<I, SocketAddr>,
    /// The chainspec info for this node.
    pub chainspec_info: ChainspecInfo,
    /// The compiled node version.
    pub version: &'static str,
}

impl<I> StatusFeed<I> {
    pub(crate) fn new(
        last_added_block: Option<Block>,
        peers: HashMap<I, SocketAddr>,
        chainspec_info: ChainspecInfo,
    ) -> Self {
        StatusFeed {
            last_added_block,
            peers,
            chainspec_info,
            version: crate::VERSION_STRING.as_str(),
        }
    }
}

/// Minimal info of a `Block`.
#[derive(Serialize, Deserialize, Debug)]
pub struct MinimalBlockInfo {
    hash: BlockHash,
    timestamp: Timestamp,
    era_id: EraId,
    height: u64,
}

impl From<Block> for MinimalBlockInfo {
    fn from(block: Block) -> Self {
        MinimalBlockInfo {
            hash: *block.hash(),
            timestamp: block.header().timestamp(),
            era_id: block.header().era_id(),
            height: block.header().height(),
        }
    }
}

/// Result for "info_get_status" RPC response.
#[derive(Serialize, Deserialize, Debug)]
pub struct GetStatusResult {
    /// The RPC API version.
    pub api_version: Option<Version>,
    /// The chainspec name.
    pub chainspec_name: String,
    /// The genesis root hash.
    pub genesis_root_hash: String,
    /// The node ID and network address of each connected peer.
    pub peers: Vec<JsonPeers>,
    /// The minimal info of the last block from the linear chain.
    pub last_added_block_info: Option<MinimalBlockInfo>,
    /// The compiled node version.
    pub build_version: String,
}

impl GetStatusResult {
    /// Set api version.
    pub fn set_api_version(&mut self, version: Version) {
        self.api_version = Some(version);
    }
}

impl From<StatusFeed<NodeId>> for GetStatusResult {
    fn from(status_feed: StatusFeed<NodeId>) -> Self {
        let chainspec_name = status_feed.chainspec_info.name();
        let genesis_root_hash = status_feed
            .chainspec_info
            .root_hash()
            .unwrap_or_default()
            .to_string();
        let api_version = None;
        let peers = status_feed
            .peers
            .iter()
            .map(|(node, addr)| JsonPeers::new(node.clone(), *addr))
            .collect();
        let last_added_block_info = status_feed.last_added_block.map(Into::into);
        let build_version = crate::VERSION_STRING.clone();
        GetStatusResult {
            api_version,
            chainspec_name,
            genesis_root_hash,
            peers,
            last_added_block_info,
            build_version,
        }
    }
}
