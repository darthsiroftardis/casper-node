use std::collections::BTreeMap;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use casper_types::{
    auction::{Bid, Bids, DelegationRate, Delegator, EraId, EraValidators},
    AccessRights, PublicKey, URef, U512,
};

use crate::{crypto::hash::Digest, rpcs::docs::DocExample};

lazy_static! {
    static ref ERA_VALIDATORS: EraValidators = {
        let public_key_1 = PublicKey::Ed25519([42; 32]);

        let mut validator_weights = BTreeMap::new();
        validator_weights.insert(public_key_1, U512::from(10));

        let mut era_validators = BTreeMap::new();
        era_validators.insert(10u64, validator_weights);

        era_validators
    };
    static ref BIDS: Bids = {
        let bonding_purse = URef::new([0; 32], AccessRights::READ_ADD_WRITE);
        let staked_amount = U512::from(10);
        let release_era: u64 = 42;

        let delegator = Delegator::new(U512::from(10), bonding_purse, PublicKey::Ed25519([43; 32]));
        let mut delegators = BTreeMap::new();
        delegators.insert(PublicKey::Ed25519([44; 32]), delegator);

        let bid = Bid::locked(bonding_purse, staked_amount, release_era);

        let public_key_1 = PublicKey::Ed25519([42; 32]);

        let mut bids = BTreeMap::new();
        bids.insert(public_key_1, bid);

        bids
    };
    static ref AUCTION_INFO: AuctionState = {
        let state_root_hash = Digest::from([11u8; Digest::LENGTH]);
        let height: u64 = 10;
        let era_validators = Some(EraValidators::doc_example().clone());
        let bids = Some(Bids::doc_example().clone());
        AuctionState::new(state_root_hash, height, era_validators, bids)
    };
}

/// A struct to capture a ValidatorWeight
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JsonValidatorWeights {
    public_key: PublicKey,
    weight: U512,
}
/// A single EraValidator
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JsonEraValidators {
    era_id: EraId,
    validator_weights: Vec<JsonValidatorWeights>,
}
/// One delegator for a given validator to be serialized in a JSON conforming format
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JsonDelegator {
    public_key: PublicKey,
    delegator: Delegator,
}

/// Convert a Bid to a conforming JSON Representation of that bid.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JsonBid {
    /// The purse that was used for bonding.
    bonding_purse: URef,
    /// The amount of tokens staked by a validator (not including delegators).
    staked_amount: U512,
    /// Delegation rate
    delegation_rate: DelegationRate,
    /// A flag that represents a winning entry.
    ///
    /// `Some` indicates locked funds for a specific era and an autowin status, and `None` case
    /// means that funds are unlocked and autowin status is removed.
    release_era: Option<EraId>,
    /// A Vector consisting of JsonDelegators
    delegators: Vec<JsonDelegator>,
    /// This validator's seigniorage reward
    reward: U512,
}

impl From<Bid> for JsonBid {
    fn from(bid: Bid) -> Self {
        let mut json_delegators: Vec<JsonDelegator> = Vec::new();
        for (public_key, delegator) in bid.delegators().iter() {
            json_delegators.push(JsonDelegator {
                public_key: *public_key,
                delegator: *delegator,
            });
        }
        JsonBid {
            bonding_purse: *bid.bonding_purse(),
            staked_amount: *bid.staked_amount(),
            delegation_rate: *bid.delegation_rate(),
            release_era: bid.release_era(),
            delegators: json_delegators,
            reward: *bid.reward(),
        }
    }
}

/// A Json representation of a single bid.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JsonBids {
    public_key: PublicKey,
    bid: JsonBid,
}

/// Data structure summarizing auction contract data.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AuctionState {
    /// Global state hash
    pub state_root_hash: Digest,
    /// Block height
    pub block_height: u64,
    /// Era validators
    pub era_validators: Vec<JsonEraValidators>,
    /// All bids contained within a vector.
    bids: Vec<JsonBids>,
}

impl AuctionState {
    /// Create new instance of `AuctionState`
    pub fn new(
        state_root_hash: Digest,
        block_height: u64,
        era_validators: Option<EraValidators>,
        bids: Option<Bids>,
    ) -> Self {
        let mut json_era_validators: Vec<JsonEraValidators> = Vec::new();
        for (era_id, validator_weights) in era_validators.unwrap().iter() {
            let mut json_validator_weights: Vec<JsonValidatorWeights> = Vec::new();
            for (public_key, weight) in validator_weights.iter() {
                json_validator_weights.push(JsonValidatorWeights {
                    public_key: *public_key,
                    weight: *weight,
                });
            }
            json_era_validators.push(JsonEraValidators {
                era_id: *era_id,
                validator_weights: json_validator_weights,
            });
        }

        let mut json_bids: Vec<JsonBids> = Vec::new();
        for (public_key, bid) in bids.unwrap().iter() {
            let json_bid = JsonBid::from(bid.clone());
            json_bids.push(JsonBids {
                public_key: *public_key,
                bid: json_bid,
            });
        }

        AuctionState {
            state_root_hash,
            block_height,
            era_validators: json_era_validators,
            bids: json_bids,
        }
    }
}

impl DocExample for AuctionState {
    fn doc_example() -> &'static Self {
        &*AUCTION_INFO
    }
}

impl DocExample for EraValidators {
    fn doc_example() -> &'static Self {
        &*ERA_VALIDATORS
    }
}

impl DocExample for Bids {
    fn doc_example() -> &'static Self {
        &*BIDS
    }
}
