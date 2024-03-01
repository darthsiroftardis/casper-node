use std::{
    cmp::{Ord, PartialOrd},
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Display, Formatter},
    hash::Hash,
};

use datasize::DataSize;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use casper_types::Transaction;
use casper_types::{
    Approval, BlockV2, EraId, PublicKey, RewardedSignatures, SecretKey,
    SingleBlockRewardedSignatures, Timestamp, TransactionCategory, TransactionHash,
    TransactionV1Hash,
};
#[cfg(test)]
use {casper_types::testing::TestRng, rand::Rng};

use super::BlockPayload;
use crate::rpcs::docs::DocExample;

static FINALIZED_BLOCK: Lazy<FinalizedBlock> = Lazy::new(|| {
    let validator_set = {
        let mut validator_set = BTreeSet::new();
        validator_set.insert(PublicKey::from(
            &SecretKey::ed25519_from_bytes([5u8; SecretKey::ED25519_LENGTH]).unwrap(),
        ));
        validator_set.insert(PublicKey::from(
            &SecretKey::ed25519_from_bytes([7u8; SecretKey::ED25519_LENGTH]).unwrap(),
        ));
        validator_set
    };
    let rewarded_signatures =
        RewardedSignatures::new(vec![SingleBlockRewardedSignatures::from_validator_set(
            &validator_set,
            &validator_set,
        )]);
    let secret_key = SecretKey::example();
    let hash = TransactionV1Hash::from_raw([19; 32]);
    let transaction_hash = hash.into();
    let approval = Approval::create(&transaction_hash, secret_key);
    let mut approvals = BTreeSet::new();
    approvals.insert(approval);
    let mint = (transaction_hash, approvals);
    let mut transactions = BTreeMap::new();
    transactions.insert(TransactionCategory::Mint, vec![mint]);
    let random_bit = true;
    let block_payload = BlockPayload::new(transactions, vec![], rewarded_signatures, random_bit);
    let era_report = Some(InternalEraReport::doc_example().clone());
    let timestamp = *Timestamp::doc_example();
    let era_id = EraId::from(1);
    let height = 10;
    let public_key = PublicKey::from(secret_key);
    FinalizedBlock::new(
        block_payload,
        era_report,
        timestamp,
        era_id,
        height,
        public_key,
    )
});

static INTERNAL_ERA_REPORT: Lazy<InternalEraReport> = Lazy::new(|| {
    let secret_key_1 = SecretKey::ed25519_from_bytes([0; 32]).unwrap();
    let public_key_1 = PublicKey::from(&secret_key_1);
    let equivocators = vec![public_key_1];

    let secret_key_3 = SecretKey::ed25519_from_bytes([2; 32]).unwrap();
    let public_key_3 = PublicKey::from(&secret_key_3);
    let inactive_validators = vec![public_key_3];

    InternalEraReport {
        equivocators,
        inactive_validators,
    }
});

/// The piece of information that will become the content of a future block after it was finalized
/// and before execution happened yet.
#[derive(Clone, DataSize, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FinalizedBlock {
    pub(crate) mint: Vec<TransactionHash>,
    pub(crate) auction: Vec<TransactionHash>,
    pub(crate) install_upgrade: Vec<TransactionHash>,
    pub(crate) standard: Vec<TransactionHash>,
    pub(crate) rewarded_signatures: RewardedSignatures,
    pub(crate) timestamp: Timestamp,
    pub(crate) random_bit: bool,
    pub(crate) era_report: Option<InternalEraReport>,
    pub(crate) era_id: EraId,
    pub(crate) height: u64,
    pub(crate) proposer: Box<PublicKey>,
}

/// `EraReport` used only internally. The one in types is a part of `EraEndV1`.
#[derive(
    Clone, DataSize, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize, Default,
)]
pub struct InternalEraReport {
    /// The set of equivocators.
    pub equivocators: Vec<PublicKey>,
    /// Validators that haven't produced any unit during the era.
    pub inactive_validators: Vec<PublicKey>,
}

impl FinalizedBlock {
    pub(crate) fn new(
        block_payload: BlockPayload,
        era_report: Option<InternalEraReport>,
        timestamp: Timestamp,
        era_id: EraId,
        height: u64,
        proposer: PublicKey,
    ) -> Self {
        FinalizedBlock {
            mint: block_payload.mint().map(|(x, _)| x).copied().collect(),
            auction: block_payload.auction().map(|(x, _)| x).copied().collect(),
            install_upgrade: block_payload
                .install_upgrade()
                .map(|(x, _)| x)
                .copied()
                .collect(),
            standard: block_payload.standard().map(|(x, _)| x).copied().collect(),
            rewarded_signatures: block_payload.rewarded_signatures().clone(),
            timestamp,
            random_bit: block_payload.random_bit(),
            era_report,
            era_id,
            height,
            proposer: Box::new(proposer),
        }
    }

    /// The list of all transaction hashes.
    pub(crate) fn all_transactions(&self) -> impl Iterator<Item = &TransactionHash> {
        self.mint
            .iter()
            .chain(&self.auction)
            .chain(&self.install_upgrade)
            .chain(&self.standard)
    }

    /// Generates a random instance using a `TestRng` and includes specified transactions.
    #[cfg(test)]
    pub(crate) fn random<'a, I: IntoIterator<Item = &'a Transaction>>(
        rng: &mut TestRng,
        txns_iter: I,
    ) -> Self {
        let era = rng.gen_range(0..5);
        let height = era * 10 + rng.gen_range(0..10);
        let is_switch = rng.gen_bool(0.1);

        FinalizedBlock::random_with_specifics(
            rng,
            EraId::from(era),
            height,
            is_switch,
            Timestamp::now(),
            txns_iter,
        )
    }

    /// Generates a random instance using a `TestRng`, but using the specified values.
    /// If `transaction` is `None`, random transactions will be generated, otherwise, the provided `transaction`
    /// will be used.
    #[cfg(test)]
    pub(crate) fn random_with_specifics<'a, I: IntoIterator<Item = &'a Transaction>>(
        rng: &mut TestRng,
        era_id: EraId,
        height: u64,
        is_switch: bool,
        timestamp: Timestamp,
        txns_iter: I,
    ) -> Self {
        let mut transactions = BTreeMap::new();
        let mut standard = vec![];
        for transaction in txns_iter {
            standard.push((transaction.hash(), BTreeSet::new()));
        }
        transactions.insert(TransactionCategory::Standard, standard);
        let rewarded_signatures = Default::default();
        let random_bit = rng.gen();
        let block_payload =
            BlockPayload::new(transactions, vec![], rewarded_signatures, random_bit);

        let era_report = if is_switch {
            Some(InternalEraReport::random(rng))
        } else {
            None
        };
        let secret_key: SecretKey = SecretKey::ed25519_from_bytes(rng.gen::<[u8; 32]>()).unwrap();
        let public_key = PublicKey::from(&secret_key);

        FinalizedBlock::new(
            block_payload,
            era_report,
            timestamp,
            era_id,
            height,
            public_key,
        )
    }
}

impl DocExample for FinalizedBlock {
    fn doc_example() -> &'static Self {
        &FINALIZED_BLOCK
    }
}

impl DocExample for InternalEraReport {
    fn doc_example() -> &'static Self {
        &INTERNAL_ERA_REPORT
    }
}

impl From<BlockV2> for FinalizedBlock {
    fn from(block: BlockV2) -> Self {
        FinalizedBlock {
            mint: block.mint().copied().collect(),
            auction: block.auction().copied().collect(),
            install_upgrade: block.install_upgrade().copied().collect(),
            standard: block.standard().copied().collect(),
            timestamp: block.timestamp(),
            random_bit: block.random_bit(),
            era_report: block.era_end().map(|era_end| InternalEraReport {
                equivocators: Vec::from(era_end.equivocators()),
                inactive_validators: Vec::from(era_end.inactive_validators()),
            }),
            era_id: block.era_id(),
            height: block.height(),
            proposer: Box::new(block.proposer().clone()),
            rewarded_signatures: block.rewarded_signatures().clone(),
        }
    }
}

impl Display for FinalizedBlock {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "finalized block #{} in {}, timestamp {}, {} mint, {} auction txns, {} \
            install/upgrade txns, {} standard txns",
            self.height,
            self.era_id,
            self.timestamp,
            self.mint.len(),
            self.auction.len(),
            self.install_upgrade.len(),
            self.standard.len(),
        )?;
        if let Some(ref ee) = self.era_report {
            write!(formatter, ", era_end: {:?}", ee)?;
        }
        Ok(())
    }
}

impl InternalEraReport {
    /// Returns a random `InternalEraReport`.
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        let equivocators_count = rng.gen_range(0..5);
        let inactive_count = rng.gen_range(0..5);
        let equivocators = core::iter::repeat_with(|| PublicKey::random(rng))
            .take(equivocators_count)
            .collect();
        let inactive_validators = core::iter::repeat_with(|| PublicKey::random(rng))
            .take(inactive_count)
            .collect();

        InternalEraReport {
            equivocators,
            inactive_validators,
        }
    }
}
