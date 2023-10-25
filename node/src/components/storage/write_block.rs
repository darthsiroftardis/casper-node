use std::{collections::HashMap, rc::Rc};

use lmdb::{Database, RwTransaction, Transaction};

use casper_types::{
    execution::ExecutionResult, Block, BlockBody, BlockBodyV1, BlockHash, BlockHeader,
    BlockHeaderV1, DeployHash, Digest,
};
use tracing::error;

use crate::types::ApprovalsHashes;

use super::{
    lmdb_ext::{LmdbExtError, WriteTransactionExt},
    FatalStorageError, Storage,
};

impl Storage {
    /// Verifies a block and writes it to a block to storage, updating indices as
    /// necessary. This function should only be used by components that deal with historical blocks,
    /// for example: `Fetcher`.
    ///
    /// Returns `Ok(true)` if the block has been successfully written, `Ok(false)` if a part of it
    /// couldn't be written because it already existed, and `Err(_)` if there was an error.
    #[cfg_attr(doc, aquamarine::aquamarine)]
    /// ```mermaid
    /// flowchart TD
    ///     style Start fill:#66ccff,stroke:#333,stroke-width:4px
    ///     style End fill:#66ccff,stroke:#333,stroke-width:4px
    ///     style write_block fill:#00FF00,stroke:#333,stroke-width:4px
    ///     
    ///     Start --> A[Block fetched]
    ///     A --> put_block_to_storage
    ///     put_block_to_storage --> StorageRequest::PutBlock
    ///     StorageRequest::PutBlock --> write_block
    ///     write_block --> write_validated_block
    ///     write_validated_block --> B{"is it a legacy block?<br>(V1)"}
    ///     B -->|Yes| put_single_legacy_block_body
    ///     B -->|No| put_single_block_body
    ///     put_single_legacy_block_body --> D[store header in DB]
    ///     put_single_block_body --> D[store header in DB]
    ///     D --> C[update indices]
    ///     C --> End
    /// ```
    pub fn write_block(&mut self, block: &Block) -> Result<bool, FatalStorageError> {
        block.verify()?;
        let env = Rc::clone(&self.env);
        let mut txn = env.begin_rw_txn()?;
        let wrote = self.write_validated_block(&mut txn, block)?;
        if wrote {
            txn.commit()?;
        }
        Ok(wrote)
    }

    /// Writes a block which has already been verified to storage, updating indices as necessary.
    ///
    /// Returns `Ok(true)` if the block has been successfully written, `Ok(false)` if a part of it
    /// couldn't be written because it already existed, and `Err(_)` if there was an error.
    #[cfg_attr(doc, aquamarine::aquamarine)]
    /// ```mermaid
    ///     flowchart TD
    ///     style Start fill:#66ccff,stroke:#333,stroke-width:4px
    ///     style End fill:#66ccff,stroke:#333,stroke-width:4px
    ///     style B fill:#00FF00,stroke:#333,stroke-width:4px
    ///     
    ///     Start --> A["Validated block needs to be stored<br>(might be coming from contract runtime)"]
    ///     A --> put_executed_block_to_storage
    ///     put_executed_block_to_storage --> StorageRequest::PutExecutedBlock
    ///     StorageRequest::PutExecutedBlock --> put_executed_block
    ///     put_executed_block --> B["write_validated_block<br>(current version)"]
    ///     B --> C[convert into BlockBody]
    ///     C --> put_single_block_body
    ///     put_single_block_body --> write_block_header
    ///     write_block_header --> D[update indices]
    ///     D --> End
    /// ```
    fn write_validated_block(
        &mut self,
        txn: &mut RwTransaction,
        block: &Block,
    ) -> Result<bool, FatalStorageError> {
        // Insert the body:
        {
            let block_body_hash = block.body_hash();
            match block {
                Block::V1(v1) => {
                    let block_body = v1.body();
                    if !Self::put_single_legacy_block_body(
                        txn,
                        block_body_hash,
                        block_body,
                        self.block_body_dbs.legacy,
                    )? {
                        error!("could not insert body for: {}", block);
                        return Ok(false);
                    }
                }
                Block::V2(_) => {
                    let block_body = block.clone_body();
                    if !Self::put_single_block_body(
                        txn,
                        block_body_hash,
                        &block_body,
                        self.block_body_dbs.current,
                    )? {
                        error!("could not insert body for: {}", block);
                        return Ok(false);
                    }
                }
            }
        }

        // Insert the header:
        {
            let block_hash = block.hash();
            match block {
                Block::V1(v1) => {
                    let block_header = v1.header();
                    if !Self::put_single_legacy_block_header(
                        txn,
                        block_hash,
                        block_header,
                        self.block_header_dbs.legacy,
                    )? {
                        error!("could not insert header for: {}", block);
                        return Ok(false);
                    }
                }
                Block::V2(_) => {
                    let block_header = block.clone_header();
                    if !Self::put_single_block_header(
                        txn,
                        block_hash,
                        &block_header,
                        self.block_header_dbs.current,
                    )? {
                        error!("could not insert header for: {}", block);
                        return Ok(false);
                    }
                }
            }
        }

        {
            Self::insert_to_block_header_indices(
                &mut self.block_height_index,
                &mut self.switch_block_era_id_index,
                &block.clone_header(),
            )?;
            Self::insert_to_deploy_index(
                &mut self.deploy_hash_index,
                *block.hash(),
                block.height(),
                block.clone_body().deploy_and_transfer_hashes(),
            )?;
        }
        Ok(true)
    }

    /// Writes a single block header in a separate transaction to storage.
    fn put_single_legacy_block_header(
        txn: &mut RwTransaction,
        block_hash: &BlockHash,
        block_header: &BlockHeaderV1,
        db: Database,
    ) -> Result<bool, LmdbExtError> {
        txn.put_value(db, block_hash, block_header, true)
            .map_err(Into::into)
    }

    /// Writes a single block header in a separate transaction to storage.
    fn put_single_block_header(
        txn: &mut RwTransaction,
        block_hash: &BlockHash,
        block_header: &BlockHeader,
        db: Database,
    ) -> Result<bool, LmdbExtError> {
        debug_assert!(!matches!(block_header, BlockHeader::V1(_)));
        txn.put_value(db, block_hash, block_header, true)
            .map_err(Into::into)
    }

    /// Writes a single block body in a separate transaction to storage.
    fn put_single_legacy_block_body(
        txn: &mut RwTransaction,
        block_body_hash: &Digest,
        block_body: &BlockBodyV1,
        db: Database,
    ) -> Result<bool, LmdbExtError> {
        txn.put_value(db, block_body_hash, block_body, true)
            .map_err(Into::into)
    }

    /// Writes a single block body in a separate transaction to storage.
    fn put_single_block_body(
        txn: &mut RwTransaction,
        block_body_hash: &Digest,
        block_body: &BlockBody,
        db: Database,
    ) -> Result<bool, LmdbExtError> {
        debug_assert!(!matches!(block_body, BlockBody::V1(_)));
        txn.put_value(db, block_body_hash, block_body, true)
            .map_err(Into::into)
    }

    pub(crate) fn put_executed_block(
        &mut self,
        block: &Block,
        approvals_hashes: &ApprovalsHashes,
        execution_results: HashMap<DeployHash, ExecutionResult>,
    ) -> Result<bool, FatalStorageError> {
        let env = Rc::clone(&self.env);
        let mut txn = env.begin_rw_txn()?;
        let wrote = self.write_validated_block(&mut txn, block)?;
        if !wrote {
            return Err(FatalStorageError::FailedToOverwriteBlock);
        }

        let _ = self.write_approvals_hashes(&mut txn, approvals_hashes)?;
        let _ = self.write_execution_results(
            &mut txn,
            block.hash(),
            block.height(),
            execution_results,
        )?;
        txn.commit()?;

        Ok(true)
    }
}