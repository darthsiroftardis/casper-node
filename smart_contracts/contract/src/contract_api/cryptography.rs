//! Functions with cryptographic utils.

use casper_types::{api_error, HashAlgorithm, BLAKE2B_DIGEST_LENGTH};

use crate::{ext_ffi, unwrap_or_revert::UnwrapOrRevert};

/// Computes digest hash, using provided algorithm type.
pub fn generic_hash<T: AsRef<[u8]>>(input: T, algo: HashAlgorithm) -> [u8; 32] {
    let mut ret = [0; 32];

    let result = unsafe {
        ext_ffi::casper_generic_hash(
            input.as_ref().as_ptr(),
            input.as_ref().len(),
            algo as u8,
            ret.as_mut_ptr(),
            BLAKE2B_DIGEST_LENGTH,
        )
    };
    api_error::result_from(result).unwrap_or_revert();
    ret
}