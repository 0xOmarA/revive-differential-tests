mod eth_rpc_proxy;
pub mod polkavm_upload;
mod process;
pub mod shared_node_ops;

pub use eth_rpc_proxy::*;
pub use process::*;

use std::sync::atomic::{AtomicU32, Ordering};

use alloy::primitives::Address;
use sp_core::crypto::Ss58Codec;
use sp_runtime::AccountId32;

static GLOBAL_NODE_COUNT: AtomicU32 = AtomicU32::new(0);

/// Allocates a unique node ID. Each call returns a new monotonically increasing ID.
pub fn allocate_node_id() -> u32 {
    GLOBAL_NODE_COUNT.fetch_add(1, Ordering::SeqCst)
}

/// Converts an Ethereum address to a Substrate SS58 address by padding the 20-byte
/// Ethereum address to 32 bytes (using 0xEE as filler) and encoding as SS58.
pub fn eth_to_substrate_address(address: &Address) -> String {
    let eth_bytes = address.0.0;

    let mut padded = [0xEEu8; 32];
    padded[..20].copy_from_slice(&eth_bytes);

    let account_id = AccountId32::from(padded);
    account_id.to_ss58check()
}
