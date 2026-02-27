//! This crate implements all node interactions.

use std::pin::Pin;
use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::{Address, StorageKey, TxHash, U256};
use alloy::providers::DynProvider;
use alloy::rpc::types::trace::geth::{DiffMode, GethDebugTracingOptions, GethTrace};
use alloy::rpc::types::{EIP1186AccountProofResponse, TransactionReceipt, TransactionRequest};
use anyhow::Result;

use futures::Stream;
use revive_common::EVMVersion;
use revive_dt_format::traits::ResolverApi;
use revive_dt_report::MinedBlockInformation;

/// An interface for all interactions with Ethereum compatible nodes.
#[allow(clippy::type_complexity)]
pub trait EthereumNode: Send + Sync {
    /// A function to run post spawning the nodes and before any transactions are run on the node.
    fn pre_transactions(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    fn id(&self) -> usize;

    /// Returns the nodes connection string.
    fn connection_string(&self) -> &str;

    fn submit_transaction(
        &self,
        transaction: TransactionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>>;

    fn get_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Pin<Box<dyn Future<Output = Result<TransactionReceipt>> + Send + '_>>;

    /// Execute the [TransactionRequest] and return a [TransactionReceipt].
    fn execute_transaction(
        &self,
        transaction: TransactionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<TransactionReceipt>> + Send + '_>>;

    /// Trace the transaction in the [TransactionReceipt] and return a [GethTrace].
    fn trace_transaction(
        &self,
        tx_hash: TxHash,
        trace_options: GethDebugTracingOptions,
    ) -> Pin<Box<dyn Future<Output = Result<GethTrace>> + Send + '_>>;

    /// Returns the state diff of the transaction hash in the [TransactionReceipt].
    fn state_diff(
        &self,
        tx_hash: TxHash,
    ) -> Pin<Box<dyn Future<Output = Result<DiffMode>> + Send + '_>>;

    /// Returns the balance of the provided [`Address`] back.
    fn balance_of(
        &self,
        address: Address,
    ) -> Pin<Box<dyn Future<Output = Result<U256>> + Send + '_>>;

    /// Returns the latest storage proof of the provided [`Address`]
    fn latest_state_proof(
        &self,
        address: Address,
        keys: Vec<StorageKey>,
    ) -> Pin<Box<dyn Future<Output = Result<EIP1186AccountProofResponse>> + Send + '_>>;

    /// Returns the resolver that is to use with this ethereum node.
    fn resolver(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ResolverApi>>> + Send + '_>>;

    /// Returns the EVM version of the node.
    fn evm_version(&self) -> EVMVersion;

    /// Returns a stream of the blocks that were mined by the node.
    fn subscribe_to_full_blocks_information(
        &self,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = anyhow::Result<
                        Pin<Box<dyn Stream<Item = MinedBlockInformation> + Send>>,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn provider(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<DynProvider<Ethereum>>> + Send + '_>>;

    /// Uploads contract code to the chain before test execution. This is a no-op for
    /// EVM-based nodes but required for PolkaVM nodes where factory contracts reference
    /// code by hash rather than including bytecode inline.
    fn upload_code(
        &self,
        _bytecodes: &[Vec<u8>],
        _deployer: Address,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}
