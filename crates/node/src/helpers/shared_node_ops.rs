//! Shared implementations of [`EthereumNode`] methods.
//!
//! Most node implementations have identical logic for submitting transactions, getting
//! receipts, tracing, etc. — they only differ in how they obtain the provider. These
//! free functions factor out the common logic so each node implementation becomes a
//! thin delegation layer.

use alloy::network::Ethereum;
use alloy::primitives::{Address, StorageKey, TxHash, U256};
use alloy::providers::{DynProvider, Provider, ext::DebugApi};
use alloy::rpc::types::trace::geth::{
    DiffMode, GethDebugTracingOptions, GethTrace, PreStateConfig, PreStateFrame,
};
use alloy::rpc::types::{EIP1186AccountProofResponse, TransactionReceipt, TransactionRequest};
use anyhow::Context as _;

pub async fn submit_transaction(
    provider: &DynProvider<Ethereum>,
    transaction: TransactionRequest,
) -> anyhow::Result<TxHash> {
    let pending_transaction = provider
        .send_transaction(transaction)
        .await
        .context("Failed to submit the transaction through the provider")?;
    Ok(*pending_transaction.tx_hash())
}

pub async fn execute_transaction(
    provider: &DynProvider<Ethereum>,
    transaction: TransactionRequest,
) -> anyhow::Result<TransactionReceipt> {
    provider
        .send_transaction(transaction)
        .await
        .context("Encountered an error when submitting a transaction")?
        .get_receipt()
        .await
        .context("Failed to get the receipt for the transaction")
}

pub async fn get_receipt(
    provider: &DynProvider<Ethereum>,
    tx_hash: TxHash,
) -> anyhow::Result<TransactionReceipt> {
    provider
        .get_transaction_receipt(tx_hash)
        .await
        .context("Failed to get the receipt of the transaction")?
        .context("Failed to get the receipt of the transaction")
}

pub async fn trace_transaction(
    provider: &DynProvider<Ethereum>,
    tx_hash: TxHash,
    trace_options: GethDebugTracingOptions,
) -> anyhow::Result<GethTrace> {
    provider
        .debug_trace_transaction(tx_hash, trace_options)
        .await
        .context("Failed to obtain debug trace from provider")
}

pub async fn state_diff(
    provider: &DynProvider<Ethereum>,
    tx_hash: TxHash,
) -> anyhow::Result<DiffMode> {
    let trace_options = GethDebugTracingOptions::prestate_tracer(PreStateConfig {
        diff_mode: Some(true),
        disable_code: None,
        disable_storage: None,
    });
    match trace_transaction(provider, tx_hash, trace_options)
        .await?
        .try_into_pre_state_frame()?
    {
        PreStateFrame::Diff(diff) => Ok(diff),
        _ => anyhow::bail!("expected a diff mode trace"),
    }
}

pub async fn balance_of(
    provider: &DynProvider<Ethereum>,
    address: Address,
) -> anyhow::Result<U256> {
    provider.get_balance(address).await.map_err(Into::into)
}

pub async fn latest_state_proof(
    provider: &DynProvider<Ethereum>,
    address: Address,
    keys: Vec<StorageKey>,
) -> anyhow::Result<EIP1186AccountProofResponse> {
    provider
        .get_proof(address, keys)
        .latest()
        .await
        .map_err(Into::into)
}
