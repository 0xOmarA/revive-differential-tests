use alloy::{
    primitives::{Address, address},
    rpc::types::TransactionRequest,
};
use anyhow::Context as _;
use subxt::{ext::codec::Decode, metadata::Metadata, tx::Payload};

#[subxt::subxt(runtime_metadata_path = "../../assets/revive_metadata.scale")]
mod revive {}

const RUNTIME_PALLET_ADDRESS: Address = address!("0x6d6f646c70792f70616464720000000000000000");

/// Encodes PolkaVM code upload transactions for all provided contract bytecodes.
///
/// Each bytecode is wrapped in a `revive.upload_code` extrinsic call targeting
/// the runtime pallet address. The returned transaction requests can be executed
/// via `EthereumNode::execute_transaction`.
pub fn encode_upload_transactions(
    bytecodes: &[Vec<u8>],
    deployer: Address,
) -> anyhow::Result<Vec<TransactionRequest>> {
    let metadata_bytes = include_bytes!("../../../../assets/revive_metadata.scale");
    let metadata = Metadata::decode(&mut &metadata_bytes[..])
        .context("Failed to decode the revive metadata")?;

    bytecodes
        .iter()
        .map(|code| {
            let payload = revive::tx().revive().upload_code(code.clone(), u128::MAX);
            let encoded_payload = payload
                .encode_call_data(&metadata)
                .context("Failed to encode the upload code payload")?;

            Ok(TransactionRequest::default()
                .from(deployer)
                .to(RUNTIME_PALLET_ADDRESS)
                .input(encoded_payload.into()))
        })
        .collect()
}
