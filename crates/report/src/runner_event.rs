//! The types associated with the events sent by the runner to the reporter.
#![allow(dead_code)]

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use alloy::primitives::Address;
use anyhow::Context as _;
use indexmap::IndexMap;
use revive_dt_common::types::PlatformIdentifier;
use revive_dt_compiler::{CompilerInput, CompilerOutput};
use revive_dt_format::metadata::ContractInstance;
use revive_dt_format::metadata::Metadata;
use revive_dt_format::steps::StepPath;
use semver::Version;
use tokio::sync::{broadcast, mpsc::UnboundedSender, oneshot};

use crate::MinedBlockInformation;
use crate::TransactionInformation;
use crate::{ExecutionSpecifier, ReporterEvent, TestSpecifier, common::MetadataFilePath};

// ---------------------------------------------------------------------------
// Event structs
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SubscribeToEventsEvent {
    pub tx: oneshot::Sender<broadcast::Receiver<ReporterEvent>>,
}

#[derive(Debug)]
pub(crate) struct MetadataFileDiscoveryEvent {
    pub path: MetadataFilePath,
    pub metadata: Metadata,
}

#[derive(Debug)]
pub(crate) struct TestCaseDiscoveryEvent {
    pub test_specifier: Arc<TestSpecifier>,
}

#[derive(Debug)]
pub(crate) struct TestIgnoredEvent {
    pub test_specifier: Arc<TestSpecifier>,
    pub reason: String,
    pub additional_fields: IndexMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub(crate) struct TestSucceededEvent {
    pub test_specifier: Arc<TestSpecifier>,
    pub steps_executed: usize,
}

#[derive(Debug)]
pub(crate) struct TestFailedEvent {
    pub test_specifier: Arc<TestSpecifier>,
    pub reason: String,
}

#[derive(Debug)]
pub(crate) struct NodeAssignedEvent {
    pub test_specifier: Arc<TestSpecifier>,
    pub id: usize,
    pub platform_identifier: PlatformIdentifier,
    pub connection_string: String,
}

#[derive(Debug)]
pub(crate) struct PreLinkContractsCompilationSucceededEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub compiler_version: Version,
    pub compiler_path: PathBuf,
    pub is_cached: bool,
    pub compiler_input: Option<CompilerInput>,
    pub compiler_output: CompilerOutput,
}

#[derive(Debug)]
pub(crate) struct PostLinkContractsCompilationSucceededEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub compiler_version: Version,
    pub compiler_path: PathBuf,
    pub is_cached: bool,
    pub compiler_input: Option<CompilerInput>,
    pub compiler_output: CompilerOutput,
}

#[derive(Debug)]
pub(crate) struct PreLinkContractsCompilationFailedEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub compiler_version: Option<Version>,
    pub compiler_path: Option<PathBuf>,
    pub compiler_input: Option<CompilerInput>,
    pub reason: String,
}

#[derive(Debug)]
pub(crate) struct PostLinkContractsCompilationFailedEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub compiler_version: Option<Version>,
    pub compiler_path: Option<PathBuf>,
    pub compiler_input: Option<CompilerInput>,
    pub reason: String,
}

#[derive(Debug)]
pub(crate) struct LibrariesDeployedEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub libraries: BTreeMap<ContractInstance, Address>,
}

#[derive(Debug)]
pub(crate) struct ContractDeployedEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub contract_instance: ContractInstance,
    pub address: Address,
}

#[derive(Debug)]
pub(crate) struct CompletionEvent {}

#[derive(Debug)]
pub(crate) struct StepTransactionInformationEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub step_path: StepPath,
    pub transaction_information: TransactionInformation,
}

#[derive(Debug)]
pub(crate) struct ContractInformationEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub source_code_path: PathBuf,
    pub contract_name: String,
    pub contract_size: usize,
}

#[derive(Debug)]
pub(crate) struct BlockMinedEvent {
    pub execution_specifier: Arc<ExecutionSpecifier>,
    pub mined_block_information: MinedBlockInformation,
}

// ---------------------------------------------------------------------------
// RunnerEvent enum
// ---------------------------------------------------------------------------

/// An event type that's sent by the test runners/drivers to the report aggregator.
#[derive(Debug)]
pub(crate) enum RunnerEvent {
    SubscribeToEvents(Box<SubscribeToEventsEvent>),
    MetadataFileDiscovery(Box<MetadataFileDiscoveryEvent>),
    TestCaseDiscovery(Box<TestCaseDiscoveryEvent>),
    TestIgnored(Box<TestIgnoredEvent>),
    TestSucceeded(Box<TestSucceededEvent>),
    TestFailed(Box<TestFailedEvent>),
    NodeAssigned(Box<NodeAssignedEvent>),
    PreLinkContractsCompilationSucceeded(Box<PreLinkContractsCompilationSucceededEvent>),
    PostLinkContractsCompilationSucceeded(Box<PostLinkContractsCompilationSucceededEvent>),
    PreLinkContractsCompilationFailed(Box<PreLinkContractsCompilationFailedEvent>),
    PostLinkContractsCompilationFailed(Box<PostLinkContractsCompilationFailedEvent>),
    LibrariesDeployed(Box<LibrariesDeployedEvent>),
    ContractDeployed(Box<ContractDeployedEvent>),
    Completion(Box<CompletionEvent>),
    StepTransactionInformation(Box<StepTransactionInformationEvent>),
    ContractInformation(Box<ContractInformationEvent>),
    BlockMined(Box<BlockMinedEvent>),
}

impl RunnerEvent {
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::SubscribeToEvents { .. } => "SubscribeToEvents",
            Self::MetadataFileDiscovery { .. } => "MetadataFileDiscovery",
            Self::TestCaseDiscovery { .. } => "TestCaseDiscovery",
            Self::TestIgnored { .. } => "TestIgnored",
            Self::TestSucceeded { .. } => "TestSucceeded",
            Self::TestFailed { .. } => "TestFailed",
            Self::NodeAssigned { .. } => "NodeAssigned",
            Self::PreLinkContractsCompilationSucceeded { .. } => {
                "PreLinkContractsCompilationSucceeded"
            }
            Self::PostLinkContractsCompilationSucceeded { .. } => {
                "PostLinkContractsCompilationSucceeded"
            }
            Self::PreLinkContractsCompilationFailed { .. } => "PreLinkContractsCompilationFailed",
            Self::PostLinkContractsCompilationFailed { .. } => "PostLinkContractsCompilationFailed",
            Self::LibrariesDeployed { .. } => "LibrariesDeployed",
            Self::ContractDeployed { .. } => "ContractDeployed",
            Self::Completion { .. } => "Completion",
            Self::StepTransactionInformation { .. } => "StepTransactionInformation",
            Self::ContractInformation { .. } => "ContractInformation",
            Self::BlockMined { .. } => "BlockMined",
        }
    }
}

// ---------------------------------------------------------------------------
// From impls (event struct → RunnerEvent)
// ---------------------------------------------------------------------------

macro_rules! impl_from_event {
    ($event:ident, $variant:ident) => {
        impl From<$event> for RunnerEvent {
            fn from(value: $event) -> Self {
                Self::$variant(Box::new(value))
            }
        }
    };
}

impl_from_event!(SubscribeToEventsEvent, SubscribeToEvents);
impl_from_event!(MetadataFileDiscoveryEvent, MetadataFileDiscovery);
impl_from_event!(TestCaseDiscoveryEvent, TestCaseDiscovery);
impl_from_event!(TestIgnoredEvent, TestIgnored);
impl_from_event!(TestSucceededEvent, TestSucceeded);
impl_from_event!(TestFailedEvent, TestFailed);
impl_from_event!(NodeAssignedEvent, NodeAssigned);
impl_from_event!(
    PreLinkContractsCompilationSucceededEvent,
    PreLinkContractsCompilationSucceeded
);
impl_from_event!(
    PostLinkContractsCompilationSucceededEvent,
    PostLinkContractsCompilationSucceeded
);
impl_from_event!(
    PreLinkContractsCompilationFailedEvent,
    PreLinkContractsCompilationFailed
);
impl_from_event!(
    PostLinkContractsCompilationFailedEvent,
    PostLinkContractsCompilationFailed
);
impl_from_event!(LibrariesDeployedEvent, LibrariesDeployed);
impl_from_event!(ContractDeployedEvent, ContractDeployed);
impl_from_event!(CompletionEvent, Completion);
impl_from_event!(StepTransactionInformationEvent, StepTransactionInformation);
impl_from_event!(ContractInformationEvent, ContractInformation);
impl_from_event!(BlockMinedEvent, BlockMined);

// ---------------------------------------------------------------------------
// RunnerEventReporter — root reporter with methods for all events
// ---------------------------------------------------------------------------

/// Provides a way to report events to the aggregator.
///
/// Under the hood, this is a wrapper around an [`UnboundedSender`] which abstracts away
/// the fact that channels are used and that implements high-level methods for reporting
/// various events to the aggregator.
#[derive(Clone, Debug)]
pub struct RunnerEventReporter(pub(crate) UnboundedSender<RunnerEvent>);

impl From<UnboundedSender<RunnerEvent>> for RunnerEventReporter {
    fn from(value: UnboundedSender<RunnerEvent>) -> Self {
        Self(value)
    }
}

impl RunnerEventReporter {
    fn report(&self, event: impl Into<RunnerEvent>) -> anyhow::Result<()> {
        self.0.send(event.into()).map_err(Into::into)
    }

    pub fn test_specific_reporter(
        &self,
        test_specifier: impl Into<Arc<TestSpecifier>>,
    ) -> RunnerEventTestSpecificReporter {
        RunnerEventTestSpecificReporter {
            reporter: self.clone(),
            test_specifier: test_specifier.into(),
        }
    }

    pub fn report_subscribe_to_events_event(
        &self,
        tx: impl Into<oneshot::Sender<broadcast::Receiver<ReporterEvent>>>,
    ) -> anyhow::Result<()> {
        self.report(SubscribeToEventsEvent { tx: tx.into() })
    }

    pub fn report_metadata_file_discovery_event(
        &self,
        path: impl Into<MetadataFilePath>,
        metadata: impl Into<Metadata>,
    ) -> anyhow::Result<()> {
        self.report(MetadataFileDiscoveryEvent {
            path: path.into(),
            metadata: metadata.into(),
        })
    }

    pub fn report_test_case_discovery_event(
        &self,
        test_specifier: impl Into<Arc<TestSpecifier>>,
    ) -> anyhow::Result<()> {
        self.report(TestCaseDiscoveryEvent {
            test_specifier: test_specifier.into(),
        })
    }

    pub fn report_test_ignored_event(
        &self,
        test_specifier: impl Into<Arc<TestSpecifier>>,
        reason: impl Into<String>,
        additional_fields: impl Into<IndexMap<String, serde_json::Value>>,
    ) -> anyhow::Result<()> {
        self.report(TestIgnoredEvent {
            test_specifier: test_specifier.into(),
            reason: reason.into(),
            additional_fields: additional_fields.into(),
        })
    }

    pub fn report_test_succeeded_event(
        &self,
        test_specifier: impl Into<Arc<TestSpecifier>>,
        steps_executed: impl Into<usize>,
    ) -> anyhow::Result<()> {
        self.report(TestSucceededEvent {
            test_specifier: test_specifier.into(),
            steps_executed: steps_executed.into(),
        })
    }

    pub fn report_test_failed_event(
        &self,
        test_specifier: impl Into<Arc<TestSpecifier>>,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.report(TestFailedEvent {
            test_specifier: test_specifier.into(),
            reason: reason.into(),
        })
    }

    pub fn report_node_assigned_event(
        &self,
        test_specifier: impl Into<Arc<TestSpecifier>>,
        id: impl Into<usize>,
        platform_identifier: impl Into<PlatformIdentifier>,
        connection_string: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.report(NodeAssignedEvent {
            test_specifier: test_specifier.into(),
            id: id.into(),
            platform_identifier: platform_identifier.into(),
            connection_string: connection_string.into(),
        })
    }

    pub fn report_pre_link_contracts_compilation_succeeded_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        compiler_version: impl Into<Version>,
        compiler_path: impl Into<PathBuf>,
        is_cached: impl Into<bool>,
        compiler_input: impl Into<Option<CompilerInput>>,
        compiler_output: impl Into<CompilerOutput>,
    ) -> anyhow::Result<()> {
        self.report(PreLinkContractsCompilationSucceededEvent {
            execution_specifier: execution_specifier.into(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            is_cached: is_cached.into(),
            compiler_input: compiler_input.into(),
            compiler_output: compiler_output.into(),
        })
    }

    pub fn report_post_link_contracts_compilation_succeeded_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        compiler_version: impl Into<Version>,
        compiler_path: impl Into<PathBuf>,
        is_cached: impl Into<bool>,
        compiler_input: impl Into<Option<CompilerInput>>,
        compiler_output: impl Into<CompilerOutput>,
    ) -> anyhow::Result<()> {
        self.report(PostLinkContractsCompilationSucceededEvent {
            execution_specifier: execution_specifier.into(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            is_cached: is_cached.into(),
            compiler_input: compiler_input.into(),
            compiler_output: compiler_output.into(),
        })
    }

    pub fn report_pre_link_contracts_compilation_failed_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        compiler_version: impl Into<Option<Version>>,
        compiler_path: impl Into<Option<PathBuf>>,
        compiler_input: impl Into<Option<CompilerInput>>,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.report(PreLinkContractsCompilationFailedEvent {
            execution_specifier: execution_specifier.into(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            compiler_input: compiler_input.into(),
            reason: reason.into(),
        })
    }

    pub fn report_post_link_contracts_compilation_failed_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        compiler_version: impl Into<Option<Version>>,
        compiler_path: impl Into<Option<PathBuf>>,
        compiler_input: impl Into<Option<CompilerInput>>,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.report(PostLinkContractsCompilationFailedEvent {
            execution_specifier: execution_specifier.into(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            compiler_input: compiler_input.into(),
            reason: reason.into(),
        })
    }

    pub fn report_libraries_deployed_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        libraries: impl Into<BTreeMap<ContractInstance, Address>>,
    ) -> anyhow::Result<()> {
        self.report(LibrariesDeployedEvent {
            execution_specifier: execution_specifier.into(),
            libraries: libraries.into(),
        })
    }

    pub fn report_contract_deployed_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        contract_instance: impl Into<ContractInstance>,
        address: impl Into<Address>,
    ) -> anyhow::Result<()> {
        self.report(ContractDeployedEvent {
            execution_specifier: execution_specifier.into(),
            contract_instance: contract_instance.into(),
            address: address.into(),
        })
    }

    pub fn report_completion_event(&self) -> anyhow::Result<()> {
        self.report(CompletionEvent {})
    }

    pub fn report_step_transaction_information_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        step_path: impl Into<StepPath>,
        transaction_information: impl Into<TransactionInformation>,
    ) -> anyhow::Result<()> {
        self.report(StepTransactionInformationEvent {
            execution_specifier: execution_specifier.into(),
            step_path: step_path.into(),
            transaction_information: transaction_information.into(),
        })
    }

    pub fn report_contract_information_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        source_code_path: impl Into<PathBuf>,
        contract_name: impl Into<String>,
        contract_size: impl Into<usize>,
    ) -> anyhow::Result<()> {
        self.report(ContractInformationEvent {
            execution_specifier: execution_specifier.into(),
            source_code_path: source_code_path.into(),
            contract_name: contract_name.into(),
            contract_size: contract_size.into(),
        })
    }

    pub fn report_block_mined_event(
        &self,
        execution_specifier: impl Into<Arc<ExecutionSpecifier>>,
        mined_block_information: impl Into<MinedBlockInformation>,
    ) -> anyhow::Result<()> {
        self.report(BlockMinedEvent {
            execution_specifier: execution_specifier.into(),
            mined_block_information: mined_block_information.into(),
        })
    }

    pub async fn subscribe(&self) -> anyhow::Result<broadcast::Receiver<ReporterEvent>> {
        let (tx, rx) = oneshot::channel::<broadcast::Receiver<ReporterEvent>>();
        self.report_subscribe_to_events_event(tx)
            .context("Failed to send subscribe request to reporter task")?;
        rx.await.map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// RunnerEventTestSpecificReporter — auto-fills test_specifier
// ---------------------------------------------------------------------------

/// A reporter that's tied to a specific test case.
#[derive(Clone, Debug)]
pub struct RunnerEventTestSpecificReporter {
    pub(crate) reporter: RunnerEventReporter,
    pub(crate) test_specifier: Arc<TestSpecifier>,
}

impl RunnerEventTestSpecificReporter {
    pub fn execution_specific_reporter(
        &self,
        node_id: impl Into<usize>,
        platform_identifier: impl Into<PlatformIdentifier>,
    ) -> RunnerEventExecutionSpecificReporter {
        RunnerEventExecutionSpecificReporter {
            reporter: self.reporter.clone(),
            execution_specifier: Arc::new(ExecutionSpecifier {
                test_specifier: self.test_specifier.clone(),
                node_id: node_id.into(),
                platform_identifier: platform_identifier.into(),
            }),
        }
    }

    fn report(&self, event: impl Into<RunnerEvent>) -> anyhow::Result<()> {
        self.reporter.report(event)
    }

    pub fn report_test_case_discovery_event(&self) -> anyhow::Result<()> {
        self.report(TestCaseDiscoveryEvent {
            test_specifier: self.test_specifier.clone(),
        })
    }

    pub fn report_test_ignored_event(
        &self,
        reason: impl Into<String>,
        additional_fields: impl Into<IndexMap<String, serde_json::Value>>,
    ) -> anyhow::Result<()> {
        self.report(TestIgnoredEvent {
            test_specifier: self.test_specifier.clone(),
            reason: reason.into(),
            additional_fields: additional_fields.into(),
        })
    }

    pub fn report_test_succeeded_event(
        &self,
        steps_executed: impl Into<usize>,
    ) -> anyhow::Result<()> {
        self.report(TestSucceededEvent {
            test_specifier: self.test_specifier.clone(),
            steps_executed: steps_executed.into(),
        })
    }

    pub fn report_test_failed_event(&self, reason: impl Into<String>) -> anyhow::Result<()> {
        self.report(TestFailedEvent {
            test_specifier: self.test_specifier.clone(),
            reason: reason.into(),
        })
    }

    pub fn report_node_assigned_event(
        &self,
        id: impl Into<usize>,
        platform_identifier: impl Into<PlatformIdentifier>,
        connection_string: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.report(NodeAssignedEvent {
            test_specifier: self.test_specifier.clone(),
            id: id.into(),
            platform_identifier: platform_identifier.into(),
            connection_string: connection_string.into(),
        })
    }
}

// ---------------------------------------------------------------------------
// RunnerEventExecutionSpecificReporter — auto-fills execution_specifier
// ---------------------------------------------------------------------------

/// A reporter that's tied to a specific execution of the test case such as execution on
/// a specific node from a specific platform.
#[derive(Clone, Debug)]
pub struct RunnerEventExecutionSpecificReporter {
    pub(crate) reporter: RunnerEventReporter,
    pub(crate) execution_specifier: Arc<ExecutionSpecifier>,
}

impl RunnerEventExecutionSpecificReporter {
    fn report(&self, event: impl Into<RunnerEvent>) -> anyhow::Result<()> {
        self.reporter.report(event)
    }

    pub fn report_pre_link_contracts_compilation_succeeded_event(
        &self,
        compiler_version: impl Into<Version>,
        compiler_path: impl Into<PathBuf>,
        is_cached: impl Into<bool>,
        compiler_input: impl Into<Option<CompilerInput>>,
        compiler_output: impl Into<CompilerOutput>,
    ) -> anyhow::Result<()> {
        self.report(PreLinkContractsCompilationSucceededEvent {
            execution_specifier: self.execution_specifier.clone(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            is_cached: is_cached.into(),
            compiler_input: compiler_input.into(),
            compiler_output: compiler_output.into(),
        })
    }

    pub fn report_post_link_contracts_compilation_succeeded_event(
        &self,
        compiler_version: impl Into<Version>,
        compiler_path: impl Into<PathBuf>,
        is_cached: impl Into<bool>,
        compiler_input: impl Into<Option<CompilerInput>>,
        compiler_output: impl Into<CompilerOutput>,
    ) -> anyhow::Result<()> {
        self.report(PostLinkContractsCompilationSucceededEvent {
            execution_specifier: self.execution_specifier.clone(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            is_cached: is_cached.into(),
            compiler_input: compiler_input.into(),
            compiler_output: compiler_output.into(),
        })
    }

    pub fn report_pre_link_contracts_compilation_failed_event(
        &self,
        compiler_version: impl Into<Option<Version>>,
        compiler_path: impl Into<Option<PathBuf>>,
        compiler_input: impl Into<Option<CompilerInput>>,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.report(PreLinkContractsCompilationFailedEvent {
            execution_specifier: self.execution_specifier.clone(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            compiler_input: compiler_input.into(),
            reason: reason.into(),
        })
    }

    pub fn report_post_link_contracts_compilation_failed_event(
        &self,
        compiler_version: impl Into<Option<Version>>,
        compiler_path: impl Into<Option<PathBuf>>,
        compiler_input: impl Into<Option<CompilerInput>>,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        self.report(PostLinkContractsCompilationFailedEvent {
            execution_specifier: self.execution_specifier.clone(),
            compiler_version: compiler_version.into(),
            compiler_path: compiler_path.into(),
            compiler_input: compiler_input.into(),
            reason: reason.into(),
        })
    }

    pub fn report_libraries_deployed_event(
        &self,
        libraries: impl Into<BTreeMap<ContractInstance, Address>>,
    ) -> anyhow::Result<()> {
        self.report(LibrariesDeployedEvent {
            execution_specifier: self.execution_specifier.clone(),
            libraries: libraries.into(),
        })
    }

    pub fn report_contract_deployed_event(
        &self,
        contract_instance: impl Into<ContractInstance>,
        address: impl Into<Address>,
    ) -> anyhow::Result<()> {
        self.report(ContractDeployedEvent {
            execution_specifier: self.execution_specifier.clone(),
            contract_instance: contract_instance.into(),
            address: address.into(),
        })
    }

    pub fn report_step_transaction_information_event(
        &self,
        step_path: impl Into<StepPath>,
        transaction_information: impl Into<TransactionInformation>,
    ) -> anyhow::Result<()> {
        self.report(StepTransactionInformationEvent {
            execution_specifier: self.execution_specifier.clone(),
            step_path: step_path.into(),
            transaction_information: transaction_information.into(),
        })
    }

    pub fn report_contract_information_event(
        &self,
        source_code_path: impl Into<PathBuf>,
        contract_name: impl Into<String>,
        contract_size: impl Into<usize>,
    ) -> anyhow::Result<()> {
        self.report(ContractInformationEvent {
            execution_specifier: self.execution_specifier.clone(),
            source_code_path: source_code_path.into(),
            contract_name: contract_name.into(),
            contract_size: contract_size.into(),
        })
    }

    pub fn report_block_mined_event(
        &self,
        mined_block_information: impl Into<MinedBlockInformation>,
    ) -> anyhow::Result<()> {
        self.report(BlockMinedEvent {
            execution_specifier: self.execution_specifier.clone(),
            mined_block_information: mined_block_information.into(),
        })
    }
}

// ---------------------------------------------------------------------------
// RunnerEventStepExecutionSpecificReporter — auto-fills step_specifier
// ---------------------------------------------------------------------------

/// A reporter that's tied to a specific step execution.
#[derive(Clone, Debug)]
pub struct RunnerEventStepExecutionSpecificReporter {
    pub(crate) reporter: RunnerEventReporter,
    pub(crate) step_specifier: Arc<crate::common::StepExecutionSpecifier>,
}

impl RunnerEventStepExecutionSpecificReporter {
    fn report(&self, event: impl Into<RunnerEvent>) -> anyhow::Result<()> {
        self.reporter.report(event)
    }
}

// ---------------------------------------------------------------------------
// Type aliases for convenience
// ---------------------------------------------------------------------------

pub type Reporter = RunnerEventReporter;
pub type TestSpecificReporter = RunnerEventTestSpecificReporter;
pub type ExecutionSpecificReporter = RunnerEventExecutionSpecificReporter;
