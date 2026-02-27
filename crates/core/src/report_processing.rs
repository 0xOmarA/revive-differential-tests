use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Display,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use revive_dt_common::types::Mode;
use revive_dt_config::{ReportAction, ReportStatus};
use revive_dt_report::{Report, TestCaseStatus};

type Expectations<'a> = BTreeMap<TestSpecifier<'a>, ReportStatus>;

pub fn handle_report(action: ReportAction) -> Result<()> {
    match action {
        ReportAction::GenerateExpectationsFile {
            report_path,
            output_path,
            remove_prefix,
            include_status,
        } => {
            let report: Report = {
                let file = File::open(&report_path).context("Failed to open the report file")?;
                serde_json::from_reader(&file).context("Failed to deserialize the report")?
            };

            let remove_prefix = remove_prefix
                .into_iter()
                .map(|path| path.canonicalize().context("Failed to canonicalize path"))
                .collect::<Result<Vec<_>>>()?;
            let include_status =
                include_status.map(|value| value.into_iter().collect::<HashSet<_>>());

            let expectations = report
                .execution_information
                .iter()
                .flat_map(|(metadata_file_path, metadata_file_report)| {
                    metadata_file_report
                        .case_reports
                        .iter()
                        .map(move |(case_idx, case_report)| {
                            (metadata_file_path, case_idx, case_report)
                        })
                })
                .flat_map(|(metadata_file_path, case_idx, case_report)| {
                    case_report.mode_execution_reports.iter().map(
                        move |(mode, execution_report)| {
                            (
                                metadata_file_path,
                                case_idx,
                                mode,
                                execution_report.status.as_ref(),
                            )
                        },
                    )
                })
                .filter_map(|(metadata_file_path, case_idx, mode, status)| {
                    status.map(|status| (metadata_file_path, case_idx, mode, status))
                })
                .map(|(metadata_file_path, case_idx, mode, status)| {
                    (
                        TestSpecifier {
                            metadata_file_path: Cow::Borrowed(
                                remove_prefix
                                    .iter()
                                    .filter_map(|prefix| {
                                        metadata_file_path.as_inner().strip_prefix(prefix).ok()
                                    })
                                    .next()
                                    .unwrap_or(metadata_file_path.as_inner()),
                            ),
                            case_idx: case_idx.into_inner(),
                            mode: Cow::Borrowed(mode),
                        },
                        to_report_status(status),
                    )
                })
                .filter(|(_, status)| {
                    include_status
                        .as_ref()
                        .map(|allowed_status| allowed_status.contains(status))
                        .unwrap_or(true)
                })
                .collect::<Expectations>();

            let output_file = OpenOptions::new()
                .truncate(true)
                .create(true)
                .write(true)
                .open(output_path)
                .context("Failed to create the output file")?;
            serde_json::to_writer_pretty(output_file, &expectations)
                .context("Failed to write the expectations to file")?;
        }
        ReportAction::CompareExpectationFiles {
            base_expectation_path,
            other_expectation_path,
        } => {
            let base: Expectations<'static> = {
                let file = File::open(&base_expectation_path)
                    .context("Failed to open the base expectation file")?;
                serde_json::from_reader(&file)
                    .context("Failed to deserialize the base expectations")?
            };
            let other: Expectations<'static> = {
                let file = File::open(&other_expectation_path)
                    .context("Failed to open the other expectation file")?;
                serde_json::from_reader(&file)
                    .context("Failed to deserialize the other expectations")?
            };

            let keys = base.keys().chain(other.keys()).collect::<BTreeSet<_>>();

            for key in keys {
                let base_status = base.get(key).context(format!(
                    "Entry not found in the base expectations: \"{}\"",
                    key
                ))?;
                let other_status = other.get(key).context(format!(
                    "Entry not found in the other expectations: \"{}\"",
                    key
                ))?;

                if base_status != other_status {
                    bail!(
                        "Expectations for entry \"{}\" have changed. They were {:?} and now they are {:?}",
                        key,
                        base_status,
                        other_status
                    )
                }
            }
        }
    };

    Ok(())
}

fn to_report_status(value: &TestCaseStatus) -> ReportStatus {
    match value {
        TestCaseStatus::Succeeded { .. } => ReportStatus::Succeeded,
        TestCaseStatus::Failed { .. } => ReportStatus::Failed,
        TestCaseStatus::Ignored { .. } => ReportStatus::Ignored,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TestSpecifier<'a> {
    metadata_file_path: Cow<'a, Path>,
    case_idx: usize,
    mode: Cow<'a, Mode>,
}

impl<'a> Display for TestSpecifier<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}::{}::{}",
            self.metadata_file_path.display(),
            self.case_idx,
            self.mode
        )
    }
}

impl<'a> Serialize for TestSpecifier<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'d, 'a> Deserialize<'d> for TestSpecifier<'a> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'d>,
    {
        let string = String::deserialize(deserializer)?;
        let mut splitted = string.split("::");
        let (Some(metadata_file_path), Some(case_idx), Some(mode), None) = (
            splitted.next(),
            splitted.next(),
            splitted.next(),
            splitted.next(),
        ) else {
            return Err(serde::de::Error::custom(
                "Test specifier doesn't contain the components required",
            ));
        };
        let metadata_file_path = PathBuf::from(metadata_file_path);
        let case_idx = usize::from_str(case_idx)
            .map_err(|_| serde::de::Error::custom("Case idx is not a usize"))?;
        let mode = Mode::from_str(mode).map_err(|_| serde::de::Error::custom("Invalid mode"))?;

        Ok(Self {
            metadata_file_path: Cow::Owned(metadata_file_path),
            case_idx,
            mode: Cow::Owned(mode),
        })
    }
}
