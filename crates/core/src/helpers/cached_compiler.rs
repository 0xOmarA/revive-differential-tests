//! A wrapper around the compiler which allows for caching of compilation artifacts so that they can
//! be reused between runs.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use futures::FutureExt;
use revive_dt_common::{iterators::FilesWithExtensionIterator, types::CompilerIdentifier};
use revive_dt_compiler::{Compiler, CompilerOutput, Mode, SolidityCompiler};
use revive_dt_core::Platform;
use revive_dt_format::metadata::{ContractIdent, ContractInstance, Metadata};

use alloy::{hex::ToHexExt, json_abi::JsonAbi, primitives::Address};
use anyhow::{Context as _, Error, Result};
use revive_dt_report::ExecutionSpecificReporter;
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tracing::{Instrument, debug, debug_span, instrument};

pub struct CachedCompiler {
    /// The cache that stores the compiled contracts.
    artifacts_cache: ArtifactsCache,

    /// This is a mechanism that the cached compiler uses so that if multiple compilation requests
    /// come in for the same contract we never compile all of them and only compile it once and all
    /// other tasks that request this same compilation concurrently get the cached version.
    cache_key_lock: RwLock<HashMap<CacheKey, Arc<Mutex<()>>>>,
}

impl CachedCompiler {
    pub async fn new(path: impl AsRef<Path>, invalidate_cache: bool) -> Result<Self> {
        let mut cache = ArtifactsCache::new(path);
        if invalidate_cache {
            cache = cache
                .with_invalidated_cache()
                .await
                .context("Failed to invalidate compilation cache directory")?;
        }
        Ok(Self {
            artifacts_cache: cache,
            cache_key_lock: Default::default(),
        })
    }

    /// Compiles or gets the compilation artifacts from the cache.
    #[allow(clippy::too_many_arguments)]
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            metadata_file_path = %metadata_file_path.display(),
            %mode,
            platform = %platform.platform_identifier()
        ),
        err
    )]
    pub async fn compile_contracts(
        &self,
        metadata: &Metadata,
        metadata_file_path: &Path,
        mode: &Mode,
        deployed_libraries: Option<&HashMap<ContractInstance, (ContractIdent, Address, JsonAbi)>>,
        compiler: Arc<dyn SolidityCompiler>,
        platform: &dyn Platform,
        reporter: &ExecutionSpecificReporter,
    ) -> Result<CompilerOutput> {
        let (resolc_heap_size, resolc_stack_size) = compiler.resolc_pvm_settings();
        let cache_key = CacheKey {
            compiler_identifier: platform.compiler_identifier(),
            compiler_version: compiler.version().clone(),
            metadata_file_path: metadata_file_path.to_path_buf(),
            solc_mode: mode.clone(),
            resolc_heap_size,
            resolc_stack_size,
        };

        // Pre-compute all data from borrowed parameters so the compilation callback only
        // captures owned types. This is required so that the futures produced here are Send,
        // enabling use with tokio::spawn.
        let metadata_directory = metadata
            .directory()
            .context("Failed to get metadata directory while preparing compilation")?
            .to_path_buf();
        let files_to_compile: Vec<PathBuf> = metadata
            .files_to_compile()
            .context("Failed to enumerate files to compile from metadata")?
            .collect();
        let mode_owned = mode.clone();
        let deployed_libraries_owned = deployed_libraries.cloned();
        let compiler_version = compiler.version().clone();
        let compiler_path = compiler.path().to_path_buf();
        let reporter_owned = reporter.clone();

        let compilation_callback = |compiler: Arc<dyn SolidityCompiler>| {
            let metadata_directory = metadata_directory.clone();
            let files_to_compile = files_to_compile.clone();
            let mode_owned = mode_owned.clone();
            let deployed_libraries_owned = deployed_libraries_owned.clone();
            let reporter_owned = reporter_owned.clone();
            let compiler_version = compiler_version.clone();
            let compiler_path = compiler_path.clone();
            async move {
                compile_contracts(
                    metadata_directory,
                    files_to_compile.into_iter(),
                    &mode_owned,
                    deployed_libraries_owned.as_ref(),
                    compiler.as_ref(),
                    compiler_version,
                    compiler_path,
                    &reporter_owned,
                )
                .map(|compilation_result| compilation_result.map(CacheValue::new))
                .await
            }
            .instrument(debug_span!(
                "Running compilation for the cache key",
                cache_key.compiler_identifier = %cache_key.compiler_identifier,
                cache_key.compiler_version = %cache_key.compiler_version,
                cache_key.metadata_file_path = %cache_key.metadata_file_path.display(),
                cache_key.solc_mode = %cache_key.solc_mode,
            ))
        };

        // We need Arc<dyn SolidityCompiler> for the callback. Since we only have a reference,
        // we'll create a thin wrapper. But actually, we can just use the trait object reference
        // before the await boundaries. For now, we restructure so the callback captures owned data.
        // The `compiler` reference is needed for `try_build` inside `compile_contracts`.
        // Since SolidityCompiler is behind Arc in TestPlatformInformation, the caller can pass it.
        // For backward compat, we accept &dyn and note this limitation.

        let compiled_contracts = match deployed_libraries {
            // If deployed libraries have been specified then we will re-compile the contract as it
            // means that linking is required in this case.
            Some(_) => {
                debug!("Deployed libraries defined, recompilation must take place");
                debug!("Cache miss");
                compilation_callback(compiler)
                    .await
                    .context("Compilation callback for deployed libraries failed")?
                    .compiler_output
            }
            // If no deployed libraries are specified then we can follow the cached flow and attempt
            // to lookup the compilation artifacts in the cache.
            None => {
                debug!("Deployed libraries undefined, attempting to make use of cache");

                // Lock this specific cache key such that we do not get inconsistent state. We want
                // that when multiple cases come in asking for the compilation artifacts then they
                // don't all trigger a compilation if there's a cache miss. Hence, the lock here.
                let read_guard = self.cache_key_lock.read().await;
                let mutex = match read_guard.get(&cache_key).cloned() {
                    Some(value) => {
                        drop(read_guard);
                        value
                    }
                    None => {
                        drop(read_guard);
                        self.cache_key_lock
                            .write()
                            .await
                            .entry(cache_key.clone())
                            .or_default()
                            .clone()
                    }
                };
                let _guard = mutex.lock().await;

                match self.artifacts_cache.get(&cache_key).await {
                    Some(cache_value) => {
                        if deployed_libraries.is_some() {
                            reporter
                                .report_post_link_contracts_compilation_succeeded_event(
                                    compiler_version.clone(),
                                    &compiler_path,
                                    true,
                                    None,
                                    cache_value.compiler_output.clone(),
                                )
                                .unwrap_or_else(|e| tracing::warn!("Reporter send failed: {e:?}"));
                        } else {
                            reporter
                                .report_pre_link_contracts_compilation_succeeded_event(
                                    compiler_version.clone(),
                                    &compiler_path,
                                    true,
                                    None,
                                    cache_value.compiler_output.clone(),
                                )
                                .unwrap_or_else(|e| tracing::warn!("Reporter send failed: {e:?}"));
                        }
                        cache_value.compiler_output
                    }
                    None => {
                        let compiler_output = compilation_callback(compiler)
                            .await
                            .context("Compilation callback failed (cache miss path)")?
                            .compiler_output;
                        self.artifacts_cache
                            .insert(
                                &cache_key,
                                &CacheValue {
                                    compiler_output: compiler_output.clone(),
                                },
                            )
                            .await
                            .context(
                                "Failed to write the cached value of the compilation artifacts",
                            )?;
                        compiler_output
                    }
                }
            }
        };

        Ok(compiled_contracts)
    }
}

#[allow(clippy::too_many_arguments)]
async fn compile_contracts(
    metadata_directory: impl AsRef<Path>,
    mut files_to_compile: impl Iterator<Item = PathBuf>,
    mode: &Mode,
    deployed_libraries: Option<&HashMap<ContractInstance, (ContractIdent, Address, JsonAbi)>>,
    compiler: &dyn SolidityCompiler,
    compiler_version: Version,
    compiler_path: PathBuf,
    reporter: &ExecutionSpecificReporter,
) -> Result<CompilerOutput> {
    // Puts a limit on how many compilations we can perform at any given instance which helps us
    // with some of the errors we've been seeing with high concurrency on MacOS (we have not tried
    // it on Linux so we don't know if these issues also persist there or not.)
    const MAX_CONCURRENT_COMPILATIONS: usize = 5;
    static SPAWN_GATE: LazyLock<Semaphore> =
        LazyLock::new(|| Semaphore::new(MAX_CONCURRENT_COMPILATIONS));
    let _permit = SPAWN_GATE.acquire().await?;

    let all_sources_in_dir = FilesWithExtensionIterator::new(metadata_directory.as_ref())
        .with_allowed_extension("sol")
        .with_use_cached_fs(true)
        .collect::<Vec<_>>();

    let compilation = Compiler::new()
        .with_allow_path(metadata_directory)
        // Handling the modes
        .with_optimization(mode.optimize_setting)
        .with_pipeline(mode.pipeline)
        // Adding the contract sources to the compiler.
        .try_then(|compiler| {
            files_to_compile.try_fold(compiler, |compiler, path| compiler.with_source(path))
        })?
        // Adding the deployed libraries to the compiler.
        .then(|compiler| {
            deployed_libraries
                .iter()
                .flat_map(|value| value.iter())
                .map(|(instance, (ident, address, abi))| (instance, ident, address, abi))
                .flat_map(|(_, ident, address, _)| {
                    all_sources_in_dir
                        .iter()
                        .map(move |path| (ident, address, path))
                })
                .fold(compiler, |compiler, (ident, address, path)| {
                    compiler.with_library(path, ident.as_str(), *address)
                })
        });

    let input = compilation.input().clone();
    let output = compilation.try_build(compiler).await;

    match (output.as_ref(), deployed_libraries.is_some()) {
        (Ok(output), true) => {
            reporter
                .report_post_link_contracts_compilation_succeeded_event(
                    compiler_version.clone(),
                    &compiler_path,
                    false,
                    input,
                    output.clone(),
                )
                .unwrap_or_else(|e| tracing::warn!("Reporter send failed: {e:?}"));
        }
        (Ok(output), false) => {
            reporter
                .report_pre_link_contracts_compilation_succeeded_event(
                    compiler_version.clone(),
                    &compiler_path,
                    false,
                    input,
                    output.clone(),
                )
                .unwrap_or_else(|e| tracing::warn!("Reporter send failed: {e:?}"));
        }
        (Err(err), true) => {
            reporter
                .report_post_link_contracts_compilation_failed_event(
                    compiler_version.clone(),
                    compiler_path.clone(),
                    input,
                    format!("{err:#}"),
                )
                .unwrap_or_else(|e| tracing::warn!("Reporter send failed: {e:?}"));
        }
        (Err(err), false) => {
            reporter
                .report_pre_link_contracts_compilation_failed_event(
                    compiler_version,
                    compiler_path,
                    input,
                    format!("{err:#}"),
                )
                .unwrap_or_else(|e| tracing::warn!("Reporter send failed: {e:?}"));
        }
    }

    output
}

struct ArtifactsCache {
    path: PathBuf,
}

impl ArtifactsCache {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    #[instrument(level = "debug", skip_all, err)]
    pub async fn with_invalidated_cache(self) -> Result<Self> {
        cacache::clear(self.path.as_path())
            .await
            .map_err(Into::<Error>::into)
            .with_context(|| format!("Failed to clear cache at {}", self.path.display()))?;
        Ok(self)
    }

    #[instrument(level = "debug", skip_all, err)]
    pub async fn insert(&self, key: &CacheKey, value: &CacheValue) -> Result<()> {
        let key = serde_json::to_vec(key).context("Failed to serialize cache key (json)")?;
        let value = serde_json::to_vec(value).context("Failed to serialize cache value (json)")?;
        cacache::write(self.path.as_path(), key.encode_hex(), value)
            .await
            .with_context(|| {
                format!("Failed to write cache entry under {}", self.path.display())
            })?;
        Ok(())
    }

    pub async fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        let key = serde_json::to_vec(key).ok()?;
        let value = cacache::read(self.path.as_path(), key.encode_hex())
            .await
            .ok()?;
        let value = serde_json::from_slice::<CacheValue>(&value).ok()?;
        Some(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
struct CacheKey {
    /// The identifier of the used compiler.
    compiler_identifier: CompilerIdentifier,

    /// The version of the compiler that was used to compile the artifacts.
    compiler_version: Version,

    /// The path of the metadata file that the compilation artifacts are for.
    metadata_file_path: PathBuf,

    /// The mode that the compilation artifacts where compiled with.
    solc_mode: Mode,

    /// The resolc PVM heap size setting, if applicable.
    resolc_heap_size: Option<u32>,

    /// The resolc PVM stack size setting, if applicable.
    resolc_stack_size: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CacheValue {
    /// The compiler output from the compilation run.
    compiler_output: CompilerOutput,
}

impl CacheValue {
    pub fn new(compiler_output: CompilerOutput) -> Self {
        Self { compiler_output }
    }
}
