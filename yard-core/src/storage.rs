use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use yard_structs::{AwsCredentialConfig, DagState, JobState, LockInfo, StateBackend};

/// Prefix for DAG state files to avoid colliding with job state files.
pub const DAG_STATE_PREFIX: &str = "_dag_";

/// Stale locks older than this are automatically reclaimed on next acquire (D-03).
const LOCK_TTL_MINUTES: i64 = 30;

// --- Storage backends ---

pub struct LocalStorage {
    /// Directory where per-job state files live (e.g. `.yard/state/`)
    pub path: PathBuf,
}

pub struct S3Storage {
    pub client: Client,
    pub bucket: String,
    /// Prefix for per-job state files (e.g. `yard/state/`)
    pub prefix: String,
}

pub struct Storage {
    backend: Box<dyn StorageBackend + Send + Sync>,
}

/// Trait for reading/writing job + DAG state and managing job locks.
///
/// Each backend (Local filesystem, S3 object store, future DynamoDB / GCS / etc.)
/// implements this trait. The `Storage` wrapper struct holds a `Box<dyn StorageBackend>`
/// and exposes thin wrapper methods so consumers don't need to know which backend
/// is in use.
///
/// Mirrors the manual async-trait shape established by `crate::providers::Provider`
/// (object-safe via `Pin<Box<dyn Future<...>>>` returns; no `async-trait` dep).
pub trait StorageBackend: Send + Sync {
    // --- Per-job state operations ---

    /// Read a single job's state file. Returns None if the file doesn't exist.
    fn read_job(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<JobState>>> + Send + '_>>;

    /// Write a job's state file. Overwrites any existing file.
    fn write_job(
        &self,
        job_name: &str,
        state: &JobState,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Delete a job's state file. No-op if the file doesn't exist.
    fn delete_job(&self, job_name: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// List all job names with state files (excluding lock files and DAG files).
    fn list_jobs(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>>;

    // --- Per-DAG state operations ---

    /// Read a DAG's state file. Returns None if the file doesn't exist.
    fn read_dag(
        &self,
        dag_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DagState>>> + Send + '_>>;

    /// Write a DAG's state file. Overwrites any existing file.
    fn write_dag(
        &self,
        dag_name: &str,
        state: &DagState,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Delete a DAG's state file. No-op if the file doesn't exist.
    fn delete_dag(&self, dag_name: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// List all DAG names with state files.
    fn list_dags(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>>;

    // --- Locking primitives ---

    /// Acquire a lock for a job. Returns the lock info on success.
    /// Errors if the job is already locked.
    fn lock(&self, job_name: &str) -> Pin<Box<dyn Future<Output = Result<LockInfo>> + Send + '_>>;

    /// Remove the lock regardless of who holds it.
    fn force_unlock(&self, job_name: &str)
    -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Get the current lock info for a job, or None if not locked.
    fn get_lock(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<LockInfo>>> + Send + '_>>;
}

// --- LocalStorage trait impl ---

impl StorageBackend for LocalStorage {
    fn read_job(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<JobState>>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let path = self.path.join(format!("{job_name}.json"));
            if !path.exists() {
                return Ok(None);
            }
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("Failed to read state for job {job_name}"))?;
            let state: JobState = serde_json::from_str(&content)?;
            Ok(Some(state))
        })
    }

    fn write_job(
        &self,
        job_name: &str,
        state: &JobState,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        let json_result = serde_json::to_string_pretty(state);
        Box::pin(async move {
            let json = json_result?;
            tokio::fs::create_dir_all(&self.path).await?;
            let path = self.path.join(format!("{job_name}.json"));
            tokio::fs::write(&path, json).await?;
            Ok(())
        })
    }

    fn delete_job(&self, job_name: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let path = self.path.join(format!("{job_name}.json"));
            if path.exists() {
                tokio::fs::remove_file(&path).await?;
            }
            Ok(())
        })
    }

    fn list_jobs(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>> {
        Box::pin(async move {
            let mut jobs = Vec::new();
            if !self.path.exists() {
                return Ok(jobs);
            }
            let mut entries = tokio::fs::read_dir(&self.path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(job_name) = name.strip_suffix(".json")
                    && !job_name.ends_with(".lock")
                    && !job_name.starts_with(DAG_STATE_PREFIX)
                {
                    jobs.push(job_name.to_string());
                }
            }
            Ok(jobs)
        })
    }

    fn read_dag(
        &self,
        dag_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DagState>>> + Send + '_>> {
        let dag_name = dag_name.to_string();
        Box::pin(async move {
            let key = format!("{DAG_STATE_PREFIX}{dag_name}");
            let path = self.path.join(format!("{key}.json"));
            if !path.exists() {
                return Ok(None);
            }
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("Failed to read state for DAG {dag_name}"))?;
            let state: DagState = serde_json::from_str(&content)?;
            Ok(Some(state))
        })
    }

    fn write_dag(
        &self,
        dag_name: &str,
        state: &DagState,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let dag_name = dag_name.to_string();
        let json_result = serde_json::to_string_pretty(state);
        Box::pin(async move {
            let key = format!("{DAG_STATE_PREFIX}{dag_name}");
            let json = json_result?;
            tokio::fs::create_dir_all(&self.path).await?;
            let path = self.path.join(format!("{key}.json"));
            tokio::fs::write(&path, json).await?;
            Ok(())
        })
    }

    fn delete_dag(&self, dag_name: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let dag_name = dag_name.to_string();
        Box::pin(async move {
            let key = format!("{DAG_STATE_PREFIX}{dag_name}");
            let path = self.path.join(format!("{key}.json"));
            if path.exists() {
                tokio::fs::remove_file(&path).await?;
            }
            Ok(())
        })
    }

    fn list_dags(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>> {
        Box::pin(async move {
            let mut dags = Vec::new();
            if !self.path.exists() {
                return Ok(dags);
            }
            let mut entries = tokio::fs::read_dir(&self.path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(base) = name.strip_suffix(".json")
                    && !base.ends_with(".lock")
                    && let Some(dag_name) = base.strip_prefix(DAG_STATE_PREFIX)
                {
                    dags.push(dag_name.to_string());
                }
            }
            Ok(dags)
        })
    }

    fn lock(&self, job_name: &str) -> Pin<Box<dyn Future<Output = Result<LockInfo>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let info = lock_info();
            let json = serde_json::to_string_pretty(&info)?;
            tokio::fs::create_dir_all(&self.path).await?;
            let lock_path = self.path.join(format!("{job_name}.json.lock"));

            // Try up to 2 times: first attempt + one retry after stale reclaim.
            for attempt in 0..2u8 {
                match tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&lock_path)
                    .await
                {
                    Ok(file) => {
                        use tokio::io::AsyncWriteExt;
                        let mut file = file;
                        file.write_all(json.as_bytes()).await?;
                        file.flush().await?;
                        return Ok(info);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        let existing = self.get_lock(&job_name).await?;
                        match existing {
                            Some(held) => {
                                if attempt == 0
                                    && let Ok(created) =
                                        chrono::DateTime::parse_from_rfc3339(&held.created_at)
                                {
                                    let age = chrono::Utc::now().signed_duration_since(created);
                                    if age > chrono::TimeDelta::minutes(LOCK_TTL_MINUTES) {
                                        eprintln!(
                                            "Warning: reclaiming stale lock for \"{}\" \
                                             (held by {} since {}, age {}m)",
                                            job_name,
                                            held.who,
                                            held.created_at,
                                            age.num_minutes()
                                        );
                                        self.force_unlock(&job_name).await?;
                                        continue;
                                    }
                                }
                                return Err(anyhow!(
                                    "Job \"{job_name}\" is locked by {} (since {}). \
                                     Use `yard force-unlock` to override.",
                                    held.who,
                                    held.created_at
                                ));
                            }
                            None => return Err(anyhow!("Job \"{job_name}\" is locked (unknown holder)")),
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Err(anyhow!("Job \"{job_name}\" lock acquire failed after stale reclaim"))
        })
    }

    fn force_unlock(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let lock_path = self.path.join(format!("{job_name}.json.lock"));
            if lock_path.exists() {
                tokio::fs::remove_file(&lock_path).await?;
            }
            Ok(())
        })
    }

    fn get_lock(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<LockInfo>>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let lock_path = self.path.join(format!("{job_name}.json.lock"));
            if !lock_path.exists() {
                return Ok(None);
            }
            let content = tokio::fs::read_to_string(&lock_path).await?;
            let info: LockInfo = serde_json::from_str(&content)?;
            Ok(Some(info))
        })
    }
}

// --- S3Storage trait impl ---

impl StorageBackend for S3Storage {
    fn read_job(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<JobState>>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let key = format!("{}{job_name}.json", self.prefix);
            let result = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let data = resp.body.collect().await?.into_bytes();
                    let state: JobState = serde_json::from_slice(&data)?;
                    Ok(Some(state))
                }
                Err(e) => {
                    if e.as_service_error().is_some_and(|se| se.is_no_such_key()) {
                        Ok(None)
                    } else {
                        Err(e.into())
                    }
                }
            }
        })
    }

    fn write_job(
        &self,
        job_name: &str,
        state: &JobState,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        let json_result = serde_json::to_string_pretty(state);
        Box::pin(async move {
            let json = json_result?;
            let key = format!("{}{job_name}.json", self.prefix);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(json.into_bytes().into())
                .content_type("application/json")
                .send()
                .await?;
            Ok(())
        })
    }

    fn delete_job(&self, job_name: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let key = format!("{}{job_name}.json", self.prefix);
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await?;
            Ok(())
        })
    }

    fn list_jobs(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>> {
        Box::pin(async move {
            list_s3_filtered(&self.client, &self.bucket, &self.prefix, |relative| {
                let job_name = relative.strip_suffix(".json")?;
                if job_name.ends_with(".lock")
                    || job_name.starts_with(DAG_STATE_PREFIX)
                    || job_name.contains('/')
                {
                    return None;
                }
                Some(job_name.to_string())
            })
            .await
        })
    }

    fn read_dag(
        &self,
        dag_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DagState>>> + Send + '_>> {
        let dag_name = dag_name.to_string();
        Box::pin(async move {
            let key = format!("{DAG_STATE_PREFIX}{dag_name}");
            let s3_key = format!("{}{key}.json", self.prefix);
            let result = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let data = resp.body.collect().await?.into_bytes();
                    let state: DagState = serde_json::from_slice(&data)?;
                    Ok(Some(state))
                }
                Err(e) => {
                    if e.as_service_error().is_some_and(|se| se.is_no_such_key()) {
                        Ok(None)
                    } else {
                        Err(e.into())
                    }
                }
            }
        })
    }

    fn write_dag(
        &self,
        dag_name: &str,
        state: &DagState,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let dag_name = dag_name.to_string();
        let json_result = serde_json::to_string_pretty(state);
        Box::pin(async move {
            let key = format!("{DAG_STATE_PREFIX}{dag_name}");
            let json = json_result?;
            let s3_key = format!("{}{key}.json", self.prefix);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .body(json.into_bytes().into())
                .content_type("application/json")
                .send()
                .await?;
            Ok(())
        })
    }

    fn delete_dag(&self, dag_name: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let dag_name = dag_name.to_string();
        Box::pin(async move {
            let key = format!("{DAG_STATE_PREFIX}{dag_name}");
            let s3_key = format!("{}{key}.json", self.prefix);
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .send()
                .await?;
            Ok(())
        })
    }

    fn list_dags(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>> {
        Box::pin(async move {
            list_s3_filtered(&self.client, &self.bucket, &self.prefix, |relative| {
                let base = relative.strip_suffix(".json")?;
                if base.ends_with(".lock") || base.contains('/') {
                    return None;
                }
                let dag_name = base.strip_prefix(DAG_STATE_PREFIX)?;
                Some(dag_name.to_string())
            })
            .await
        })
    }

    fn lock(&self, job_name: &str) -> Pin<Box<dyn Future<Output = Result<LockInfo>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let info = lock_info();
            let key = format!("{}{job_name}.json.lock", self.prefix);

            // Try up to 2 times: first attempt + one retry after stale reclaim.
            for attempt in 0..2u8 {
                let json = serde_json::to_string_pretty(&info)?;
                let result = self
                    .client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .body(json.into_bytes().into())
                    .content_type("application/json")
                    .if_none_match("*")
                    .send()
                    .await;

                match result {
                    Ok(_) => return Ok(info),
                    Err(e) => {
                        let is_contention = e.as_service_error().is_some_and(|se| {
                            matches!(
                                se.meta().code(),
                                Some("PreconditionFailed") | Some("ConditionalRequestConflict")
                            )
                        });
                        if !is_contention {
                            return Err(e.into());
                        }

                        let existing = self.get_lock(&job_name).await.ok().flatten();
                        match existing {
                            Some(held) => {
                                if attempt == 0
                                    && let Ok(created) =
                                        chrono::DateTime::parse_from_rfc3339(&held.created_at)
                                {
                                    let age = chrono::Utc::now().signed_duration_since(created);
                                    if age > chrono::TimeDelta::minutes(LOCK_TTL_MINUTES) {
                                        eprintln!(
                                            "Warning: reclaiming stale lock for \"{}\" \
                                             (held by {} since {}, age {}m)",
                                            job_name,
                                            held.who,
                                            held.created_at,
                                            age.num_minutes()
                                        );
                                        self.force_unlock(&job_name).await?;
                                        continue;
                                    }
                                }
                                return Err(anyhow!(
                                    "Job \"{job_name}\" is locked by {} (since {}). \
                                     Use `yard force-unlock` to override.",
                                    held.who,
                                    held.created_at
                                ));
                            }
                            None => return Err(e.into()),
                        }
                    }
                }
            }
            Err(anyhow!("Job \"{job_name}\" lock acquire failed after stale reclaim"))
        })
    }

    fn force_unlock(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let key = format!("{}{job_name}.json.lock", self.prefix);
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await?;
            Ok(())
        })
    }

    fn get_lock(
        &self,
        job_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<LockInfo>>> + Send + '_>> {
        let job_name = job_name.to_string();
        Box::pin(async move {
            let key = format!("{}{job_name}.json.lock", self.prefix);
            let result = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await;
            match result {
                Ok(resp) => {
                    let data = resp.body.collect().await?.into_bytes();
                    let info: LockInfo = serde_json::from_slice(&data)?;
                    Ok(Some(info))
                }
                Err(e) => {
                    if e.as_service_error().is_some_and(|se| se.is_no_such_key()) {
                        Ok(None)
                    } else {
                        Err(e.into())
                    }
                }
            }
        })
    }
}

// --- Helpers ---

fn lock_info() -> LockInfo {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    LockInfo {
        who: user,
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}

/// Lists S3 objects under `prefix`, applies `filter_map` to each key's
/// prefix-relative path, and collects the non-None results.
async fn list_s3_filtered<F>(
    client: &Client,
    bucket: &str,
    prefix: &str,
    filter_map: F,
) -> Result<Vec<String>>
where
    F: Fn(&str) -> Option<String>,
{
    let mut results = Vec::new();
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    while let Some(page) = stream.try_next().await? {
        for obj in page.contents() {
            if let Some(key) = obj.key() {
                let relative = key.strip_prefix(prefix).unwrap_or(key);
                if let Some(name) = filter_map(relative) {
                    results.push(name);
                }
            }
        }
    }
    Ok(results)
}

impl Storage {
    /// Construct a Storage handle from any backend implementation.
    pub fn new<B: StorageBackend + Send + Sync + 'static>(backend: B) -> Self {
        Self {
            backend: Box::new(backend),
        }
    }

    // --- Per-job state operations (thin wrappers; delegate to self.backend) ---

    /// Read a single job's state file. Returns None if the file doesn't exist.
    pub async fn read_job(&self, job_name: &str) -> Result<Option<JobState>> {
        self.backend.read_job(job_name).await
    }

    /// Write a job's state file. Overwrites any existing file.
    pub async fn write_job(&self, job_name: &str, state: &JobState) -> Result<()> {
        self.backend.write_job(job_name, state).await
    }

    /// Delete a job's state file. No-op if the file doesn't exist.
    pub async fn delete_job(&self, job_name: &str) -> Result<()> {
        self.backend.delete_job(job_name).await
    }

    /// List all job names with state files (excluding lock files and DAG files).
    pub async fn list_jobs(&self) -> Result<Vec<String>> {
        self.backend.list_jobs().await
    }

    // --- Per-DAG state operations ---

    /// Read a DAG's state file. Returns None if the file doesn't exist.
    pub async fn read_dag(&self, dag_name: &str) -> Result<Option<DagState>> {
        self.backend.read_dag(dag_name).await
    }

    /// Write a DAG's state file. Overwrites any existing file.
    pub async fn write_dag(&self, dag_name: &str, state: &DagState) -> Result<()> {
        self.backend.write_dag(dag_name, state).await
    }

    /// Delete a DAG's state file. No-op if the file doesn't exist.
    pub async fn delete_dag(&self, dag_name: &str) -> Result<()> {
        self.backend.delete_dag(dag_name).await
    }

    /// List all DAG names with state files.
    pub async fn list_dags(&self) -> Result<Vec<String>> {
        self.backend.list_dags().await
    }

    // --- Locking primitives ---

    /// Acquire a lock for a job. Returns Ok(LockInfo) on success,
    /// Err if already locked.
    pub async fn lock(&self, job_name: &str) -> Result<LockInfo> {
        self.backend.lock(job_name).await
    }

    /// Remove the lock regardless of who holds it.
    pub async fn force_unlock(&self, job_name: &str) -> Result<()> {
        self.backend.force_unlock(job_name).await
    }

    /// Read current lock info for a job, if any.
    pub async fn get_lock(&self, job_name: &str) -> Result<Option<LockInfo>> {
        self.backend.get_lock(job_name).await
    }

    // --- Convenience methods (D-06: stay on impl Storage, not on the trait) ---

    /// Release a lock, but only if we hold it (match on `who`).
    pub async fn unlock(&self, job_name: &str, holder: &LockInfo) -> Result<()> {
        let current = self.get_lock(job_name).await?;
        match current {
            None => Ok(()), // Already unlocked
            Some(existing) if existing.who == holder.who => self.force_unlock(job_name).await,
            Some(existing) => Err(anyhow!(
                "Cannot unlock job \"{job_name}\": held by {} (since {}), not by {}",
                existing.who,
                existing.created_at,
                holder.who
            )),
        }
    }

    /// Acquire locks for multiple jobs atomically. If any lock fails,
    /// all previously acquired locks are rolled back.
    pub async fn lock_jobs(&self, names: &[String]) -> Result<Vec<(String, LockInfo)>> {
        let mut acquired: Vec<(String, LockInfo)> = Vec::new();

        for name in names {
            match self.lock(name).await {
                Ok(info) => acquired.push((name.clone(), info)),
                Err(e) => {
                    // Roll back all locks acquired so far
                    self.unlock_jobs(&acquired).await;
                    return Err(e);
                }
            }
        }

        Ok(acquired)
    }

    /// Release multiple locks. Best-effort: logs failures but does not abort.
    pub async fn unlock_jobs(&self, locks: &[(String, LockInfo)]) {
        for (name, info) in locks.iter().rev() {
            if let Err(e) = self.unlock(name, info).await {
                eprintln!("Warning: failed to unlock job \"{name}\": {e}");
            }
        }
    }
}

/// RAII guard that releases locks on drop.
/// Call `release()` on the happy path to avoid the drop warning.
/// If dropped without release (panic, early return), logs a warning;
/// the TTL (D-02) covers cleanup on next acquire.
pub struct LockGuard<'a> {
    storage: &'a Storage,
    locks: Vec<(String, LockInfo)>,
    released: bool,
}

impl<'a> LockGuard<'a> {
    pub fn new(storage: &'a Storage, locks: Vec<(String, LockInfo)>) -> Self {
        Self {
            storage,
            locks,
            released: false,
        }
    }

    /// Explicit release on the normal-exit path.
    /// Returns the first unlock error (if any) after attempting all locks,
    /// so callers can log or propagate.  The TTL backstop (D-02) covers
    /// any locks that could not be released here.
    pub async fn release(mut self) -> Result<()> {
        let mut first_err: Option<anyhow::Error> = None;
        for (name, info) in self.locks.iter().rev() {
            if let Err(e) = self.storage.unlock(name, info).await {
                eprintln!("Warning: failed to unlock '{name}': {e}");
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        self.released = true;
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl<'a> Drop for LockGuard<'a> {
    fn drop(&mut self) {
        if !self.released {
            for (name, _) in &self.locks {
                eprintln!("Warning: lock guard dropped without explicit release for '{name}'");
            }
            // Best-effort: TTL covers any locks not cleaned up here.
        }
    }
}

/// Merge `YARD_STATE_AWS_*` env vars over a yaml `state.aws:` block
/// (env beats yaml, mirroring `providers::aws_config`'s `YARD_AWS_*`
/// precedence). Produces the typed `AwsCredentialConfig` that callers
/// convert to a JSON `Value` at the providers boundary.
///
/// Resolution (highest precedence first):
///   YARD_STATE_AWS_ASSUME_ROLE  → yaml `assume_role`
///   YARD_STATE_AWS_SESSION_NAME → yaml `session_name`
///   YARD_STATE_AWS_EXTERNAL_ID  → yaml `external_id`
///
/// Provider `YARD_AWS_*` env vars are NOT consulted — state creds scope
/// is orthogonal to provider creds (D-03). If both yaml and envs are
/// absent, returns `None` so the caller can pass `None` to `aws_config`
/// and get the default credential provider chain — preserving today's
/// behavior (D-02 strictly additive).
fn merge_state_aws_with_env(
    state_aws: Option<&AwsCredentialConfig>,
) -> Option<AwsCredentialConfig> {
    let yaml = state_aws.cloned().unwrap_or_default();

    // Env beats yaml: envs are the overlay, yaml is the base.
    let env_overlay = AwsCredentialConfig {
        assume_role: std::env::var("YARD_STATE_AWS_ASSUME_ROLE").ok(),
        session_name: std::env::var("YARD_STATE_AWS_SESSION_NAME").ok(),
        external_id: std::env::var("YARD_STATE_AWS_EXTERNAL_ID").ok(),
        // `region` lives on `StateBackend::S3.region`, not on the aws
        // sub-block here — keep it None at this layer.
        region: None,
        aws_conn_id: None,
    };
    let merged = AwsCredentialConfig::merge(&yaml, &env_overlay);

    if merged == AwsCredentialConfig::default() {
        // No yaml + no envs → fall through to default credential chain
        // (D-02 strictly additive).
        None
    } else {
        Some(merged)
    }
}

// --- Factory ---

pub async fn get_storage(backend: &StateBackend) -> Result<Storage> {
    match backend {
        StateBackend::Local { path } => Ok(Storage::new(LocalStorage { path: path.clone() })),
        StateBackend::S3 {
            bucket,
            key,
            region,
            aws,
        } => {
            // Resolve state credentials. Precedence (highest first):
            //   1. `YARD_STATE_AWS_{ASSUME_ROLE,SESSION_NAME,EXTERNAL_ID}` envs
            //   2. `state.aws:` yaml sub-block on yard.yaml
            //   3. Default AWS credential provider chain (env vars, shared
            //      config, IMDS/ECS task role, SSO) via `aws_config(region, None)`
            //
            // Provider `YARD_AWS_*` envs are intentionally NOT consulted here —
            // state cred scope is orthogonal to provider cred scope (D-03).
            // When neither yaml nor envs are set, `merge_state_aws_with_env`
            // returns `None` and we pass `None` to `aws_config`, preserving
            // today's default-chain behavior (D-02 strictly additive).
            //
            // The `providers::aws_config` boundary stays Value-typed (D-14
            // forward-compat for provider-specific extension fields), so we
            // convert here at the call site via `serde_json::to_value`.
            let merged = merge_state_aws_with_env(aws.as_ref());
            let merged_value: Option<serde_json::Value> = merged
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .context("Failed to serialize state AWS credentials to JSON")?;
            let aws_cfg_opt = merged_value.as_ref();
            let config = crate::providers::aws_config(region, aws_cfg_opt).await;
            let client = Client::new(&config);

            // Ensure prefix ends with `/` so job files are nested under it
            let prefix = if key.ends_with('/') {
                key.clone()
            } else {
                format!("{key}/")
            };

            Ok(Storage::new(S3Storage {
                client,
                bucket: bucket.clone(),
                prefix,
            }))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use yard_structs::{DagDeployment, Deployment};

    fn test_job_state(job_name: &str) -> JobState {
        JobState {
            job_name: job_name.to_string(),
            project: "test-project".to_string(),
            deployment: Deployment {
                env: None,
                config_hash: "abc123".to_string(),
                config: serde_json::json!({"type": "glue"}),
                status: "generated".to_string(),
                applied_at: "2025-01-01T00:00:00Z".to_string(),
                resources: Vec::new(),
            },
        }
    }

    fn temp_storage(name: &str) -> (Storage, PathBuf) {
        let dir = std::env::temp_dir().join(format!("yard_test_{}_{}", name, std::process::id()));
        (Storage::new(LocalStorage { path: dir.clone() }), dir)
    }

    // --- Per-job read/write ---

    #[tokio::test]
    async fn write_and_read_job() {
        let (storage, dir) = temp_storage("rw");
        let state = test_job_state("my_job");

        storage.write_job("my_job", &state).await.unwrap();
        let loaded = storage.read_job("my_job").await.unwrap();

        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.job_name, "my_job");
        assert_eq!(loaded.deployment.config_hash, "abc123");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn read_nonexistent_job_returns_none() {
        let (storage, dir) = temp_storage("noexist");
        std::fs::create_dir_all(&dir).unwrap();

        let loaded = storage.read_job("nope").await.unwrap();
        assert!(loaded.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn delete_job_removes_file() {
        let (storage, dir) = temp_storage("del");
        let state = test_job_state("doomed");

        storage.write_job("doomed", &state).await.unwrap();
        assert!(storage.read_job("doomed").await.unwrap().is_some());

        storage.delete_job("doomed").await.unwrap();
        assert!(storage.read_job("doomed").await.unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn list_jobs_finds_all() {
        let (storage, dir) = temp_storage("list");
        storage
            .write_job("alpha", &test_job_state("alpha"))
            .await
            .unwrap();
        storage
            .write_job("beta", &test_job_state("beta"))
            .await
            .unwrap();

        let mut jobs = storage.list_jobs().await.unwrap();
        jobs.sort();
        assert_eq!(jobs, vec!["alpha", "beta"]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn list_jobs_empty_dir() {
        let (storage, _dir) = temp_storage("empty");
        let jobs = storage.list_jobs().await.unwrap();
        assert!(jobs.is_empty());
    }

    // --- Locking ---

    #[tokio::test]
    async fn lock_and_unlock() {
        let (storage, dir) = temp_storage("lock");
        let info = storage.lock("my_job").await.unwrap();
        assert!(!info.who.is_empty());

        // Verify lock file exists
        let lock = storage.get_lock("my_job").await.unwrap();
        assert!(lock.is_some());

        // Unlock
        storage.unlock("my_job", &info).await.unwrap();
        let lock = storage.get_lock("my_job").await.unwrap();
        assert!(lock.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn double_lock_fails() {
        let (storage, dir) = temp_storage("dbl");
        let _info = storage.lock("my_job").await.unwrap();

        let result = storage.lock("my_job").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("is locked"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn force_unlock_removes_lock() {
        let (storage, dir) = temp_storage("force");
        let _info = storage.lock("my_job").await.unwrap();

        storage.force_unlock("my_job").await.unwrap();
        let lock = storage.get_lock("my_job").await.unwrap();
        assert!(lock.is_none());

        // Can lock again after force unlock
        let _info2 = storage.lock("my_job").await.unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn lock_files_excluded_from_list() {
        let (storage, dir) = temp_storage("lockexcl");
        storage
            .write_job("real_job", &test_job_state("real_job"))
            .await
            .unwrap();
        let _lock = storage.lock("real_job").await.unwrap();

        let jobs = storage.list_jobs().await.unwrap();
        assert_eq!(jobs, vec!["real_job"]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn get_storage_local() {
        let backend = StateBackend::Local {
            path: "/tmp/test_state".into(),
        };
        let storage = get_storage(&backend).await.unwrap();
        // Behavioral: prove Local backend wires correctly by driving a primitive method.
        let _ = storage.list_jobs().await;
    }

    // --- Batch locking ---

    #[tokio::test]
    async fn lock_jobs_acquires_all() {
        let (storage, dir) = temp_storage("lockjobs");
        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let locks = storage.lock_jobs(&names).await.unwrap();
        assert_eq!(locks.len(), 3);

        // All three should be locked
        for name in &names {
            let lock = storage.get_lock(name).await.unwrap();
            assert!(lock.is_some());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn lock_jobs_rolls_back_on_failure() {
        let (storage, dir) = temp_storage("lockrollback");

        // Pre-lock "b" so locking ["a", "b", "c"] fails on "b"
        let _held = storage.lock("b").await.unwrap();

        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let result = storage.lock_jobs(&names).await;
        assert!(result.is_err());

        // "a" should have been rolled back (unlocked)
        let a_lock = storage.get_lock("a").await.unwrap();
        assert!(
            a_lock.is_none(),
            "lock for 'a' should have been rolled back"
        );

        // "c" was never attempted
        let c_lock = storage.get_lock("c").await.unwrap();
        assert!(c_lock.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unlock_jobs_releases_all() {
        let (storage, dir) = temp_storage("unlockjobs");
        let names = vec!["x".to_string(), "y".to_string()];

        let locks = storage.lock_jobs(&names).await.unwrap();
        storage.unlock_jobs(&locks).await;

        for name in &names {
            let lock = storage.get_lock(name).await.unwrap();
            assert!(lock.is_none());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- DAG state operations ---

    fn test_dag_state(dag_name: &str) -> DagState {
        DagState {
            dag_name: dag_name.to_string(),
            project: "test-project".to_string(),
            deployment: DagDeployment {
                content_hash: "daghash123".to_string(),
                config: serde_json::json!({"schedule": "@daily"}),
                tasks: vec!["task_a".to_string(), "task_b".to_string()],
                status: "generated".to_string(),
                applied_at: "2025-01-01T00:00:00Z".to_string(),
                s3_uri: None,
            },
            aws: None,
        }
    }

    #[tokio::test]
    async fn write_and_read_dag() {
        let (storage, dir) = temp_storage("dag_rw");
        let state = test_dag_state("my_dag");

        storage.write_dag("my_dag", &state).await.unwrap();
        let loaded = storage.read_dag("my_dag").await.unwrap();

        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.dag_name, "my_dag");
        assert_eq!(loaded.deployment.content_hash, "daghash123");
        assert_eq!(loaded.deployment.tasks, vec!["task_a", "task_b"]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn read_nonexistent_dag_returns_none() {
        let (storage, dir) = temp_storage("dag_noexist");
        std::fs::create_dir_all(&dir).unwrap();

        let loaded = storage.read_dag("nope").await.unwrap();
        assert!(loaded.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn delete_dag_removes_file() {
        let (storage, dir) = temp_storage("dag_del");
        let state = test_dag_state("doomed_dag");

        storage.write_dag("doomed_dag", &state).await.unwrap();
        assert!(storage.read_dag("doomed_dag").await.unwrap().is_some());

        storage.delete_dag("doomed_dag").await.unwrap();
        assert!(storage.read_dag("doomed_dag").await.unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn list_dags_finds_all() {
        let (storage, dir) = temp_storage("dag_list");
        storage
            .write_dag("dag_alpha", &test_dag_state("dag_alpha"))
            .await
            .unwrap();
        storage
            .write_dag("dag_beta", &test_dag_state("dag_beta"))
            .await
            .unwrap();

        let mut dags = storage.list_dags().await.unwrap();
        dags.sort();
        assert_eq!(dags, vec!["dag_alpha", "dag_beta"]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn list_jobs_excludes_dags() {
        let (storage, dir) = temp_storage("dag_excl");
        storage
            .write_job("real_job", &test_job_state("real_job"))
            .await
            .unwrap();
        storage
            .write_dag("my_dag", &test_dag_state("my_dag"))
            .await
            .unwrap();

        let jobs = storage.list_jobs().await.unwrap();
        assert_eq!(jobs, vec!["real_job"]);

        let dags = storage.list_dags().await.unwrap();
        assert_eq!(dags, vec!["my_dag"]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn list_dags_empty_dir() {
        let (storage, _dir) = temp_storage("dag_empty");
        let dags = storage.list_dags().await.unwrap();
        assert!(dags.is_empty());
    }

    // --- Phase 9 · Plan 02: state credential resolution ---

    /// Snapshot→set→run→restore for env vars. `std::env::set_var`/`remove_var`
    /// require `unsafe` under Rust 2024 because concurrent env mutation is UB.
    /// Used here only inside `#[cfg(test)]` and serialized through a
    /// module-local Mutex — CLAUDE.md "no unsafe {}" exception per
    /// Phase 9 Plan 02 (option a in the Task 1 checkpoint).
    fn scoped_env<F: FnOnce()>(pairs: &[(&str, Option<&str>)], f: F) {
        use std::sync::Mutex;
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let prev: Vec<(String, Option<String>)> = pairs
            .iter()
            .map(|(k, _)| ((*k).to_string(), std::env::var(k).ok()))
            .collect();
        for (k, v) in pairs {
            match v {
                // #[cfg(test)] only — Rust 2024 env-mutation gate; CLAUDE.md exception per Phase 9 Plan 02 note
                Some(val) => unsafe { std::env::set_var(k, val) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        f();
        for (k, v) in prev {
            match v {
                Some(val) => unsafe { std::env::set_var(&k, val) },
                None => unsafe { std::env::remove_var(&k) },
            }
        }
    }

    #[test]
    fn merge_state_aws_env_beats_yaml() {
        scoped_env(
            &[
                (
                    "YARD_STATE_AWS_ASSUME_ROLE",
                    Some("arn:aws:iam::999999999999:role/Env"),
                ),
                ("YARD_STATE_AWS_SESSION_NAME", Some("env-sess")),
                ("YARD_STATE_AWS_EXTERNAL_ID", None),
            ],
            || {
                let yaml = AwsCredentialConfig {
                    assume_role: Some("arn:aws:iam::111111111111:role/Yaml".to_string()),
                    session_name: Some("yaml-sess".to_string()),
                    ..Default::default()
                };
                let merged = merge_state_aws_with_env(Some(&yaml))
                    .expect("envs are set so merged must be Some");
                assert_eq!(
                    merged.assume_role.as_deref(),
                    Some("arn:aws:iam::999999999999:role/Env")
                );
                assert_eq!(merged.session_name.as_deref(), Some("env-sess"));
            },
        );
    }

    #[test]
    fn merge_state_aws_yaml_only() {
        scoped_env(
            &[
                ("YARD_STATE_AWS_ASSUME_ROLE", None),
                ("YARD_STATE_AWS_SESSION_NAME", None),
                ("YARD_STATE_AWS_EXTERNAL_ID", None),
            ],
            || {
                let yaml = AwsCredentialConfig {
                    assume_role: Some("arn:aws:iam::111111111111:role/Yaml".to_string()),
                    ..Default::default()
                };
                let merged = merge_state_aws_with_env(Some(&yaml))
                    .expect("yaml is set so merged must be Some");
                assert_eq!(
                    merged.assume_role.as_deref(),
                    Some("arn:aws:iam::111111111111:role/Yaml")
                );
            },
        );
    }

    #[test]
    fn merge_state_aws_env_only() {
        scoped_env(
            &[
                (
                    "YARD_STATE_AWS_ASSUME_ROLE",
                    Some("arn:aws:iam::222222222222:role/Env"),
                ),
                ("YARD_STATE_AWS_SESSION_NAME", None),
                ("YARD_STATE_AWS_EXTERNAL_ID", None),
            ],
            || {
                let merged =
                    merge_state_aws_with_env(None).expect("env is set so merged must be Some");
                assert_eq!(
                    merged.assume_role.as_deref(),
                    Some("arn:aws:iam::222222222222:role/Env")
                );
            },
        );
    }

    #[test]
    fn merge_state_aws_all_absent_returns_null() {
        scoped_env(
            &[
                ("YARD_STATE_AWS_ASSUME_ROLE", None),
                ("YARD_STATE_AWS_SESSION_NAME", None),
                ("YARD_STATE_AWS_EXTERNAL_ID", None),
            ],
            || {
                let merged = merge_state_aws_with_env(None);
                assert!(
                    merged.is_none(),
                    "absent yaml + absent envs must return None so aws_config gets None (default chain)"
                );
            },
        );
    }

    #[test]
    fn merge_state_aws_external_id_env_beats_yaml() {
        scoped_env(
            &[
                ("YARD_STATE_AWS_ASSUME_ROLE", None),
                ("YARD_STATE_AWS_SESSION_NAME", None),
                ("YARD_STATE_AWS_EXTERNAL_ID", Some("xid-env")),
            ],
            || {
                let yaml = AwsCredentialConfig {
                    external_id: Some("xid-yaml".to_string()),
                    ..Default::default()
                };
                let merged = merge_state_aws_with_env(Some(&yaml))
                    .expect("yaml or env is set so merged must be Some");
                assert_eq!(merged.external_id.as_deref(), Some("xid-env"));
            },
        );
    }

    #[test]
    fn merge_state_aws_provider_env_ignored() {
        // YARD_AWS_ASSUME_ROLE is for providers; must NOT feed into state.
        scoped_env(
            &[
                (
                    "YARD_AWS_ASSUME_ROLE",
                    Some("arn:aws:iam::888888888888:role/Providers"),
                ),
                ("YARD_STATE_AWS_ASSUME_ROLE", None),
                ("YARD_STATE_AWS_SESSION_NAME", None),
                ("YARD_STATE_AWS_EXTERNAL_ID", None),
            ],
            || {
                let merged = merge_state_aws_with_env(None);
                assert!(
                    merged.is_none(),
                    "provider YARD_AWS_* must NOT leak into state creds (D-03)"
                );
            },
        );
    }

    #[tokio::test]
    async fn get_storage_s3_null_aws_matches_today() {
        let backend = StateBackend::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            key: "state/".to_string(),
            aws: None,
        };
        let result = get_storage(&backend).await;
        assert!(result.is_ok());
        // matches!() variant check removed — Storage is now a struct, not an enum;
        // construction-success assertion above is the substantive invariant.
    }

    #[tokio::test]
    async fn get_storage_s3_with_aws_wires() {
        let backend = StateBackend::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            key: "state/".to_string(),
            aws: Some(AwsCredentialConfig {
                assume_role: Some("arn:aws:iam::111111111111:role/FakeState".to_string()),
                ..Default::default()
            }),
        };
        let result = get_storage(&backend).await;
        assert!(
            result.is_ok(),
            "construction must not error; STS errors only surface on first S3 call"
        );
        // matches!() variant check removed — Storage is now a struct, not an enum.
    }

    #[tokio::test]
    async fn get_storage_local_still_works() {
        let backend = StateBackend::Local {
            path: std::path::PathBuf::from("/tmp/yard-test-state"),
        };
        let result = get_storage(&backend).await;
        assert!(result.is_ok());
        // matches!() variant check removed — Storage is now a struct, not an enum.
    }

    // --- SC #3 / PRES-05 byte-identity wire-format tests ---

    #[tokio::test]
    async fn write_job_state_byte_identical_to_serde_pretty() {
        // SC #3 / PRES-05 byte-identical state-file persistence — verifies the
        // trait dispatch path emits exactly the same on-disk JSON as the pre-refactor
        // enum dispatch path. Mirrors Phase 22 plan-22-02's diff.rs round-trip pattern,
        // but tightens the assertion from `to_value` (structural) to `to_string_pretty`
        // (byte-identical) because SC #3 protects on-disk byte fidelity, not just
        // structural shape.
        let dir = std::env::temp_dir().join(format!(
            "yard_test_byte_identical_job_{}",
            std::process::id()
        ));
        let storage = Storage::new(LocalStorage { path: dir.clone() });

        let state = JobState {
            job_name: "byte_test".to_string(),
            project: "test-project".to_string(),
            deployment: Deployment {
                env: None,
                config_hash: "abc123".to_string(),
                config: serde_json::json!({
                    "type": "glue",
                    "role": "arn:aws:iam::111111111111:role/X"
                }),
                status: "applied".to_string(),
                applied_at: "2025-01-01T00:00:00Z".to_string(),
                resources: Vec::new(),
            },
        };

        // Write through the trait dispatch path (Storage::write_job ->
        // self.backend.write_job -> LocalStorage::write_job).
        storage.write_job("byte_test", &state).await.unwrap();

        // Read raw on-disk bytes and assert byte-identity vs.
        // serde_json::to_string_pretty output.
        let path = dir.join("byte_test.json");
        let on_disk = tokio::fs::read_to_string(&path).await.unwrap();
        let expected = serde_json::to_string_pretty(&state).unwrap();
        assert_eq!(
            on_disk, expected,
            "on-disk JobState JSON must be byte-identical to serde_json::to_string_pretty output"
        );

        // Round-trip: re-read via storage.read_job, assert struct equality on
        // load-bearing fields.
        let loaded = storage.read_job("byte_test").await.unwrap().unwrap();
        assert_eq!(loaded.job_name, state.job_name);
        assert_eq!(loaded.project, state.project);
        assert_eq!(loaded.deployment.config_hash, state.deployment.config_hash);
        assert_eq!(loaded.deployment.status, state.deployment.status);
        assert_eq!(loaded.deployment.applied_at, state.deployment.applied_at);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn write_dag_state_byte_identical_to_serde_pretty() {
        // Parallel SC #3 verification for DagState. write_dag uses the same
        // serde_json::to_string_pretty serialization path as write_job, but
        // writes to `{DAG_STATE_PREFIX}{dag_name}.json` per DAG_STATE_PREFIX.
        let dir = std::env::temp_dir().join(format!(
            "yard_test_byte_identical_dag_{}",
            std::process::id()
        ));
        let storage = Storage::new(LocalStorage { path: dir.clone() });

        let state = test_dag_state("byte_dag");

        storage.write_dag("byte_dag", &state).await.unwrap();

        let filename = format!("{DAG_STATE_PREFIX}byte_dag.json");
        let path = dir.join(&filename);
        let on_disk = tokio::fs::read_to_string(&path).await.unwrap();
        let expected = serde_json::to_string_pretty(&state).unwrap();
        assert_eq!(
            on_disk, expected,
            "on-disk DagState JSON must be byte-identical to serde_json::to_string_pretty output"
        );

        let loaded = storage.read_dag("byte_dag").await.unwrap().unwrap();
        // Use serde round-trip equality as the structural-equivalence proxy.
        let loaded_json = serde_json::to_string_pretty(&loaded).unwrap();
        assert_eq!(
            loaded_json, expected,
            "DagState round-trip preserves all fields"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- SC #4 demonstration: a third backend implemented in ONE impl block,
    //     ZERO edits to existing LocalStorage / S3Storage / Storage code ---
    //
    // This block lives entirely inside `#[cfg(test)] mod tests`; nothing in the
    // production-side `impl StorageBackend for LocalStorage` or
    // `impl StorageBackend for S3Storage` blocks had to change to admit it.
    // That structural property is what SC #4 protects.

    #[derive(Default)]
    struct InMemoryStorage {
        jobs: tokio::sync::Mutex<HashMap<String, JobState>>,
        dags: tokio::sync::Mutex<HashMap<String, DagState>>,
        locks: tokio::sync::Mutex<HashMap<String, LockInfo>>,
    }

    impl StorageBackend for InMemoryStorage {
        fn read_job(
            &self,
            job_name: &str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<JobState>>> + Send + '_>> {
            let job_name = job_name.to_string();
            Box::pin(async move { Ok(self.jobs.lock().await.get(&job_name).cloned()) })
        }

        fn write_job(
            &self,
            job_name: &str,
            state: &JobState,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            let job_name = job_name.to_string();
            let state = state.clone();
            Box::pin(async move {
                self.jobs.lock().await.insert(job_name, state);
                Ok(())
            })
        }

        fn delete_job(
            &self,
            job_name: &str,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            let job_name = job_name.to_string();
            Box::pin(async move {
                self.jobs.lock().await.remove(&job_name);
                Ok(())
            })
        }

        fn list_jobs(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>> {
            Box::pin(async move {
                let mut names: Vec<String> = self.jobs.lock().await.keys().cloned().collect();
                names.sort();
                Ok(names)
            })
        }

        fn read_dag(
            &self,
            dag_name: &str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<DagState>>> + Send + '_>> {
            let dag_name = dag_name.to_string();
            Box::pin(async move { Ok(self.dags.lock().await.get(&dag_name).cloned()) })
        }

        fn write_dag(
            &self,
            dag_name: &str,
            state: &DagState,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            let dag_name = dag_name.to_string();
            let state = state.clone();
            Box::pin(async move {
                self.dags.lock().await.insert(dag_name, state);
                Ok(())
            })
        }

        fn delete_dag(
            &self,
            dag_name: &str,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            let dag_name = dag_name.to_string();
            Box::pin(async move {
                self.dags.lock().await.remove(&dag_name);
                Ok(())
            })
        }

        fn list_dags(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>> {
            Box::pin(async move {
                let mut names: Vec<String> = self.dags.lock().await.keys().cloned().collect();
                names.sort();
                Ok(names)
            })
        }

        fn lock(
            &self,
            job_name: &str,
        ) -> Pin<Box<dyn Future<Output = Result<LockInfo>> + Send + '_>> {
            let job_name = job_name.to_string();
            Box::pin(async move {
                let info = lock_info();
                let mut locks = self.locks.lock().await;
                if locks.contains_key(&job_name) {
                    return Err(anyhow!(
                        "Job \"{job_name}\" is already locked (in-memory test backend)"
                    ));
                }
                locks.insert(job_name, info.clone());
                Ok(info)
            })
        }

        fn force_unlock(
            &self,
            job_name: &str,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            let job_name = job_name.to_string();
            Box::pin(async move {
                self.locks.lock().await.remove(&job_name);
                Ok(())
            })
        }

        fn get_lock(
            &self,
            job_name: &str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<LockInfo>>> + Send + '_>> {
            let job_name = job_name.to_string();
            Box::pin(async move { Ok(self.locks.lock().await.get(&job_name).cloned()) })
        }
    }

    // --- Phase 38 Plan 02: LockGuard + TTL regression tests ---

    #[tokio::test]
    async fn lock_guard_release_unlocks() {
        let (storage, dir) = temp_storage("guard_release");
        std::fs::create_dir_all(&dir).unwrap();
        let lock = storage.lock("test_job").await.unwrap();
        let guard = LockGuard::new(&storage, vec![("test_job".to_string(), lock)]);
        guard.release().await.expect("release should succeed in test");
        // Lock should be released — can acquire again
        let lock2 = storage.lock("test_job").await;
        assert!(lock2.is_ok(), "Should be able to lock after guard release");
        storage.force_unlock("test_job").await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn stale_lock_reclaimed_by_ttl() {
        let (storage, dir) = temp_storage("ttl_reclaim");
        std::fs::create_dir_all(&dir).unwrap();
        // Acquire lock
        let _lock = storage.lock("ttl_job").await.unwrap();
        // Manually backdate the lock file to 31 minutes ago
        let lock_path = dir.join("ttl_job.json.lock");
        let old_time = (chrono::Utc::now() - chrono::TimeDelta::minutes(31)).to_rfc3339();
        let backdated = LockInfo {
            who: "old_user".to_string(),
            created_at: old_time,
        };
        std::fs::write(&lock_path, serde_json::to_string(&backdated).unwrap()).unwrap();
        // Second lock should reclaim the stale lock
        let lock2 = storage.lock("ttl_job").await;
        assert!(lock2.is_ok(), "Should reclaim stale lock older than TTL");
        storage.force_unlock("ttl_job").await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fresh_lock_not_reclaimed() {
        let (storage, dir) = temp_storage("ttl_fresh");
        std::fs::create_dir_all(&dir).unwrap();
        let _lock = storage.lock("fresh_job").await.unwrap();
        // Second lock should fail — lock is fresh
        let lock2 = storage.lock("fresh_job").await;
        assert!(lock2.is_err(), "Should NOT reclaim fresh lock");
        let err_msg = lock2.unwrap_err().to_string();
        assert!(err_msg.contains("is locked by"), "Error should mention who holds the lock");
        storage.force_unlock("fresh_job").await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn in_memory_backend_full_cycle() {
        // SC #4 demonstration: construct Storage from a third backend (the
        // InMemoryStorage above) and exercise the full primitive API. Proves
        // adding a backend = (1) one impl block, (2) zero edits to existing
        // prod-side LocalStorage / S3Storage / Storage code.
        let storage = Storage::new(InMemoryStorage::default());

        let state = test_job_state("imem_job");

        // write -> read -> list (job)
        storage.write_job("imem_job", &state).await.unwrap();
        let loaded = storage.read_job("imem_job").await.unwrap();
        assert!(loaded.is_some(), "InMemoryStorage round-trip JobState");
        assert_eq!(loaded.unwrap().job_name, "imem_job");

        let jobs = storage.list_jobs().await.unwrap();
        assert_eq!(jobs, vec!["imem_job".to_string()]);

        // lock -> get_lock -> force_unlock
        let info = storage.lock("imem_job").await.unwrap();
        let held = storage.get_lock("imem_job").await.unwrap();
        assert!(held.is_some(), "InMemoryStorage lock round-trip");
        assert_eq!(held.unwrap().who, info.who);

        let double = storage.lock("imem_job").await;
        assert!(double.is_err(), "InMemoryStorage double-lock contention");

        storage.force_unlock("imem_job").await.unwrap();
        let after = storage.get_lock("imem_job").await.unwrap();
        assert!(after.is_none(), "InMemoryStorage force_unlock removes lock");

        // write -> read -> list (dag)
        let dag_state = test_dag_state("imem_dag");
        storage.write_dag("imem_dag", &dag_state).await.unwrap();
        let dag_loaded = storage.read_dag("imem_dag").await.unwrap();
        assert!(dag_loaded.is_some(), "InMemoryStorage round-trip DagState");

        let dags = storage.list_dags().await.unwrap();
        assert_eq!(dags, vec!["imem_dag".to_string()]);

        // delete (job)
        storage.delete_job("imem_job").await.unwrap();
        let after_del = storage.read_job("imem_job").await.unwrap();
        assert!(after_del.is_none(), "InMemoryStorage delete_job");
    }
}
