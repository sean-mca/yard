use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use std::path::PathBuf;
use yard_structs::{JobState, LockInfo, StateBackend};

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

pub enum Storage {
    Local(LocalStorage),
    S3(S3Storage),
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

impl Storage {
    // --- Per-job state operations ---

    /// Read a single job's state file. Returns None if the file doesn't exist.
    pub async fn read_job(&self, job_name: &str) -> Result<Option<JobState>> {
        match self {
            Storage::Local(s) => {
                let path = s.path.join(format!("{job_name}.json"));
                if !path.exists() {
                    return Ok(None);
                }
                let content = tokio::fs::read_to_string(&path)
                    .await
                    .with_context(|| format!("Failed to read state for job {job_name}"))?;
                let state: JobState = serde_json::from_str(&content)?;
                Ok(Some(state))
            }
            Storage::S3(s) => {
                let key = format!("{}{job_name}.json", s.prefix);
                let result = s
                    .client
                    .get_object()
                    .bucket(&s.bucket)
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
            }
        }
    }

    /// Write a single job's state file.
    pub async fn write_job(&self, job_name: &str, state: &JobState) -> Result<()> {
        match self {
            Storage::Local(s) => {
                let json = serde_json::to_string_pretty(state)?;
                tokio::fs::create_dir_all(&s.path).await?;
                let path = s.path.join(format!("{job_name}.json"));
                tokio::fs::write(&path, json).await?;
                Ok(())
            }
            Storage::S3(s) => {
                let json = serde_json::to_string_pretty(state)?;
                let key = format!("{}{job_name}.json", s.prefix);
                s.client
                    .put_object()
                    .bucket(&s.bucket)
                    .key(&key)
                    .body(json.into_bytes().into())
                    .content_type("application/json")
                    .send()
                    .await?;
                Ok(())
            }
        }
    }

    /// Delete a single job's state file.
    pub async fn delete_job(&self, job_name: &str) -> Result<()> {
        match self {
            Storage::Local(s) => {
                let path = s.path.join(format!("{job_name}.json"));
                if path.exists() {
                    tokio::fs::remove_file(&path).await?;
                }
                Ok(())
            }
            Storage::S3(s) => {
                let key = format!("{}{job_name}.json", s.prefix);
                s.client
                    .delete_object()
                    .bucket(&s.bucket)
                    .key(&key)
                    .send()
                    .await?;
                Ok(())
            }
        }
    }

    /// List all job names that have state files.
    pub async fn list_jobs(&self) -> Result<Vec<String>> {
        match self {
            Storage::Local(s) => {
                let mut jobs = Vec::new();
                if !s.path.exists() {
                    return Ok(jobs);
                }
                let mut entries = tokio::fs::read_dir(&s.path).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if let Some(job_name) = name.strip_suffix(".json")
                        && !job_name.ends_with(".lock")
                    {
                        jobs.push(job_name.to_string());
                    }
                }
                Ok(jobs)
            }
            Storage::S3(s) => {
                let mut jobs = Vec::new();
                let resp = s
                    .client
                    .list_objects_v2()
                    .bucket(&s.bucket)
                    .prefix(&s.prefix)
                    .send()
                    .await?;

                for obj in resp.contents() {
                    if let Some(key) = obj.key() {
                        let relative = key.strip_prefix(&s.prefix).unwrap_or(key);
                        if let Some(job_name) = relative.strip_suffix(".json")
                            && !job_name.ends_with(".lock")
                            && !job_name.contains('/')
                        {
                            jobs.push(job_name.to_string());
                        }
                    }
                }
                Ok(jobs)
            }
        }
    }

    // --- Locking ---

    /// Acquire a lock for a job. Returns Ok(LockInfo) on success,
    /// Err if already locked.
    pub async fn lock(&self, job_name: &str) -> Result<LockInfo> {
        let info = lock_info();
        let json = serde_json::to_string_pretty(&info)?;

        match self {
            Storage::Local(s) => {
                tokio::fs::create_dir_all(&s.path).await?;
                let lock_path = s.path.join(format!("{job_name}.json.lock"));

                // O_CREAT | O_EXCL — fails if file already exists
                match tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&lock_path)
                    .await
                {
                    Ok(_file) => {
                        tokio::fs::write(&lock_path, &json).await?;
                        Ok(info)
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        let existing = self.get_lock(job_name).await?;
                        match existing {
                            Some(held) => Err(anyhow!(
                                "Job \"{job_name}\" is locked by {} (since {}). \
                                 Use `yard force-unlock` to override.",
                                held.who,
                                held.created_at
                            )),
                            None => Err(anyhow!("Job \"{job_name}\" is locked (unknown holder)")),
                        }
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Storage::S3(s) => {
                let key = format!("{}{job_name}.json.lock", s.prefix);
                let result = s
                    .client
                    .put_object()
                    .bucket(&s.bucket)
                    .key(&key)
                    .body(json.into_bytes().into())
                    .content_type("application/json")
                    .if_none_match("*")
                    .send()
                    .await;

                match result {
                    Ok(_) => Ok(info),
                    Err(e) => {
                        // Object already exists — someone holds the lock
                        let existing = self.get_lock(job_name).await.ok().flatten();
                        match existing {
                            Some(held) => Err(anyhow!(
                                "Job \"{job_name}\" is locked by {} (since {}). \
                                 Use `yard force-unlock` to override.",
                                held.who,
                                held.created_at
                            )),
                            None => Err(e.into()),
                        }
                    }
                }
            }
        }
    }

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

    /// Remove the lock regardless of who holds it.
    pub async fn force_unlock(&self, job_name: &str) -> Result<()> {
        match self {
            Storage::Local(s) => {
                let lock_path = s.path.join(format!("{job_name}.json.lock"));
                if lock_path.exists() {
                    tokio::fs::remove_file(&lock_path).await?;
                }
                Ok(())
            }
            Storage::S3(s) => {
                let key = format!("{}{job_name}.json.lock", s.prefix);
                s.client
                    .delete_object()
                    .bucket(&s.bucket)
                    .key(&key)
                    .send()
                    .await?;
                Ok(())
            }
        }
    }

    /// Read current lock info for a job, if any.
    pub async fn get_lock(&self, job_name: &str) -> Result<Option<LockInfo>> {
        match self {
            Storage::Local(s) => {
                let lock_path = s.path.join(format!("{job_name}.json.lock"));
                if !lock_path.exists() {
                    return Ok(None);
                }
                let content = tokio::fs::read_to_string(&lock_path).await?;
                let info: LockInfo = serde_json::from_str(&content)?;
                Ok(Some(info))
            }
            Storage::S3(s) => {
                let key = format!("{}{job_name}.json.lock", s.prefix);
                let result = s
                    .client
                    .get_object()
                    .bucket(&s.bucket)
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
            }
        }
    }
}

// --- Factory ---

pub async fn get_storage(backend: &StateBackend) -> Result<Storage> {
    match backend {
        StateBackend::Local { path } => Ok(Storage::Local(LocalStorage { path: path.clone() })),
        StateBackend::S3 {
            bucket,
            key,
            region,
        } => {
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(aws_config::Region::new(region.clone()))
                .load()
                .await;
            let client = Client::new(&config);

            // Ensure prefix ends with `/` so job files are nested under it
            let prefix = if key.ends_with('/') {
                key.clone()
            } else {
                format!("{key}/")
            };

            Ok(Storage::S3(S3Storage {
                client,
                bucket: bucket.clone(),
                prefix,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yard_structs::Deployment;

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

    fn temp_storage(name: &str) -> Storage {
        let dir = std::env::temp_dir().join(format!("yard_test_{}_{}", name, std::process::id()));
        Storage::Local(LocalStorage { path: dir })
    }

    fn storage_path(storage: &Storage) -> &PathBuf {
        match storage {
            Storage::Local(s) => &s.path,
            _ => panic!("expected local storage"),
        }
    }

    // --- Per-job read/write ---

    #[tokio::test]
    async fn write_and_read_job() {
        let storage = temp_storage("rw");
        let state = test_job_state("my_job");

        storage.write_job("my_job", &state).await.unwrap();
        let loaded = storage.read_job("my_job").await.unwrap();

        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.job_name, "my_job");
        assert_eq!(loaded.deployment.config_hash, "abc123");

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn read_nonexistent_job_returns_none() {
        let storage = temp_storage("noexist");
        std::fs::create_dir_all(storage_path(&storage)).unwrap();

        let loaded = storage.read_job("nope").await.unwrap();
        assert!(loaded.is_none());

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn delete_job_removes_file() {
        let storage = temp_storage("del");
        let state = test_job_state("doomed");

        storage.write_job("doomed", &state).await.unwrap();
        assert!(storage.read_job("doomed").await.unwrap().is_some());

        storage.delete_job("doomed").await.unwrap();
        assert!(storage.read_job("doomed").await.unwrap().is_none());

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn list_jobs_finds_all() {
        let storage = temp_storage("list");
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

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn list_jobs_empty_dir() {
        let storage = temp_storage("empty");
        let jobs = storage.list_jobs().await.unwrap();
        assert!(jobs.is_empty());
    }

    // --- Locking ---

    #[tokio::test]
    async fn lock_and_unlock() {
        let storage = temp_storage("lock");
        let info = storage.lock("my_job").await.unwrap();
        assert!(!info.who.is_empty());

        // Verify lock file exists
        let lock = storage.get_lock("my_job").await.unwrap();
        assert!(lock.is_some());

        // Unlock
        storage.unlock("my_job", &info).await.unwrap();
        let lock = storage.get_lock("my_job").await.unwrap();
        assert!(lock.is_none());

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn double_lock_fails() {
        let storage = temp_storage("dbl");
        let _info = storage.lock("my_job").await.unwrap();

        let result = storage.lock("my_job").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("is locked"));

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn force_unlock_removes_lock() {
        let storage = temp_storage("force");
        let _info = storage.lock("my_job").await.unwrap();

        storage.force_unlock("my_job").await.unwrap();
        let lock = storage.get_lock("my_job").await.unwrap();
        assert!(lock.is_none());

        // Can lock again after force unlock
        let _info2 = storage.lock("my_job").await.unwrap();

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn lock_files_excluded_from_list() {
        let storage = temp_storage("lockexcl");
        storage
            .write_job("real_job", &test_job_state("real_job"))
            .await
            .unwrap();
        let _lock = storage.lock("real_job").await.unwrap();

        let jobs = storage.list_jobs().await.unwrap();
        assert_eq!(jobs, vec!["real_job"]);

        let _ = std::fs::remove_dir_all(storage_path(&storage));
    }

    #[tokio::test]
    async fn get_storage_local() {
        let backend = StateBackend::Local {
            path: "/tmp/test_state".into(),
        };
        let storage = get_storage(&backend).await.unwrap();
        assert!(matches!(storage, Storage::Local(_)));
    }
}
