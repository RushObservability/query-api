use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::export::{ExportFormat, ExportJobState};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct JobRecord {
    pub id: String,
    pub tenant_id: String,
    pub signal: String,
    pub format: ExportFormat,
    pub filename: String,
    pub requested_rows: u64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub state: ExportJobState,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub chunks: u64,
    pub truncated: bool,
    pub error: Option<String>,
    pub owner: String,
    pub lease_until: DateTime<Utc>,
    pub revision: u64,
}

#[derive(Clone)]
pub(super) struct Version {
    revision: u64,
    object: Option<UpdateVersion>,
}

#[derive(Clone)]
pub(super) struct JobStore {
    pub objects: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    local_root: Option<PathBuf>,
}

impl JobStore {
    pub fn from_env(root: &Path) -> anyhow::Result<Self> {
        let bucket = std::env::var("RUSH_EXPORT_S3_BUCKET")
            .or_else(|_| std::env::var("RUSH_BUFFER_S3_BUCKET"));
        let Ok(bucket) = bucket else {
            return Self::local(root);
        };
        let shares_ingest_bucket =
            std::env::var("RUSH_BUFFER_S3_BUCKET").ok().as_ref() == Some(&bucket);
        let setting = |suffix: &str| {
            std::env::var(format!("RUSH_EXPORT_S3_{suffix}"))
                .or_else(|_| std::env::var(format!("RUSH_BUFFER_S3_{suffix}")))
                .ok()
        };
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(
                setting("REGION")
                    .or_else(|| std::env::var("AWS_REGION").ok())
                    .unwrap_or_else(|| "us-east-1".into()),
            )
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_virtual_hosted_style_request(false);
        if let Some(endpoint) = setting("ENDPOINT") {
            builder = builder
                .with_allow_http(endpoint.starts_with("http://"))
                .with_endpoint(endpoint);
        }
        if let Some(key) = setting("ACCESS_KEY") {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = setting("SECRET_KEY") {
            builder = builder.with_secret_access_key(secret);
        }
        let prefix = std::env::var("RUSH_EXPORT_S3_PREFIX").unwrap_or_else(|_| "exports/".into());
        if shares_ingest_bucket {
            let ingest =
                std::env::var("RUSH_BUFFER_S3_PREFIX").unwrap_or_else(|_| "ingest/".into());
            validate_separate_prefixes(&prefix, &ingest)?;
        }
        Self::shared(Arc::new(builder.build()?), &prefix)
    }

    pub fn local(root: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        let root = root.canonicalize()?;
        Ok(Self {
            objects: Arc::new(LocalFileSystem::new_with_prefix(&root)?.with_fsync(true)),
            prefix: ObjectPath::from("exports"),
            local_root: Some(root),
        })
    }

    pub fn shared(objects: Arc<dyn ObjectStore>, prefix: &str) -> anyhow::Result<Self> {
        Ok(Self {
            objects,
            prefix: ObjectPath::parse(prefix.trim_matches('/'))?,
            local_root: None,
        })
    }

    fn namespace(&self, tenant: &str, id: &str) -> anyhow::Result<ObjectPath> {
        let id = uuid::Uuid::parse_str(id)?.to_string();
        let tenant = hex::encode(Sha256::digest(tenant.as_bytes()));
        Ok(self.prefix.clone().join(tenant).join(id))
    }

    fn metadata_key(&self, tenant: &str, id: &str) -> anyhow::Result<ObjectPath> {
        Ok(self.namespace(tenant, id)?.join("job.json"))
    }

    pub fn artifact_key(&self, record: &JobRecord) -> anyhow::Result<ObjectPath> {
        let owner = uuid::Uuid::parse_str(&record.owner)?.to_string();
        Ok(self
            .namespace(&record.tenant_id, &record.id)?
            .join(format!("artifact-{owner}")))
    }

    pub fn keys(&self) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.objects.list(Some(&self.prefix))
    }

    pub async fn load(
        &self,
        tenant: &str,
        id: &str,
    ) -> anyhow::Result<Option<(JobRecord, Version)>> {
        if uuid::Uuid::parse_str(id).is_err() {
            return Ok(None);
        }
        let key = self.metadata_key(tenant, id)?;
        let result = match self.objects.get(&key).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if result.meta.size > 64 * 1024 {
            anyhow::bail!("export metadata exceeds size limit");
        }
        let object = Some(UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        });
        let record: JobRecord = serde_json::from_slice(&result.bytes().await?)?;
        if record.tenant_id != tenant || record.id != uuid::Uuid::parse_str(id)?.to_string() {
            anyhow::bail!("export metadata scope mismatch");
        }
        let version = Version {
            revision: record.revision,
            object,
        };
        Ok(Some((record, version)))
    }

    pub async fn save(
        &self,
        record: &JobRecord,
        expected: Option<Version>,
    ) -> anyhow::Result<bool> {
        let key = self.metadata_key(&record.tenant_id, &record.id)?;
        if let Some(root) = &self.local_root {
            let root = root.clone();
            let record = record.clone();
            return tokio::task::spawn_blocking(move || local_cas(&root, &key, &record, expected))
                .await?;
        }
        let mode = match expected {
            Some(version) => PutMode::Update(
                version
                    .object
                    .ok_or_else(|| anyhow::anyhow!("missing export object version"))?,
            ),
            None => PutMode::Create,
        };
        match self
            .objects
            .put_opts(
                &key,
                Bytes::from(serde_json::to_vec(record)?).into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn delete_metadata(&self, record: &JobRecord) -> anyhow::Result<()> {
        match self
            .objects
            .delete(&self.metadata_key(&record.tenant_id, &record.id)?)
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub async fn delete_artifacts(&self, record: &JobRecord) -> anyhow::Result<()> {
        let prefix = self.artifact_key(record)?;
        let mut keys = self.objects.list(Some(&prefix));
        while let Some(meta) = keys.next().await {
            match self.objects.delete(&meta?.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub async fn record_at(&self, key: &ObjectPath) -> anyhow::Result<JobRecord> {
        let result = self.objects.get(key).await?;
        if result.meta.size > 64 * 1024 {
            anyhow::bail!("export metadata exceeds size limit");
        }
        let record: JobRecord = serde_json::from_slice(&result.bytes().await?)?;
        if self.metadata_key(&record.tenant_id, &record.id)? != *key {
            anyhow::bail!("export metadata scope mismatch");
        }
        Ok(record)
    }
}

fn validate_separate_prefixes(export: &str, ingest: &str) -> anyhow::Result<()> {
    let export = ObjectPath::parse(export.trim_matches('/'))?;
    let ingest = ObjectPath::parse(if ingest.is_empty() {
        "ingest"
    } else {
        ingest.trim_matches('/')
    })?;
    let overlaps = |a: &ObjectPath, b: &ObjectPath| {
        a.as_ref().is_empty() || a == b || b.as_ref().starts_with(&format!("{}/", a.as_ref()))
    };
    if overlaps(&export, &ingest) || overlaps(&ingest, &export) {
        anyhow::bail!("export and ingest S3 prefixes must not overlap");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn export_objects_cannot_be_listed_by_the_ingest_drainer() {
        assert!(validate_separate_prefixes("exports/", "ingest/").is_ok());
        assert!(validate_separate_prefixes("a/export", "a/ingest").is_ok());
        assert!(validate_separate_prefixes("exports", "").is_ok());
        for (export, ingest) in [
            ("ingest/exports", "ingest"),
            ("exports", "exports/ingest"),
            ("same", "same"),
            ("", "ingest"),
            ("exports", "/"),
        ] {
            assert!(validate_separate_prefixes(export, ingest).is_err());
        }
    }
}

fn local_cas(
    root: &Path,
    key: &ObjectPath,
    record: &JobRecord,
    expected: Option<Version>,
) -> anyhow::Result<bool> {
    use std::os::unix::{fs::OpenOptionsExt, io::AsRawFd};
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join(".export-jobs.lock"))?;
    // SAFETY: lock owns a live descriptor; closing it releases the advisory lock.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let path = root.join(key.as_ref());
    let current = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    {
        Ok(file) => Some(serde_json::from_reader::<_, JobRecord>(
            file.take(64 * 1024),
        )?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    match (current, expected) {
        (None, None) => {}
        (Some(current), Some(version)) if current.revision == version.revision => {}
        _ => return Ok(false),
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("missing export metadata directory"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(record)?)?;
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    let mut directory = parent;
    loop {
        fs::File::open(directory)?.sync_all()?;
        if directory == root {
            break;
        }
        directory = directory
            .parent()
            .ok_or_else(|| anyhow::anyhow!("invalid export metadata root"))?;
    }
    Ok(true)
}
