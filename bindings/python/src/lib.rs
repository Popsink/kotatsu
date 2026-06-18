//! Python bindings for the Kotatsu reader.
//!
//! Exposes an async `Source` that reads Tansu's native S3 storage directly
//! (topics, messages, consumer groups, schemas) — no Kafka broker. Methods are
//! `async` and integrate with asyncio (FastAPI etc.) via `pyo3-async-runtimes`.

use kotatsu_core::{
    config::{S3Config, StorageProvider},
    pagination::Page,
    query::{self, MessageParams, QueryError},
    schema::{SchemaError, SchemaRegistry},
    storage::{StorageError, StorageSource},
};
use pyo3::{create_exception, exceptions::PyException, prelude::*};
use serde::Serialize;

create_exception!(kotatsu, KotatsuError, PyException, "Kotatsu read error.");

fn storage_err(e: StorageError) -> PyErr {
    KotatsuError::new_err(e.to_string())
}
fn schema_err(e: SchemaError) -> PyErr {
    KotatsuError::new_err(e.to_string())
}
fn query_err(e: QueryError) -> PyErr {
    KotatsuError::new_err(e.to_string())
}

/// Serializes any value into a Python object (dict/list/scalar).
fn to_py<T: Serialize>(value: &T) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| {
        pythonize::pythonize(py, value)
            .map(|b| b.unbind())
            .map_err(|e| KotatsuError::new_err(e.to_string()))
    })
}

/// A read-only source bound to one Tansu cluster in object storage.
#[pyclass]
struct Source {
    source: StorageSource,
    registry: Option<SchemaRegistry>,
}

#[pymethods]
impl Source {
    /// Build a source. `provider` is `"s3"` (default) or `"gcs"`.
    /// S3 credentials default to the ambient AWS chain when
    /// `access_key`/`secret_key` are omitted. GCS credentials are read from
    /// `GOOGLE_SERVICE_ACCOUNT` / `GOOGLE_APPLICATION_CREDENTIALS`, or picked
    /// up automatically via Workload Identity on GKE.
    #[new]
    #[pyo3(signature = (
        bucket,
        cluster,
        provider="s3",
        endpoint=None,
        region=None,
        access_key=None,
        secret_key=None,
        session_token=None,
        force_path_style=true,
        kora_url=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        cluster: String,
        provider: &str,
        endpoint: Option<String>,
        region: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        session_token: Option<String>,
        force_path_style: bool,
        kora_url: Option<String>,
    ) -> PyResult<Self> {
        let storage_provider = match provider.to_lowercase().as_str() {
            "gcs" => StorageProvider::Gcs,
            "s3" => StorageProvider::S3,
            other => {
                return Err(KotatsuError::new_err(format!(
                    "unknown provider {other:?}: expected \"s3\" or \"gcs\""
                )))
            }
        };
        let allow_http = endpoint
            .as_deref()
            .map(|e| e.starts_with("http://"))
            .unwrap_or(false);
        let cfg = S3Config {
            provider: storage_provider,
            bucket,
            cluster,
            endpoint,
            region: region.unwrap_or_else(|| "us-east-1".to_string()),
            access_key,
            secret_key,
            session_token,
            force_path_style,
            allow_http,
        };
        let source = StorageSource::from_config(&cfg)
            .map_err(|e| KotatsuError::new_err(format!("building storage source: {e}")))?;
        let registry = kora_url.map(SchemaRegistry::new);
        Ok(Self { source, registry })
    }

    /// List cluster names present in the bucket.
    fn clusters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source.list_clusters().await.map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// Summary of the configured cluster's `meta.json`.
    fn cluster<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source.cluster_summary().await.map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// Low/high watermark of a topic partition.
    fn watermark<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partition: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source
                .watermark(&topic, partition)
                .await
                .map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// List topics (name, partitions, approx message count), filtered + paged.
    #[pyo3(signature = (search=None, limit=50, offset=0))]
    fn topics<'py>(
        &self,
        py: Python<'py>,
        search: Option<String>,
        limit: usize,
        offset: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        let page = Page::new(search, limit, offset);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source.list_topics(&page).await.map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// Per-partition detail + configuration of a topic.
    fn topic<'py>(&self, py: Python<'py>, topic: String) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source.topic_detail(&topic).await.map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// Consumer groups with a committed offset on this topic (scans groups).
    fn topic_groups<'py>(&self, py: Python<'py>, topic: String) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source.groups_consuming(&topic).await.map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// List consumer groups (name, state, members), filtered + paged.
    #[pyo3(signature = (search=None, limit=50, offset=0))]
    fn groups<'py>(
        &self,
        py: Python<'py>,
        search: Option<String>,
        limit: usize,
        offset: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        let page = Page::new(search, limit, offset);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source.list_groups(&page).await.map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// Consumer group detail: members, assignments, committed offsets, lag.
    fn group<'py>(&self, py: Python<'py>, group: String) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = source.group_detail(&group).await.map_err(storage_err)?;
            to_py(&v)
        })
    }

    /// List schema subjects (filtered + paged). Requires a registry.
    #[pyo3(signature = (search=None, limit=50, offset=0))]
    fn schemas<'py>(
        &self,
        py: Python<'py>,
        search: Option<String>,
        limit: usize,
        offset: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(registry) = self.registry.clone() else {
            return Err(KotatsuError::new_err("no schema registry configured"));
        };
        let page = Page::new(search, limit, offset);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subjects = registry.subjects().await.map_err(schema_err)?;
            let (items, total) = page.select(subjects);
            to_py(&serde_json::json!({ "items": items, "total": total }))
        })
    }

    /// A subject's versions, latest schema and compatibility level.
    fn schema<'py>(&self, py: Python<'py>, subject: String) -> PyResult<Bound<'py, PyAny>> {
        let Some(registry) = self.registry.clone() else {
            return Err(KotatsuError::new_err("no schema registry configured"));
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let versions = registry.versions(&subject).await.map_err(schema_err)?;
            let latest = registry
                .version(&subject, "latest")
                .await
                .map_err(schema_err)?;
            let compatibility = registry.compatibility(&subject).await;
            to_py(&serde_json::json!({
                "subject": subject,
                "versions": versions,
                "latest": latest,
                "compatibility": compatibility,
            }))
        })
    }

    /// A specific schema version.
    fn schema_version<'py>(
        &self,
        py: Python<'py>,
        subject: String,
        version: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(registry) = self.registry.clone() else {
            return Err(KotatsuError::new_err("no schema registry configured"));
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = registry
                .version(&subject, &version)
                .await
                .map_err(schema_err)?;
            to_py(&v)
        })
    }

    /// Fetch (and decode/filter) messages from a topic partition.
    #[pyo3(signature = (
        topic,
        partition=0,
        offset="latest".to_string(),
        limit=50,
        value_format=None,
        key_format=None,
        key_contains=None,
        value_contains=None,
        header_key=None,
        header_value=None,
        regex=false,
        max_scan=query::DEFAULT_MAX_SCAN,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn messages<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partition: i32,
        offset: String,
        limit: usize,
        value_format: Option<String>,
        key_format: Option<String>,
        key_contains: Option<String>,
        value_contains: Option<String>,
        header_key: Option<String>,
        header_value: Option<String>,
        regex: bool,
        max_scan: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone();
        let registry = self.registry.clone();
        let params = MessageParams {
            partition,
            offset,
            limit,
            key_format,
            value_format,
            key_contains,
            value_contains,
            header_key,
            header_value,
            regex,
            max_scan,
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let v = query::messages(&source, registry.as_ref(), &topic, &params)
                .await
                .map_err(query_err)?;
            to_py(&v)
        })
    }
}

#[pymodule]
fn kotatsu(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Source>()?;
    m.add("KotatsuError", m.py().get_type::<KotatsuError>())?;
    Ok(())
}
