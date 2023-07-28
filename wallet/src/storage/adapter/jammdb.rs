// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::StorageAdapter;
use jammdb::{DB, OpenOptions};
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::sync::Mutex;

/// The storage id.
pub const STORAGE_ID: &str = "JammDB";

const BUCKET_NAME: &str = "storage";

/// Key value storage adapter.
pub struct JammdbStorageAdapter {
    db: Arc<Mutex<DB>>,
}

fn storage_err<E: ToString>(error: E) -> crate::Error {
    crate::Error::Storage(error.to_string())
}

impl JammdbStorageAdapter {
    /// Initialises the storage adapter.
    pub fn new(path: impl AsRef<Path>) -> crate::Result<Self> {
        let db = OpenOptions::new().pagesize(4096).num_pages(32).open(path).map_err(storage_err)?;
        // create a default bucket
        let tx = db.tx(true).map_err(storage_err)?;
        tx.get_or_create_bucket(BUCKET_NAME).map_err(storage_err)?;
        tx.commit().map_err(storage_err)?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }
}

#[async_trait::async_trait]
impl StorageAdapter for JammdbStorageAdapter {
    fn id(&self) -> &'static str {
        STORAGE_ID
    }

    async fn get(&self, key: &str) -> crate::Result<String> {
        let db = self.db.lock().await;
        let tx = db.tx(false).map_err(storage_err)?;
        let bucket = tx.get_bucket(BUCKET_NAME).map_err(storage_err)?;
        match bucket.get(key) {
                Some(r) => Ok(String::from_utf8_lossy(&r.kv().value()).to_string()),
                None => Err(crate::Error::RecordNotFound),
        }
    }

    async fn set(&mut self, key: &str, record: String) -> crate::Result<()> {
        let db = self.db.lock().await;
        let tx = db.tx(true).map_err(storage_err)?;
        let bucket = tx.get_bucket(BUCKET_NAME).map_err(storage_err)?;
        bucket.put(key, record).map_err(storage_err)?;
        tx.commit().map_err(storage_err)?;
        Ok(())
    }

    async fn batch_set(&mut self, records: HashMap<String, String>) -> crate::Result<()> {
        let db = self.db.lock().await;
        let tx = db.tx(true).map_err(storage_err)?;
        let bucket = tx.get_bucket(BUCKET_NAME).map_err(storage_err)?;
        for (key, value) in records {
            bucket.put(key, value).map_err(storage_err)?;
        }
        tx.commit().map_err(storage_err)?;
        Ok(())
    }

    async fn remove(&mut self, key: &str) -> crate::Result<()> {
        let db = self.db.lock().await;
        let tx = db.tx(true).map_err(storage_err)?;
        let bucket = tx.get_bucket(BUCKET_NAME).map_err(storage_err)?;

        bucket.delete(key).map_err(storage_err)?;
        Ok(())
    }
}
