// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::StorageAdapter;
use jammdb::{DB, OpenOptions};
use std::{collections::HashMap, path::{Path, PathBuf}, sync::Arc, fmt::Debug};
use tokio::sync::Mutex;

/// The storage id.
pub const STORAGE_ID: &str = "JammDB";

const BUCKET_NAME: &str = "storage";

impl Debug for JammdbStorageAdapter{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JammDbStorageAdapter")
    }
}

/// Key value storage adapter.
pub struct JammdbStorageAdapter {
    db: Arc<Mutex<DB>>,
}

impl JammdbStorageAdapter {
    /// Initialises the storage adapter.
    pub fn new(path: impl AsRef<Path>) -> crate::Result<Self> {
        let mut db_path = PathBuf::from("./sdk-wallet.db");
        let dir_path = path.as_ref().to_string_lossy().to_string();
        let mut temp_path = PathBuf::from(dir_path);
        if path.as_ref().is_dir() {
            temp_path.push(db_path);

        }
        db_path = temp_path;
        let db = OpenOptions::new().pagesize(4096).num_pages(32).open(db_path)?;
        // create a default bucket
        let tx = db.tx(true)?;
        tx.get_or_create_bucket(BUCKET_NAME)?;
        tx.commit()?;
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

    async fn get(&self, key: &str) -> crate::Result<Option<String>> {
        let db = self.db.lock().await;
        let tx = db.tx(false)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        match bucket.get(key) {
            Some(r) => Ok(Some(String::from_utf8_lossy(&r.kv().value()).to_string())),
            None => Err(crate::Error::from(jammdb::Error::KeyValueMissing))
        }
    }

    async fn set(&mut self, key: &str, record: String) -> crate::Result<()> {
        let db = self.db.lock().await;
        let tx = db.tx(true)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        bucket.put(key, record)?;
        tx.commit()?;
        Ok(())
    }

    async fn batch_set(&mut self, records: HashMap<String, String>) -> crate::Result<()> {
        let db = self.db.lock().await;
        let tx = db.tx(true)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        for (key, value) in records {
            bucket.put(key, value)?;
        }
        tx.commit()?;
        Ok(())
    }

    async fn remove(&mut self, key: &str) -> crate::Result<()> {
        let db = self.db.lock().await;
        let tx = db.tx(true)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;

        bucket.delete(key)?;
        Ok(())
    }
}
