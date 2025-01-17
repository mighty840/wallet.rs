// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use iota_client::secret::{SecretManager, SecretManagerDto};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::{
    account::Account,
    account_manager::builder::AccountManagerBuilder,
    storage::{constants::*, Storage, StorageAdapter},
};

/// The storage used by the manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ManagerStorage {
    /// RocksDB storage.
    #[cfg(feature = "rocksdb")]
    Rocksdb,
     /// JammDB storage.
    #[cfg(feature = "jammdb")]
    JammDB,
    /// Storage backed by a Map in memory.
    Memory,
    /// Wasm storage.
    #[cfg(target_family = "wasm")]
    Wasm,
}

impl Default for ManagerStorage {
    fn default() -> Self {
        #[cfg(not(feature = "jammdb"))]
        return Self::Rocksdb;
        #[cfg(feature = "jammdb")]
        return Self::JammDB;
        #[cfg(target_family = "wasm")]
        return Self::Wasm;
        #[cfg(not(any(feature = "jammdb", target_family = "wasm")))]
        Self::Memory
    }
}

pub(crate) type StorageManagerHandle = Arc<Mutex<StorageManager>>;

/// Sets the storage adapter.
pub(crate) async fn new_storage_manager(
    encryption_key: Option<[u8; 32]>,
    storage: Box<dyn StorageAdapter + Send + Sync + 'static>,
) -> crate::Result<StorageManagerHandle> {
    let mut storage = Storage {
        inner: storage,
        encryption_key,
    };
    // Get the db version or set it
    if let Some(db_schema_version) = storage.get::<u8>(DATABASE_SCHEMA_VERSION_KEY).await? {
        if db_schema_version != DATABASE_SCHEMA_VERSION {
            return Err(crate::Error::Storage(format!(
                "unsupported database schema version {db_schema_version}"
            )));
        }
    } else {
        storage
            .set(DATABASE_SCHEMA_VERSION_KEY, DATABASE_SCHEMA_VERSION)
            .await?;
    };

    let account_indexes = storage.get(ACCOUNTS_INDEXATION_KEY).await?.unwrap_or_default();

    let storage_manager = StorageManager {
        storage,
        account_indexes,
    };

    Ok(Arc::new(Mutex::new(storage_manager)))
}

/// Storage manager
#[derive(Debug)]
pub struct StorageManager {
    pub(crate) storage: Storage,
    // account indexes for accounts in the database
    account_indexes: Vec<u32>,
}

impl StorageManager {
    pub fn id(&self) -> &'static str {
        self.storage.id()
    }

    #[cfg(test)]
    pub fn is_encrypted(&self) -> bool {
        self.storage.encryption_key.is_some()
    }

    pub async fn get<T: for<'de> Deserialize<'de>>(&self, key: &str) -> crate::Result<Option<T>> {
        self.storage.get(key).await
    }

    pub async fn save_account_manager_data(
        &mut self,
        account_manager_builder: &AccountManagerBuilder,
    ) -> crate::Result<()> {
        log::debug!("save_account_manager_data");
        self.storage
            .set(ACCOUNT_MANAGER_INDEXATION_KEY, account_manager_builder)
            .await?;

        if let Some(secret_manager) = &account_manager_builder.secret_manager {
            let secret_manager = secret_manager.read().await;
            let secret_manager_dto = SecretManagerDto::from(&*secret_manager);
            // Only store secret_managers that aren't SecretManagerDto::Mnemonic, because there the Seed can't be
            // serialized, so we can't create the SecretManager again
            match secret_manager_dto {
                SecretManagerDto::Mnemonic(_) => {}
                _ => {
                    self.storage.set(SECRET_MANAGER_KEY, secret_manager_dto).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn get_account_manager_data(&self) -> crate::Result<Option<AccountManagerBuilder>> {
        log::debug!("get_account_manager_data");
        if let Some(mut builder) = self
            .storage
            .get::<AccountManagerBuilder>(ACCOUNT_MANAGER_INDEXATION_KEY)
            .await?
        {
            log::debug!("get_account_manager_data {builder:?}");

            if let Some(secret_manager_dto) = self.storage.get::<SecretManagerDto>(SECRET_MANAGER_KEY).await? {
                log::debug!("get_secret_manager {secret_manager_dto:?}");

                // Only secret_managers that aren't SecretManagerDto::Mnemonic can be restored, because there the Seed
                // can't be serialized, so we can't create the SecretManager again
                match secret_manager_dto {
                    SecretManagerDto::Mnemonic(_) => {}
                    _ => {
                        let secret_manager = SecretManager::try_from(&secret_manager_dto)?;
                        builder.secret_manager = Some(Arc::new(RwLock::new(secret_manager)));
                    }
                }
            }
            Ok(Some(builder))
        } else {
            Ok(None)
        }
    }

    pub async fn get_accounts(&mut self) -> crate::Result<Vec<Account>> {
        if let Some(account_indexes) = self.storage.get(ACCOUNTS_INDEXATION_KEY).await? {
            if self.account_indexes.is_empty() {
                self.account_indexes = account_indexes;
            }
        } else {
            return Ok(Vec::new());
        }

        let mut accounts = Vec::new();
        for account_index in self.account_indexes.clone() {
            // PANIC: we assume that ACCOUNTS_INDEXATION_KEY and the different indexes are set together and
            // ACCOUNTS_INDEXATION_KEY has already been checked.
            accounts.push(
                self.get(&format!("{ACCOUNT_INDEXATION_KEY}{account_index}"))
                    .await?
                    .unwrap(),
            );
        }

        Ok(accounts)
    }

    pub async fn save_account(&mut self, account: &Account) -> crate::Result<()> {
        // Only add account index if not already present
        if !self.account_indexes.contains(account.index()) {
            self.account_indexes.push(*account.index());
        }

        self.storage
            .set(ACCOUNTS_INDEXATION_KEY, self.account_indexes.clone())
            .await?;
        self.storage
            .set(&format!("{ACCOUNT_INDEXATION_KEY}{}", account.index()), account)
            .await
    }

    pub async fn remove_account(&mut self, account_index: u32) -> crate::Result<()> {
        self.storage
            .remove(&format!("{ACCOUNT_INDEXATION_KEY}{account_index}"))
            .await?;
        self.account_indexes.retain(|a| a != &account_index);
        self.storage
            .set(ACCOUNTS_INDEXATION_KEY, self.account_indexes.clone())
            .await
    }
}
