use crate::{Location, StoreImpl};
use redb::{Database, ReadableTable, TableDefinition};
use serde::de::DeserializeSeed;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::{Debug, Formatter};
use tracing::info;

pub struct ReDbStore {
    db: Database,
}
impl Debug for ReDbStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(f, "ReDb")?;
        Ok(())
    }
}
pub use ReDbStore as InnerStore;

/// Errors that can occur during `PkvStore::get`
#[derive(thiserror::Error, Debug)]
pub enum GetError {
    /// An internal storage error from the `redb` crate
    #[error("ReDbStorageError error")]
    ReDbStorageError(#[from] redb::StorageError),
    /// An internal transaction error from the `redb` crate
    #[error("ReDbTransactionError error")]
    ReDbTransactionError(#[from] redb::TransactionError),
    /// An internal table error from the `redb` crate
    #[error("ReDbTableError error")]
    ReDbTableError(#[from] redb::TableError),
    /// The value for the given key was not found
    #[error("No value found for the given key")]
    NotFound,
    /// Error when deserializing the value
    #[error("MessagePack deserialization error")]
    MessagePack(#[from] rmp_serde::decode::Error),
}

/// Errors that can occur during `PkvStore::set`
#[derive(thiserror::Error, Debug)]
pub enum SetError {
    /// An internal commit error from the `redb` crate
    #[error("ReDbCommitError error")]
    ReDbCommitError(#[from] redb::CommitError),
    /// An internal storage error from the `redb` crate
    #[error("ReDbStorageError error")]
    ReDbStorageError(#[from] redb::StorageError),
    /// An internal transaction error from the `redb` crate
    #[error("ReDbTransactionError error")]
    ReDbTransactionError(#[from] redb::TransactionError),
    /// An internal table error from the `redb` crate
    #[error("ReDbTableError error")]
    ReDbTableError(#[from] redb::TableError),
    /// Error when serializing the value
    #[error("MessagePack serialization error")]
    MessagePack(#[from] rmp_serde::encode::Error),
    #[error("KeyConversionError")]
    KeyConversion,
}

impl ReDbStore {
    pub(crate) fn new(location: Location) -> Self {
        let dir_path = location.get_path();
        std::fs::create_dir_all(&dir_path)
            .expect("Failed to create directory to init key value store");
        let db_path = dir_path.join("bevy_pkv.redb");
        let path = db_path.as_path().to_str().unwrap_or("./").to_string();
        let db = Database::create(db_path).expect("Failed to init key value store");

        info!("Opened new redb data store at {:?}", path);

        let write_txn = db.begin_write().unwrap();
        write_txn.open_table(TABLE).unwrap();
        write_txn.commit().unwrap();

        Self { db }
    }
}

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("redb");

impl StoreImpl for ReDbStore {
    type GetError = GetError;
    type SetError = SetError;

    /// Serialize and store the value
    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), Self::SetError> {
        let mut serializer = rmp_serde::Serializer::new(Vec::new()).with_struct_map();
        value.serialize(&mut serializer)?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert(key, serializer.into_inner().as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    /// More or less the same as set::<String>, but can take a &str
    fn set_string(&mut self, key: &str, value: &str) -> Result<(), Self::SetError> {
        let bytes = rmp_serde::to_vec(value)?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert(key, bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Get the value for the given key
    /// returns Err(GetError::NotFound) if the key does not exist in the key value store.
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T, Self::GetError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        let key = table.get(key)?.ok_or(Self::GetError::NotFound)?;
        let bytes = key.value();
        let value = rmp_serde::from_slice(bytes)?;
        Ok(value)
    }

    fn get_with<T: for<'de> DeserializeSeed<'de>>(
        &self,
        key: &str,
        seed: T,
    ) -> Result<<T as DeserializeSeed<'_>>::Value, Self::GetError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        let key = table.get(key)?.ok_or(Self::GetError::NotFound)?;
        let bytes = key.value();
        let mut deserializer = rmp_serde::decode::Deserializer::new(bytes);
        seed.deserialize(&mut deserializer).map_err(Into::into)
    }

    /// Clear all keys and their values
    fn clear(&mut self) -> Result<(), Self::SetError> {
        let write_txn = self.db.begin_write()?;
        write_txn.delete_table(TABLE)?;
        write_txn.commit()?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<(), Self::SetError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            // let key = table.get(key)?.ok_or(Self::SetError::KeyConversion)?;
            table.remove(key)?.ok_or(Self::SetError::KeyConversion)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn keys(&self) -> Result<Vec<String>, Self::GetError> {
        let keys: Vec<String> = {
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            let range = table.iter()?;
            range.map(|r| r.unwrap().0.value().to_string()).collect()
        };

        Ok(keys)
    }
}
