use crate::{Location, StoreImpl};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::de::DeserializeSeed;
use serde::{de::DeserializeOwned, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug)]
pub struct ParquetStore {
    base_path: PathBuf,
}

pub use ParquetStore as InnerStore;

#[derive(thiserror::Error, Debug)]
pub enum GetError {
    #[error("No value found for the given key")]
    NotFound,
    #[error("Error deserializing data")]
    Deserialization(#[from] serde_json::Error),
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error")]
    Arrow(#[from] arrow::error::ArrowError),
}

#[derive(thiserror::Error, Debug)]
pub enum SetError {
    #[error("Error serializing data")]
    Serialization(#[from] serde_json::Error),
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error")]
    Arrow(#[from] arrow::error::ArrowError),
}

impl ParquetStore {
    pub(crate) fn new(location: Location) -> Self {
        let base_path = location.get_path().join("bevy_pkv_parquet");
        fs::create_dir_all(&base_path).expect("Failed to create directory for parquet store");
        Self { base_path }
    }

    fn get_component_path(&self, component_type: &str) -> PathBuf {
        self.base_path.join(format!("{}.parquet", component_type))
    }

    fn write_batch(&self, component_type: &str, key: &str, value: &str) -> Result<(), SetError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let key_array = StringArray::from(vec![key]);
        let value_array = StringArray::from(vec![value]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(key_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
            ],
        )?;

        let path = self.get_component_path(component_type);
        let file = fs::File::create(path)?;
        
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        
        writer.write(&batch)?;
        writer.close()?;
        
        Ok(())
    }
}

impl StoreImpl for ParquetStore {
    type GetError = GetError;
    type SetError = SetError;

    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), Self::SetError> {
        // Extract component type from key (assuming format "ComponentType/EntityId")
        let parts: Vec<&str> = key.split('/').collect();
        let component_type = parts.first().ok_or_else(|| {
            SetError::Serialization(serde_json::Error::custom("Invalid key format"))
        })?;

        let value_str = serde_json::to_string(value)?;
        self.write_batch(component_type, key, &value_str)
    }

    fn set_string(&mut self, key: &str, value: &str) -> Result<(), Self::SetError> {
        let parts: Vec<&str> = key.split('/').collect();
        let component_type = parts.first().ok_or_else(|| {
            SetError::Serialization(serde_json::Error::custom("Invalid key format"))
        })?;

        self.write_batch(component_type, key, value)
    }

    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T, Self::GetError> {
        // Implementation needed
        todo!("Implement get() for ParquetStore")
    }

    fn get_with<T: for<'de> DeserializeSeed<'de>>(
        &self,
        key: &str,
        seed: T,
    ) -> Result<<T as DeserializeSeed<'_>>::Value, Self::GetError> {
        // Implementation needed
        todo!("Implement get_with() for ParquetStore")
    }

    fn remove(&mut self, key: &str) -> Result<(), Self::SetError> {
        // Implementation needed
        todo!("Implement remove() for ParquetStore")
    }

    fn clear(&mut self) -> Result<(), Self::SetError> {
        fs::remove_dir_all(&self.base_path)?;
        fs::create_dir_all(&self.base_path)?;
        Ok(())
    }
}
