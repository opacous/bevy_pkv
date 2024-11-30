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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestComponent {
        value: String,
        number: i32,
    }

    fn create_test_store() -> (ParquetStore, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(Location::CustomPath(temp_dir.path()));
        (store, temp_dir)
    }

    #[test]
    fn test_set_and_get() {
        let (mut store, _temp_dir) = create_test_store();
        let test_data = TestComponent {
            value: "test".to_string(),
            number: 42,
        };

        store
            .set("TestComponent/1", &test_data)
            .expect("Failed to store data");

        let retrieved: TestComponent = store
            .get("TestComponent/1")
            .expect("Failed to retrieve data");

        assert_eq!(test_data, retrieved);
    }

    #[test]
    fn test_remove() {
        let (mut store, _temp_dir) = create_test_store();
        let test_data = TestComponent {
            value: "test".to_string(),
            number: 42,
        };

        store
            .set("TestComponent/1", &test_data)
            .expect("Failed to store data");
        
        store.remove("TestComponent/1").expect("Failed to remove data");

        assert!(matches!(
            store.get::<TestComponent>("TestComponent/1"),
            Err(GetError::NotFound)
        ));
    }

    #[test]
    fn test_clear() {
        let (mut store, _temp_dir) = create_test_store();
        let test_data = TestComponent {
            value: "test".to_string(),
            number: 42,
        };

        store
            .set("TestComponent/1", &test_data)
            .expect("Failed to store data");
        
        store.clear().expect("Failed to clear store");

        assert!(matches!(
            store.get::<TestComponent>("TestComponent/1"),
            Err(GetError::NotFound)
        ));
    }
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
        let parts: Vec<&str> = key.split('/').collect();
        let component_type = parts.first().ok_or_else(|| {
            GetError::NotFound
        })?;

        let path = self.get_component_path(component_type);
        if !path.exists() {
            return Err(GetError::NotFound);
        }

        let file = fs::File::open(path)?;
        let reader = parquet::arrow::arrow_reader::ParquetFileArrowReader::try_new(file)?;
        let mut batch_reader = reader.get_record_batch_reader(1024)?;

        while let Some(batch) = batch_reader.next() {
            let batch = batch?;
            let key_array = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| GetError::Deserialization(serde_json::Error::custom("Invalid key type")))?;
            let value_array = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| GetError::Deserialization(serde_json::Error::custom("Invalid value type")))?;

            for i in 0..batch.num_rows() {
                if key_array.value(i) == key {
                    let value_str = value_array.value(i);
                    return Ok(serde_json::from_str(value_str)?);
                }
            }
        }

        Err(GetError::NotFound)
    }

    fn get_with<T: for<'de> DeserializeSeed<'de>>(
        &self,
        key: &str,
        seed: T,
    ) -> Result<<T as DeserializeSeed<'_>>::Value, Self::GetError> {
        let parts: Vec<&str> = key.split('/').collect();
        let component_type = parts.first().ok_or_else(|| {
            GetError::NotFound
        })?;

        let path = self.get_component_path(component_type);
        if !path.exists() {
            return Err(GetError::NotFound);
        }

        let file = fs::File::open(path)?;
        let reader = parquet::arrow::arrow_reader::ParquetFileArrowReader::try_new(file)?;
        let mut batch_reader = reader.get_record_batch_reader(1024)?;

        while let Some(batch) = batch_reader.next() {
            let batch = batch?;
            let key_array = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| GetError::Deserialization(serde_json::Error::custom("Invalid key type")))?;
            let value_array = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| GetError::Deserialization(serde_json::Error::custom("Invalid value type")))?;

            for i in 0..batch.num_rows() {
                if key_array.value(i) == key {
                    let value_str = value_array.value(i);
                    let mut deserializer = serde_json::Deserializer::from_str(value_str);
                    return seed.deserialize(&mut deserializer).map_err(Into::into);
                }
            }
        }

        Err(GetError::NotFound)
    }

    fn remove(&mut self, key: &str) -> Result<(), Self::SetError> {
        let parts: Vec<&str> = key.split('/').collect();
        let component_type = parts.first().ok_or_else(|| {
            SetError::Serialization(serde_json::Error::custom("Invalid key format"))
        })?;

        let path = self.get_component_path(component_type);
        if !path.exists() {
            return Ok(());
        }

        // Read existing data
        let file = fs::File::open(&path)?;
        let reader = parquet::arrow::arrow_reader::ParquetFileArrowReader::try_new(file)?;
        let mut batch_reader = reader.get_record_batch_reader(1024)?;
        
        let mut keys = Vec::new();
        let mut values = Vec::new();
        
        while let Some(batch) = batch_reader.next() {
            let batch = batch?;
            let key_array = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| SetError::Serialization(serde_json::Error::custom("Invalid key type")))?;
            let value_array = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| SetError::Serialization(serde_json::Error::custom("Invalid value type")))?;

            for i in 0..batch.num_rows() {
                if key_array.value(i) != key {
                    keys.push(key_array.value(i));
                    values.push(value_array.value(i));
                }
            }
        }

        // Write back filtered data
        if !keys.is_empty() {
            self.write_batch(component_type, &keys[0], &values[0])?;
            for i in 1..keys.len() {
                self.write_batch(component_type, &keys[i], &values[i])?;
            }
        } else {
            // If no data left, remove the file
            fs::remove_file(path)?;
        }

        Ok(())
    }

    fn clear(&mut self) -> Result<(), Self::SetError> {
        fs::remove_dir_all(&self.base_path)?;
        fs::create_dir_all(&self.base_path)?;
        Ok(())
    }
}
