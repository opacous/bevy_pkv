use std::{fs, io};
use std::path::Path;
use serde::de::DeserializeSeed;
use crate::{Location, PlatformDefault, StoreImpl};

#[derive(Debug, Default)]
pub struct FSStore {
    path: String,
}

pub use FSStore as InnerStore;

#[derive(thiserror::Error, Debug)]
pub enum GetError {
    #[error("No value found for the given key")]
    NotFound,
    #[error("error deserializing json")]
    Json(#[from] serde_json::Error),
    #[error("Error opening file")]
    File(#[from] io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum SetError {
    #[error("Error serializing as json")]
    Json(#[from] serde_json::Error),
    #[error("Error opening file")]
    File(#[from] io::Error),
}

impl FSStore {
    pub(crate) fn new(location: Location) -> Self {
        let dir_path = location.get_path();
        fs::create_dir_all(&dir_path)
            .expect("Failed to create directory to init key value store");
        Self {
            path: dir_path.as_path().to_str().unwrap_or("./").to_string(),
        }
    }

    fn format_key(&self, key: &str) -> String {
        format!("{}/{}", self.path, key)
    }
}

impl StoreImpl for FSStore {
    type GetError = GetError;
    type SetError = SetError;

    fn set_string(&mut self, key: &str, value: &str) -> Result<(), SetError> {
        let json = serde_json::to_string(value)?;
        let key = self.format_key(key);
        fs::write(key,json.as_bytes())?;
        Ok(())
    }

    fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<T, GetError> {
        let key = self.format_key(key);
        let data = fs::read_to_string(key)?;
        let value: T = serde_json::from_str(data.as_str())?;
        Ok(value)
    }

    fn get_with<T: for<'de> DeserializeSeed<'de>>(&self, key: &str, seed: T) -> Result<<T as DeserializeSeed<'_>>::Value, Self::GetError> {
        todo!()
    }

    fn set<T: serde::Serialize>(&mut self, key: &str, value: &T) -> Result<(), SetError> {
        let json = serde_json::to_string(value)?;
        let key = self.format_key(key);
        fs::write(key,json.as_bytes())?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<(), Self::SetError> {
        let key = self.format_key(key);
        fs::remove_file(key)?;
        Ok(())
    }

    /// Because the data is cleared by looping through it, it may take time or run slowly
    fn clear(&mut self) -> Result<(), SetError> {
        for entry in fs::read_dir(self.path.as_str())?{
            fs::remove_file(entry?.path());
        }
        Ok(())
    }
}
