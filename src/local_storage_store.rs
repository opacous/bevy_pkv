use crate::{StoreConfig, StoreImpl};

#[derive(Debug, Default)]
pub struct LocalStorageStore {
    prefix: String,
}

pub use LocalStorageStore as InnerStore;

#[derive(thiserror::Error, Debug)]
pub enum GetError {
    #[error("No value found for the given key")]
    NotFound,
    #[error("JavaScript error from getItem")]
    GetItem(wasm_bindgen::JsValue),
}

#[derive(thiserror::Error, Debug)]
pub enum SetError {
    #[error("JavaScript error from setItem")]
    SetItem(wasm_bindgen::JsValue),
    #[error("Error serializing as json")]
    Json(#[from] serde_json::Error),
}

impl LocalStorageStore {
    fn storage(&self) -> web_sys::Storage {
        web_sys::window()
            .expect("No window")
            .local_storage()
            .expect("Failed to get local storage")
            .expect("No local storage")
    }

    pub(crate) fn new(config: &StoreConfig) -> Self {
        let StoreConfig {
            qualifier,
            organization,
            application,
        } = config;
        Self {
            prefix: match qualifier.as_deref() {
                Some(qualifier) => format!("{qualifier}.{organization}.{application}"),
                None => format!("{organization}.{application}"),
            },
        }
    }

    fn format_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

impl StoreImpl for LocalStorageStore {
    type GetError = GetError;
    type SetError = SetError;

    fn set_string(&mut self, key: &str, value: &str) -> Result<(), SetError> {
        let json = serde_json::to_string(value)?;
        let storage = self.storage();
        let key = self.format_key(key);
        storage.set_item(&key, &json).map_err(SetError::SetItem)?;
        Ok(())
    }

    fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<T, GetError> {
        let storage = self.storage();
        let key = self.format_key(key);
        let entry = storage.get_item(&key).map_err(GetError::GetItem)?;
        let json = entry.as_ref().ok_or(GetError::NotFound)?;
        let value: T = serde_json::from_str(json).unwrap();
        Ok(value)
    }

    fn set<T: serde::Serialize>(&mut self, key: &str, value: &T) -> Result<(), SetError> {
        let json = serde_json::to_string(value)?;
        let storage = self.storage();
        let key = self.format_key(key);
        storage.set_item(&key, &json).map_err(SetError::SetItem)?;
        Ok(())
    }
}
