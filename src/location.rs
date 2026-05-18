use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreLocation {
    path: String,
}

impl StoreLocation {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    pub fn as_str(&self) -> &str {
        &self.path
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.path)
    }
}
