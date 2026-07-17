//! Pre-migration store preservation.
//!
//! Before the first store repair mutates the file, the migration copies it
//! aside under the store directory's existing preserve naming convention:
//! `<store>.v<target>-premigration-<utc-stamp>Z`. The copy's age is readable
//! from its own name, so it stays reap-eligible under the standing retention
//! windows instead of accumulating silently.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sema_engine::SchemaVersion;

use crate::{Result, StoreLocation};

/// A copy of the store file taken before migration mutates it, sitting beside
/// the original. Creation refuses to overwrite an existing file, and any
/// failure aborts the migration: no repair runs against an unpreserved store.
#[derive(Debug)]
pub struct PreMigrationPreserve {
    path: PathBuf,
}

impl PreMigrationPreserve {
    /// Copy the store aside before the first repair, naming the copy after the
    /// migration's target schema version and the current UTC second.
    pub fn create(store: &StoreLocation, target: SchemaVersion) -> Result<Self> {
        let stamp = UtcStamp::now()?;
        let path = Self::path_for(store.as_path(), target, &stamp)
            .ok_or_else(|| Self::failure(store, "store path has no file name"))?;
        if path.exists() {
            return Err(Self::failure(
                store,
                format!("preserve path already exists: {}", path.display()),
            ));
        }
        std::fs::copy(store.as_path(), &path)
            .map_err(|source| Self::failure(store, source.to_string()))?;
        Ok(Self { path })
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    /// The sibling path the preserve is written to:
    /// `<store>.v<target>-premigration-<utc-stamp>Z`.
    fn path_for(store: &Path, target: SchemaVersion, stamp: &UtcStamp) -> Option<PathBuf> {
        let file_name = store.file_name()?.to_str()?;
        Some(store.with_file_name(format!(
            "{file_name}.v{}-premigration-{stamp}",
            target.value()
        )))
    }

    fn failure(store: &StoreLocation, message: impl Into<String>) -> crate::Error {
        crate::Error::PreMigrationPreserve {
            store: store.as_str().to_string(),
            message: message.into(),
        }
    }
}

/// A second-resolution UTC wall-clock stamp rendered `YYYYMMDDTHHMMSSZ`,
/// matching the store directory's preserve names.
struct UtcStamp {
    seconds_since_epoch: u64,
}

impl UtcStamp {
    fn now() -> Result<Self> {
        let seconds_since_epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        Ok(Self { seconds_since_epoch })
    }

    /// The proleptic-Gregorian calendar date for this stamp's day, via Howard
    /// Hinnant's civil-from-days algorithm.
    fn civil_date(&self) -> (i64, u64, u64) {
        let days = (self.seconds_since_epoch / 86_400) as i64;
        let shifted = days + 719_468;
        let era = shifted.div_euclid(146_097);
        let day_of_era = shifted.rem_euclid(146_097) as u64;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_index = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_index + 2) / 5 + 1;
        let month = if month_index < 10 {
            month_index + 3
        } else {
            month_index - 9
        };
        let mut year = year_of_era as i64 + era * 400;
        if month <= 2 {
            year += 1;
        }
        (year, month, day)
    }
}

impl fmt::Display for UtcStamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = self.civil_date();
        let seconds_of_day = self.seconds_since_epoch % 86_400;
        write!(
            formatter,
            "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
            seconds_of_day / 3_600,
            (seconds_of_day % 3_600) / 60,
            seconds_of_day % 60
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_renders_epoch_origin() {
        let stamp = UtcStamp {
            seconds_since_epoch: 0,
        };
        assert_eq!(stamp.to_string(), "19700101T000000Z");
    }

    #[test]
    fn stamp_matches_store_directory_preserve_convention() {
        // 2026-07-17T09:36:11Z, the instant in the store directory's existing
        // `orchestrate.sema.v8-preserve-20260717T093611Z` name.
        let stamp = UtcStamp {
            seconds_since_epoch: 1_784_280_971,
        };
        assert_eq!(stamp.to_string(), "20260717T093611Z");
    }

    #[test]
    fn stamp_handles_leap_year_day() {
        // 2024-02-29T12:00:00Z.
        let stamp = UtcStamp {
            seconds_since_epoch: 1_709_208_000,
        };
        assert_eq!(stamp.to_string(), "20240229T120000Z");
    }

    #[test]
    fn preserve_path_names_target_version_beside_store() {
        let stamp = UtcStamp {
            seconds_since_epoch: 0,
        };
        let path = PreMigrationPreserve::path_for(
            Path::new("/var/state/orchestrate.sema"),
            SchemaVersion::new(8),
            &stamp,
        )
        .expect("path");
        assert_eq!(
            path,
            Path::new("/var/state/orchestrate.sema.v8-premigration-19700101T000000Z")
        );
    }
}
