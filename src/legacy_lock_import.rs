use std::path::{Path, PathBuf};

use signal_orchestrate::{RoleName, ScopeReason, ScopeReference, TaskToken, WirePath};

use crate::{Error, OrchestrateLayout, OrchestrateTables, Result, StoredClaim};

pub struct LegacyLockImport<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

struct LegacyLockFile {
    role: RoleName,
    path: PathBuf,
    body: String,
}

struct LegacyLockLine<'line> {
    role: RoleName,
    path: &'line Path,
    line_number: usize,
    text: &'line str,
}

struct LegacyScopeText<'text> {
    text: &'text str,
}

impl<'tables> LegacyLockImport<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    pub fn import_if_store_has_no_claims(&self) -> Result<()> {
        if !self.tables.claim_records()?.is_empty() {
            return Ok(());
        }

        let claims = self.imported_claims()?;
        if !claims.is_empty() {
            self.tables.replace_all_claims(&claims)?;
        }
        Ok(())
    }

    fn imported_claims(&self) -> Result<Vec<StoredClaim>> {
        let mut claims = Vec::new();
        for role in self.tables.role_records()? {
            claims.extend(LegacyLockFile::read(role.role, self.layout)?.claims()?);
        }
        Ok(claims)
    }
}

impl LegacyLockFile {
    fn read(role: RoleName, layout: &OrchestrateLayout) -> Result<Self> {
        let path = layout.role_lock_path(&role);
        let body = match std::fs::read_to_string(&path) {
            Ok(body) => body,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self { role, path, body })
    }

    fn claims(&self) -> Result<Vec<StoredClaim>> {
        let mut claims = Vec::new();
        for (index, line) in self.body.lines().enumerate() {
            let line_number = index + 1;
            if line.trim().is_empty() {
                continue;
            }
            claims.push(
                LegacyLockLine {
                    role: self.role.clone(),
                    path: &self.path,
                    line_number,
                    text: line,
                }
                .claim()?,
            );
        }
        Ok(claims)
    }
}

impl LegacyLockLine<'_> {
    fn claim(&self) -> Result<StoredClaim> {
        let Some((scope, reason)) = self.text.split_once(" # ") else {
            return Err(Error::InvalidLegacyLockLine {
                path: self.path.display().to_string(),
                line_number: self.line_number,
                line: self.text.to_string(),
            });
        };
        let scope = LegacyScopeText { text: scope }.scope()?;
        let reason = ScopeReason::from_text(reason)?;
        Ok(StoredClaim::new(self.role.clone(), scope, reason))
    }
}

impl LegacyScopeText<'_> {
    fn scope(&self) -> Result<ScopeReference> {
        if let Some(token) = self
            .text
            .strip_prefix('[')
            .and_then(|text| text.strip_suffix(']'))
        {
            return Ok(ScopeReference::Task(TaskToken::from_wire_token(token)?));
        }
        Ok(ScopeReference::Path(WirePath::from_absolute_path(
            self.text,
        )?))
    }
}
