//! Minting the canonical orchestrator agent identity: the short base36 code
//! that is the join key across orchestrate (registry), router
//! (`ActorIdentifier`), and message (recipient).
//!
//! The algorithm is Spirit's record-identifier mint verbatim (see
//! `spirit/src/store/record_identifier.rs`): a random base36 code drawn from
//! the OS CSPRNG, four characters long, growing one character at a time only
//! when a length saturates, rejected against the live key set for up to 128
//! random draws per length before a deterministic first-free scan closes the
//! gap. Exhausting the whole length span is a typed error.
//!
//! The mint holds the set of identifiers already in use; a code range owns the
//! value span and base36 rendering for a single code length. Both are real
//! data-bearing types — the mint cannot decide the next free identifier without
//! its `used_identifiers` set, and the range cannot render a code without its
//! `first_value`/`value_count`.

use std::collections::BTreeSet;

use signal_orchestrate::OrchestratorAgentIdentifier;

use crate::{Error, Result};

const MINIMUM_CODE_LENGTH: usize = 4;
const MAXIMUM_CODE_LENGTH: usize = 7;
const CODE_RADIX: u64 = 36;
const RANDOM_IDENTIFIER_ATTEMPTS_PER_LENGTH: usize = 128;

/// The live orchestrator-agent key set plus the code-length span the mint may
/// draw from. Production uses the four-to-seven span; the bounds are
/// constructor-injected so a saturation test can drive a tiny keyspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorAgentIdentifierMint {
    used_identifiers: BTreeSet<String>,
    minimum_code_length: usize,
    maximum_code_length: usize,
}

/// The value span and base36 rendering for a single code length. `pad_to` is
/// the mint's minimum length: a value that renders shorter is left-padded with
/// `0` so every code at the minimum length is exactly that wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrchestratorAgentIdentifierCodeRange {
    first_value: u64,
    value_count: u64,
    pad_to: usize,
}

impl OrchestratorAgentIdentifierMint {
    /// Build the mint over the identifiers already seated in the registry.
    pub fn from_identifiers<Identifiers>(identifiers: Identifiers) -> Self
    where
        Identifiers: IntoIterator<Item = String>,
    {
        Self::with_code_length_bounds(identifiers, MINIMUM_CODE_LENGTH, MAXIMUM_CODE_LENGTH)
    }

    fn with_code_length_bounds<Identifiers>(
        identifiers: Identifiers,
        minimum_code_length: usize,
        maximum_code_length: usize,
    ) -> Self
    where
        Identifiers: IntoIterator<Item = String>,
    {
        Self {
            used_identifiers: identifiers.into_iter().collect(),
            minimum_code_length,
            maximum_code_length,
        }
    }

    /// Draw the next free identifier, growing the code length only when a
    /// shorter length is saturated. Exhausting the whole span is a typed error.
    pub fn next_identifier(&self) -> Result<OrchestratorAgentIdentifier> {
        for code_length in self.minimum_code_length..=self.maximum_code_length {
            if let Some(identifier) = self.identifier_for_code_length(code_length)? {
                return Ok(OrchestratorAgentIdentifier::from_wire_token(identifier)?);
            }
        }
        Err(Error::OrchestratorAgentIdentifierExhausted {
            minimum: self.minimum_code_length,
            maximum: self.maximum_code_length,
        })
    }

    fn identifier_for_code_length(&self, code_length: usize) -> Result<Option<String>> {
        let range =
            OrchestratorAgentIdentifierCodeRange::new(code_length, self.minimum_code_length);
        for _ in 0..RANDOM_IDENTIFIER_ATTEMPTS_PER_LENGTH {
            let identifier = range.random_identifier()?;
            if !self.used_identifiers.contains(&identifier) {
                return Ok(Some(identifier));
            }
        }
        Ok(range.first_available_identifier(&self.used_identifiers))
    }
}

impl OrchestratorAgentIdentifierCodeRange {
    fn new(code_length: usize, minimum_code_length: usize) -> Self {
        let first_value = if code_length == minimum_code_length {
            0
        } else {
            Self::radix_power(code_length - 1)
        };
        let next_length_first_value = Self::radix_power(code_length);
        Self {
            first_value,
            value_count: next_length_first_value - first_value,
            pad_to: minimum_code_length,
        }
    }

    fn random_identifier(self) -> Result<String> {
        let mut bytes = [0_u8; 8];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::OrchestratorAgentIdentifierRandomness {
                message: error.to_string(),
            }
        })?;
        let offset = u64::from_be_bytes(bytes) % self.value_count;
        Ok(self.code_from_value(self.first_value + offset))
    }

    fn first_available_identifier(self, used_identifiers: &BTreeSet<String>) -> Option<String> {
        let last_value = self.first_value + self.value_count;
        (self.first_value..last_value)
            .map(|value| self.code_from_value(value))
            .find(|identifier| !used_identifiers.contains(identifier))
    }

    fn code_from_value(self, mut value: u64) -> String {
        let mut digits = Vec::new();
        while value > 0 {
            let digit = (value % CODE_RADIX) as u8;
            digits.push(Self::digit_character(digit));
            value /= CODE_RADIX;
        }
        while digits.len() < self.pad_to {
            digits.push('0');
        }
        digits.iter().rev().collect()
    }

    fn digit_character(digit: u8) -> char {
        match digit {
            0..=9 => char::from(b'0' + digit),
            10..=35 => char::from(b'a' + digit - 10),
            _ => unreachable!("base36 digit is constrained by modulo"),
        }
    }

    fn radix_power(exponent: usize) -> u64 {
        (0..exponent).fold(1, |value, _| value * CODE_RADIX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_character_codes() -> Vec<String> {
        (0..CODE_RADIX)
            .map(|value| OrchestratorAgentIdentifierCodeRange::new(1, 1).code_from_value(value))
            .collect()
    }

    #[test]
    fn default_mint_draws_four_character_codes() {
        let mint = OrchestratorAgentIdentifierMint::from_identifiers(std::iter::empty());
        let identifier = mint.next_identifier().expect("mint an identifier");
        assert_eq!(identifier.as_str().len(), MINIMUM_CODE_LENGTH);
        assert!(
            identifier
                .as_str()
                .chars()
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        );
    }

    #[test]
    fn sequential_draws_are_unique_against_the_live_set() {
        let mut used: BTreeSet<String> = BTreeSet::new();
        for _ in 0..64 {
            let mint = OrchestratorAgentIdentifierMint::from_identifiers(used.iter().cloned());
            let identifier = mint.next_identifier().expect("mint an identifier");
            assert!(
                used.insert(identifier.as_str().to_string()),
                "mint returned a duplicate identifier {}",
                identifier.as_str()
            );
        }
    }

    #[test]
    fn a_saturated_length_falls_through_to_the_first_free_code() {
        // A single-character span holds 36 codes; injecting 35 forces the
        // random draws to miss and the deterministic scan to find the one gap.
        let mut used = single_character_codes();
        let expected_free = used.pop().expect("a code to leave free");
        let mint = OrchestratorAgentIdentifierMint::with_code_length_bounds(used, 1, 1);
        let identifier = mint.next_identifier().expect("mint the one free code");
        assert_eq!(identifier.as_str(), expected_free);
    }

    #[test]
    fn exhausting_the_whole_span_is_a_typed_error() {
        let used = single_character_codes();
        let mint = OrchestratorAgentIdentifierMint::with_code_length_bounds(used, 1, 1);
        let error = mint
            .next_identifier()
            .expect_err("a fully saturated span mints nothing");
        assert!(matches!(
            error,
            Error::OrchestratorAgentIdentifierExhausted {
                minimum: 1,
                maximum: 1
            }
        ));
    }

    #[test]
    fn growing_the_length_seats_more_codes_than_the_minimum_span() {
        // Saturate the single-character span but allow growth to two
        // characters: the mint must find a longer free code.
        let used = single_character_codes();
        let mint = OrchestratorAgentIdentifierMint::with_code_length_bounds(used, 1, 2);
        let identifier = mint.next_identifier().expect("mint a two-character code");
        assert_eq!(identifier.as_str().len(), 2);
    }
}
