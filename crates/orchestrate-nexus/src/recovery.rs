//! Declaring, against the store file itself, that it was moved here.
//!
//! # Why this is not a signal
//!
//! Every other privileged operation on this Nexus goes through the meta
//! socket, and this one cannot, for a reason particular to what it is about.
//! The Nexus finds its store first and reads its configuration out of it, so
//! the socket paths a client would reach it on are named by the very store
//! whose right to those paths is in question. A refused store that opened its
//! meta socket to be told it may serve would have bound, in order to ask, the
//! paths belonging to the Nexus still serving at the original. The authority
//! that decides a relocation therefore cannot be reached through the thing
//! the relocation is about.
//!
//! So the authority here is the filesystem's: whoever may write the store
//! file may say where it now lives. That is the same boundary Postgres draws
//! around `pg_resetwal` and etcd around `etcdutl snapshot restore`, and for
//! the same reason — re-identifying data is done with nothing attached to it.
//! etcd offers the other shape too, `--force-new-cluster`, a flag on a live
//! start, and files it under "Unsafe feature" precisely because a running
//! server is the wrong place to settle what the server is.
//!
//! # Why a declaration at all, when the world can be looked at
//!
//! Because an absence is evidence and not consent. `rm` and the second half
//! of `mv` leave the same empty path behind, and a guard that read consent
//! off an empty path would consent on behalf of whoever deleted a file. It
//! would also admit every stale copy whose original has since gone — a
//! restored backup is exactly that — with no one having said anything.
//!
//! So both: the owner declares, which supplies the entitlement, and this tool
//! checks the world before writing it down, which is what makes the
//! declaration hard to make falsely. The two checks are what a copy cannot
//! pass:
//!
//! - **Nothing remains at the origin.** `cp` leaves the original in place and
//!   `mv` does not. A copy therefore cannot be declared a move without the
//!   operator first removing or renaming the original — at which point there
//!   genuinely is one store, and admitting it is right.
//! - **Nothing holds the sockets.** A store can be unlinked while the Nexus
//!   serving from it runs on, so an absent origin is not proof of an absent
//!   claimant. The claims 0.34.0 put beside the socket paths answer that
//!   directly, and this tool takes them exactly as the Nexus does — the same
//!   inversion Postgres uses when `pg_resetwal` refuses to run while
//!   `postmaster.pid` is there.
//!
//! Neither check is the guard. The guard is the declaration the checks earn,
//! which names one origin and one destination and is spent by the open that
//! honours it.

use std::path::Path;

use nexus::{
    Bearing, Configurable, Identifying, Relocated, Relocating, Relocation, Situated, StoreIdentity,
};
use sema_engine::{Assertion, Engine, EngineOpen, Mutation, QueryPlan, TableReference};
use signal_orchestrate::OrchestrateNexusConfiguration;
use thiserror::Error;

use crate::{
    store::{
        StoreError,
        record::{
            Familial, SCHEMA_VERSION, StoredConfiguration, StoredMetadata, StoredRelocation,
            StoredSituation, Storing,
        },
    },
    transport::{
        TransportError,
        socket::{Claiming, SocketClaim},
    },
};

/// A move that has been declared against a store file.
pub struct StoreRelocation {
    relocation: Relocation,
    vacated_socket_vector: Vec<String>,
}

/// Declares, with no Nexus running, that the store now at this path is the
/// one that was bound at the path it records.
pub trait DeclaresRelocation: Sized {
    fn declare(store_path: &Path) -> Result<Self, RecoveryError>;
    fn origin(&self) -> &str;
    fn destination(&self) -> &str;
    /// The socket paths found free while declaring — what the relocated Nexus
    /// is expected to bind, reported so the operator can see what this
    /// declaration is about to let something listen on.
    fn vacated_socket_vector(&self) -> &[String];
}

impl DeclaresRelocation for StoreRelocation {
    fn declare(store_path: &Path) -> Result<Self, RecoveryError> {
        if !store_path.exists() {
            return Err(RecoveryError::NoStore {
                store_path: store_path.display().to_string(),
            });
        }
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))?;
        let situation: TableReference<StoredSituation> =
            engine.register_table(StoredSituation::descriptor())?;
        let recorded = match engine.match_records(QueryPlan::all(situation))?.records() {
            [] => {
                return Err(RecoveryError::NeverBound {
                    store_path: store_path.display().to_string(),
                });
            }
            [recorded] => recorded.situation.clone(),
            rows => return Err(StoreError::SituationInvariant { count: rows.len() }.into()),
        };
        let store = StoreIdentity::of(store_path);
        let origin = recorded.store().path().to_owned();
        match recorded.bearing(&store) {
            Bearing::Settled => {
                return Err(RecoveryError::Settled { origin });
            }
            Bearing::Moved => {
                return Err(RecoveryError::AlreadyRecognised {
                    origin,
                    destination: store.path().to_owned(),
                });
            }
            Bearing::Carried => {}
        }

        // The origin, first, because it is the cheap half and it is the one a
        // copy fails.
        if Path::new(&origin).exists() {
            return Err(RecoveryError::OriginStillPresent { origin });
        }

        // Then the sockets. Every path the original bound and every path the
        // relocated Nexus would bind, because either kind being held means
        // something is serving where this store is about to.
        let desired = engine.desired_socket_vector()?;
        let mut vacated_socket_vector: Vec<String> = recorded.bound_socket_vector().to_vec();
        for socket_path in desired {
            if !vacated_socket_vector.contains(&socket_path) {
                vacated_socket_vector.push(socket_path);
            }
        }
        {
            // Held only for as long as the question takes to answer, and
            // dropped before anything is written: this tool is asking whether
            // the paths are free, not taking them.
            let mut held = Vec::new();
            for socket_path in &vacated_socket_vector {
                match SocketClaim::claim(Path::new(socket_path)) {
                    Ok(claim) => held.push(claim),
                    Err(TransportError::SocketAlreadyActive(path)) => {
                        return Err(RecoveryError::SocketStillClaimed {
                            socket_path: path.display().to_string(),
                        });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            // Named so that what happens here is the release, not an
            // oversight: the claims go when this block does.
            drop(held);
        }

        let relocation = Relocation::declared(origin, store.path().to_owned());
        let table: TableReference<StoredRelocation> =
            engine.register_table(StoredRelocation::descriptor())?;
        let record = StoredRelocation {
            relocation: relocation.clone(),
        };
        // At most one declaration stands at a time: a second run replaces the
        // first rather than leaving two moves declared for one store.
        match engine.match_records(QueryPlan::all(table))?.records() {
            [] => {
                engine.assert(Assertion::new(table, record))?;
            }
            [_] => {
                engine.mutate(Mutation::new(table, record))?;
            }
            rows => return Err(StoreError::RelocationInvariant { count: rows.len() }.into()),
        }
        Ok(Self {
            relocation,
            vacated_socket_vector,
        })
    }

    fn origin(&self) -> &str {
        self.relocation.origin()
    }

    fn destination(&self) -> &str {
        self.relocation.destination()
    }

    fn vacated_socket_vector(&self) -> &[String] {
        &self.vacated_socket_vector
    }
}

/// The socket paths a store is configured to bind next, read without the
/// Nexus's own open — which is refusing this store, and is the reason this
/// tool exists.
trait ReadsDesiredSockets {
    fn desired_socket_vector(&mut self) -> Result<Vec<String>, RecoveryError>;
}

impl ReadsDesiredSockets for Engine {
    fn desired_socket_vector(&mut self) -> Result<Vec<String>, RecoveryError> {
        let metadata: TableReference<StoredMetadata> =
            self.register_table(StoredMetadata::descriptor())?;
        match self.match_records(QueryPlan::all(metadata))?.records() {
            // A store with no metadata tree has never been configured and so
            // names no sockets; the ones it recorded binding are still
            // checked.
            [] => Ok(Vec::new()),
            [stored] => {
                let desired: StoredConfiguration = stored.state.desired_configuration().clone();
                let configuration: OrchestrateNexusConfiguration = desired.into_public();
                Ok(vec![
                    configuration.ordinary_socket_path,
                    configuration.meta_socket_path,
                ])
            }
            rows => Err(StoreError::MetadataInvariant { count: rows.len() }.into()),
        }
    }
}

/// Why a relocation was not declared.
///
/// Every one of these is a refusal to write the declaration, and none of them
/// changes the store. A store this tool refused is exactly as it was.
#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("there is no store at {store_path:?} to relocate")]
    NoStore { store_path: String },
    #[error(
        "the store at {store_path:?} has never been bound, so it has no address to be moved from and will open here as it stands"
    )]
    NeverBound { store_path: String },
    #[error(
        "this store records having been bound at {origin:?}, which is where it is: nothing has moved"
    )]
    Settled { origin: String },
    #[error(
        "this store was bound at {origin:?} and is now at {destination:?}, and it is the same file: the move is already recognised and needs no declaration"
    )]
    AlreadyRecognised { origin: String, destination: String },
    #[error(
        "the store this one was copied from is still at {origin:?}: a copy is not a move. If this is the store you mean to keep, remove or rename what remains at {origin:?} first — while both exist, declaring this one a move would give two stores one identity."
    )]
    OriginStillPresent { origin: String },
    #[error(
        "{socket_path:?} is still held by a running Nexus: stop it before declaring the move, or the relocated store will be a second claimant to the sockets it is about to bind"
    )]
    SocketStillClaimed { socket_path: String },
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
}
