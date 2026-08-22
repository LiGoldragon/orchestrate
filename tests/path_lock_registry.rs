//! Direct-service witnesses for the durable path-lock registry.

use std::fs;

use datom::{EvidencedRealizing, PathLockText, PathLockViewing, RealizationViewing};
use orchestrate::{
    Error, NativePathLock, OrchestrateReply, OrchestrateService, PathLock,
    PathLockRegistrationRejection, StoreLocation,
};
use protos::SourceText;

fn lock(name: &str, paths: &[&str], description: &str) -> PathLock {
    let native = PathLockText {
        source: SourceText(format!(
            "PathLock.{{{name} [{}] ({description})}}",
            paths.join(" ")
        )),
    }
    .realize_evidenced()
    .expect("native Datom path lock")
    .value()
    .clone();
    PathLock::try_from(native).expect("Signal path-lock carrier")
}

fn service(temporary: &tempfile::TempDir) -> OrchestrateService {
    OrchestrateService::open(&StoreLocation::new(
        temporary.path().join("path-locks.sema").to_string_lossy(),
    ))
    .expect("open isolated service")
}

fn rejection(reply: OrchestrateReply) -> PathLockRegistrationRejection {
    match reply {
        OrchestrateReply::PathLockRegistrationRejected(rejected) => rejected.reason,
        other => panic!("expected typed registration rejection, got {other:?}"),
    }
}

fn name(lock: PathLock) -> String {
    let native = NativePathLock::try_from(lock).expect("native holder");
    native.name().into()
}

#[test]
fn registers_directly_without_mutating_listed_paths() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let existing = temporary.path().join("existing-marker");
    let absent = temporary.path().join("must-stay-absent");
    fs::write(&existing, "untouched").expect("write fixture marker");
    let mut service = service(&temporary);

    let reply = service.register(lock(
        "direct",
        &[
            existing.to_str().expect("UTF-8 path"),
            absent.to_str().expect("UTF-8 path"),
        ],
        "direct registration",
    ));

    assert!(matches!(
        reply.expect("register"),
        OrchestrateReply::PathLockRegistered(_)
    ));
    assert_eq!(
        fs::read_to_string(&existing).expect("read marker"),
        "untouched"
    );
    assert!(!absent.exists());
}

#[test]
fn rejects_duplicate_name_and_every_overlap_direction() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut service = service(&temporary);
    assert!(matches!(
        service
            .register(lock("alpha", &["/tmp/path-locks/parent"], "first lock"))
            .expect("register first"),
        OrchestrateReply::PathLockRegistered(_)
    ));

    assert!(matches!(
        rejection(
            service
                .register(lock("alpha", &["/tmp/path-locks/other"], "same name"))
                .expect("duplicate reply")
        ),
        PathLockRegistrationRejection::DuplicateActiveName { holder } if name(holder.clone()) == "alpha"
    ));
    assert!(matches!(
        rejection(
            service
                .register(lock("descendant", &["/tmp/path-locks/parent/child"], "descendant"))
                .expect("descendant reply")
        ),
        PathLockRegistrationRejection::PathOverlap { holder, .. } if name(holder.clone()) == "alpha"
    ));
    assert!(matches!(
        rejection(
            service
                .register(lock("ancestor", &["/tmp/path-locks"], "ancestor"))
                .expect("ancestor reply")
        ),
        PathLockRegistrationRejection::PathOverlap { holder, .. } if name(holder.clone()) == "alpha"
    ));
}

#[test]
fn rejects_normalized_overlap_and_duplicate_normalized_input_before_wire_construction() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut service = service(&temporary);
    assert!(matches!(
        service
            .register(lock(
                "normalized",
                &["//tmp//path-locks//same/./"],
                "normalized first"
            ))
            .expect("register normalized first"),
        OrchestrateReply::PathLockRegistered(_)
    ));
    assert!(matches!(
        rejection(
            service
                .register(lock("second", &["/tmp/path-locks/same"], "normalized conflict"))
                .expect("normalized overlap reply")
        ),
        PathLockRegistrationRejection::PathOverlap { holder, .. } if name(holder.clone()) == "normalized"
    ));

    let duplicate_native = PathLockText {
        source: SourceText(
            "PathLock.{duplicate [/tmp/path-locks/a //tmp//path-locks/a/.] (duplicate paths)}"
                .into(),
        ),
    };
    assert!(duplicate_native.realize_evidenced().is_err());

    let nameless_native = PathLockText {
        source: SourceText("PathLock.{() [/tmp/path-locks/name] (missing name)}".into()),
    };
    assert!(nameless_native.realize_evidenced().is_err());
}

#[test]
fn atomic_storage_failure_leaves_no_registration() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut service = service(&temporary);
    service.fail_next_atomic_commit_for_test();

    assert!(matches!(
        service.register(lock(
            "atomic",
            &["/tmp/path-locks/atomic"],
            "atomic failure"
        )),
        Err(Error::InjectedAtomicCommitFailure)
    ));
    assert!(
        service
            .active_path_locks()
            .expect("read registry")
            .is_empty()
    );
    assert!(matches!(
        service
            .register(lock(
                "atomic",
                &["/tmp/path-locks/atomic"],
                "atomic success"
            ))
            .expect("retry after injected failure"),
        OrchestrateReply::PathLockRegistered(_)
    ));
}

#[test]
fn restart_preserves_active_registration() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let store = StoreLocation::new(temporary.path().join("path-locks.sema").to_string_lossy());
    {
        let mut service = OrchestrateService::open(&store).expect("open initial service");
        assert!(matches!(
            service
                .register(lock(
                    "persistent",
                    &["/tmp/path-locks/persistent"],
                    "persistent lock"
                ))
                .expect("initial registration"),
            OrchestrateReply::PathLockRegistered(_)
        ));
    }
    let mut restarted = OrchestrateService::open(&store).expect("reopen durable store");
    assert!(matches!(
        rejection(
            restarted
                .register(lock("persistent", &["/tmp/path-locks/other"], "duplicate after restart"))
                .expect("duplicate reply after restart")
        ),
        PathLockRegistrationRejection::DuplicateActiveName { holder } if name(holder.clone()) == "persistent"
    ));
}

#[test]
fn native_lock_conversion_retains_normalized_fields() {
    let carrier = lock(
        "native",
        &["//tmp//path-locks//native/./"],
        "carrier conversion",
    );
    let native = NativePathLock::try_from(carrier).expect("native carrier");
    assert_eq!(native.paths(), ["/tmp/path-locks/native"]);
}
